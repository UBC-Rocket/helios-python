from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager

from .generated.helios.transport import (
    Event,
    EventPublish,
    EventRequest,
    EventSubscribe,
    EventUnsubscribe,
    HandshakeRequest,
    TransportMessage,
)

from helios.errors import ConnectionError as HeliosConnectionError
from helios.errors import HandshakeError
from helios.request import RequestManager
from helios.subscription import SubscriptionManager
from helios.transport import HeliosTransport

import logging

logger = logging.getLogger(__name__)


class HeliosClient:
    """Async TCP client for the Helios transport protocol."""

    def __init__(
        self,
        core_address: str,
        core_port: int,
        node_uri: str,
        *,
        must_be_registered: bool = False,
        async_publish: bool = True,
        use_background_io: bool = True,
    ) -> None:
        self._node_uri = node_uri
        self._must_be_registered = must_be_registered
        self._async_publish = async_publish
        self._use_background_io = use_background_io
        self._sequence_number = 0
        self._request_manager = RequestManager()
        self._subscription_manager = SubscriptionManager()
        self._transport = HeliosTransport(core_address, core_port)
        self._get_event_reply_consumed = False

    async def connect(self) -> None:
        """Connects to the Helios transport layer, and performs the handshake with the Helios core."""

        if self._transport.is_connected:
            raise HeliosConnectionError("Cannot connect to Helios: Already connected")
        try:
            # Connect to the transport
            await self._transport.connect()
            if not await self._perform_handshake():
                raise HandshakeError("Failed to perform handshake with Helios")

            # Start background tasks if needed
            if self._use_background_io:
                await self._transport.start_tasks()

                # Register message callbacks
                self._transport.register_message_callback(
                    "request_response",
                    self._request_manager.handle_incoming,
                )
                self._transport.register_message_callback(
                    "subscription_response",
                    self._subscription_manager.handle_incoming,
                )
                self._transport.register_message_callback(
                    "event_error",
                    self._handle_event_error,
                )

        except Exception as e:
            await self._reset_connection()
            raise e

    def _handle_event_error(self, message: TransportMessage) -> None:
        err = message.event_error
        if err is None:
            return
        code_val = err.error_code
        code = getattr(code_val, "name", str(code_val))
        rid = err.request_id if err.request_id else None
        logger.warning(
            "Helios EventError: address=%r event_name=%r error_code=%s message=%r "
            "request_id=%r",
            err.address,
            err.event_name,
            code,
            err.message,
            rid,
        )

    async def _perform_handshake(self) -> bool:
        # Create handshake request
        request = HandshakeRequest(
            version=HeliosTransport.PROTOCOL_VERSION,
            client_address=self._node_uri,
            must_be_registered=self._must_be_registered,
        )
        outgoing = TransportMessage(handshake_request=request)

        # Send handshake request to Helios, and then wait for handshake response
        try:
            await self._transport.write_payload(outgoing)
            envelope = await self._transport.read_payload()
        except asyncio.IncompleteReadError as e:
            raise HeliosConnectionError("Connection closed during handshake") from e
        except (TypeError, ValueError) as e:
            raise HandshakeError(f"Invalid handshake response: {e}") from e

        # Parse and validate handshake response
        response = envelope.handshake_response
        if response is None:
            raise HandshakeError(
                "Invalid handshake response: expected handshake_response in "
                "TransportMessage"
            )

        if response.version != HeliosTransport.PROTOCOL_VERSION:
            raise HandshakeError(
                f"Protocol version mismatch: server {response.version}, "
                f"client {HeliosTransport.PROTOCOL_VERSION}"
            )
        return True

    async def _send_outgoing(self, message: TransportMessage) -> None:
        if self._async_publish and self._use_background_io:
            await self._transport.enqueue_outgoing(message)
        else:
            await self._transport.write_payload(message)

    async def _reset_connection(self) -> None:
        self._subscription_manager.close_all()
        self._request_manager.clear_all()
        await self._transport.reset()

    async def publish_event(
        self,
        *,
        event_name: str,
        data: bytes,
        event_id: int | None = None,
        override_address: str | None = None,
    ) -> None:
        """Publishes an event to Helios.

        Args:
            event_name: The name of the event to publish.
            data: The data of the event to publish.
            event_id: The ID of the event to publish. If not provided, a new sequence number will be generated.
            override_address: Publish using this address instead of the client's node URI.
        """
        if not self._transport.is_connected:
            raise HeliosConnectionError("Failed to publish event: Not connected to Helios")

        event_id = event_id if event_id is not None else self._sequence_number
        self._sequence_number += 1
        address = override_address if override_address is not None else self._node_uri

        event = Event(
            id=event_id,
            event_name=event_name,
            source_address=self._node_uri,
            data=data,
        )
        publish = EventPublish(
            address=address,
            event_name=event_name,
            event=event
        )
        message = TransportMessage(event_publish=publish)

        try:
            await self._send_outgoing(message)
        except Exception as e:
            raise HeliosConnectionError("Failed to publish event: Failed to write payload") from e

    async def get_event(
        self,
        *,
        address: str,
        event_name: str
    ) -> Event:
        """Gets an event from Helios.

        Requires use_background_io=True.

        Args:
            address: The address of the event to get.
            event_name: The name of the event to get.

        Returns:
            A future that will be resolved with the event that was received from the Helios transport layer.
        """
        if not self._transport.is_connected:
            raise HeliosConnectionError("Failed to get event: Not connected to Helios")
        if not self._use_background_io:
            raise HeliosConnectionError("get_event requires use_background_io=True so incoming publishes are read")

        request_id, pending_future = self._request_manager.create_event()
        message = TransportMessage(
            event_request=EventRequest(
                address=address,
                event_name=event_name,
                request_id=request_id,
            )
        )
        await self._send_outgoing(message)
        return await pending_future

    @asynccontextmanager
    async def subscribe_event(
        self,
        *,
        address: str,
        event_name: str,
        queue_maxlen: int | None = None,
    ) -> AsyncGenerator[AsyncIterator[Event], None]:
        """Subscribe to event updates from Helios.

        Requires use_background_io=True.

        Args:
            address: The address of the event to subscribe to.
            event_name: The name of the event to subscribe to.
            queue_maxlen: None — unbounded backlog. 0 — keep only the latest undelivered
                event. >= 1 — at most that many pending events (oldest dropped when full).

        Usage::

            async with client.subscribe_event(address=..., event_name=...) as subscription:
                async for event in subscription:
                    print(event)
        """
        if not self._transport.is_connected:
            raise HeliosConnectionError("Failed to subscribe: Not connected to Helios")
        if not self._use_background_io:
            raise HeliosConnectionError("subscribe_event requires use_background_io=True so incoming publishes are read")
        if queue_maxlen is not None and queue_maxlen < 0:
            raise ValueError("queue_maxlen must be None or >= 0")

        subscription_id, sub = self._subscription_manager.create_subscription(queue_maxlen=queue_maxlen)
        subscribe_msg = TransportMessage(
            event_subscribe=EventSubscribe(
                address=address,
                event_name=event_name,
                subscription_id=subscription_id,
            ),
        )
        try:
            await self._send_outgoing(subscribe_msg)
            yield sub.events()
        finally:
            self._subscription_manager.remove_subscription(subscription_id)
            try:
                sub.close()
            except Exception:
                pass
            if self._transport.is_connected:
                unsubscribe_msg = TransportMessage(
                    event_unsubscribe=EventUnsubscribe(subscription_id=subscription_id),
                )
                try:
                    await self._send_outgoing(unsubscribe_msg)
                except Exception as e:
                    logger.warning("EventUnsubscribe failed: %s", e)

    async def disconnect(self) -> None:
        self._request_manager.clear_all()
        self._subscription_manager.close_all()
        await self._transport.disconnect()
