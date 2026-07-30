# Helios Python SDK

Async Python client for the [Helios core](https://github.com/helios-data/helios-core) transport
protocol. A Helios *node* uses this SDK to publish events to the core, read the latest value of an
event, and subscribe to event streams from other nodes.

Everything is `asyncio`-based. Wire format is length-prefixed protobuf over plain TCP (default port
`5000`), defined in [helios-protos](helios-protos/transport/protocol.proto).

## Concepts

| Term | Meaning |
| --- | --- |
| **Address** | A node's dotted path in the core's component tree, e.g. `Helios.FALCON.SRAD_Telemetry`. Set once as `node_uri`. |
| **Event name** | A named slot on an address, e.g. `telemetry`, `aprs`, `command`. |
| **Event** | An `(address, event_name)` pair holding the most recent payload. Publishing overwrites the stored value and fans out to subscribers. |

Routing is an **exact `(address, event_name)` match**. The core does not do prefix or wildcard
matching, and it only accepts addresses that already exist in its component tree (registered via the
launcher / `tree.json`) — publishing or subscribing to an unknown address comes back as an
`EventError`.

## Installation

The SDK is consumed as a git submodule + editable `uv` source (this is what
`helios-cots-telemetry`, `helios-aprs-telemetry`, and `helios-mission-control` all do):

```bash
git submodule add https://github.com/helios-data/helios-python-sdk
git submodule update --init --recursive
```

```toml
# pyproject.toml
[project]
dependencies = ["helios-python-sdk"]

[tool.uv.sources]
helios-python-sdk = { path = "helios-python-sdk", editable = true }
```

```bash
uv sync
```

Protobuf classes are generated at install time by the build hook in
[scripts/hatch_build.py](scripts/hatch_build.py) — no manual `protoc` step needed. They land in
`src/helios/generated/` (gitignored) and are importable as `helios.generated.helios.transport`.

## Quick start

```python
import asyncio
from helios import HeliosClient

async def main():
    client = HeliosClient(
        core_address="Helios",              # container/host name of the core
        core_port=5000,
        node_uri="Helios.FALCON.SRAD_Telemetry",
    )
    await client.connect()                  # TCP connect + protocol handshake

    # Publish
    await client.publish_event(event_name="telemetry", data=packet.SerializeToString())

    # Read the latest stored value once
    event = await client.get_event(address="Helios.FALCON.APRS_Telemetry", event_name="aprs")

    # Stream updates
    async with client.subscribe_event(
        address="Helios.FALCON.APRS_Telemetry", event_name="aprs"
    ) as events:
        async for event in events:
            print(event.source_address, len(event.data))

    await client.disconnect()

asyncio.run(main())
```

## API

### `HeliosClient(core_address, core_port, node_uri, *, must_be_registered=False, async_publish=True, use_background_io=True)`

- `node_uri` — this node's address; used as the default publish address and as `source_address` on
  every event it emits.
- `must_be_registered` — if `True`, the handshake is rejected unless the core already has this
  address registered. Default `False` (connect succeeds, but publishes to an unregistered address
  still fail).
- `async_publish` — queue outgoing messages on a background writer instead of awaiting the socket
  write. Requires `use_background_io=True`.
- `use_background_io` — run the reader/writer tasks. Required for `get_event` and
  `subscribe_event`; both raise without it.

### `await connect()`

Opens the TCP connection and performs the version handshake. Raises `ConnectionError` if already
connected or unreachable, `HandshakeError` on a version mismatch or malformed response. On any
failure the connection is reset, so the same client object can be retried.

### `await publish_event(*, event_name, data, event_id=None, override_address=None)`

Publishes `data` (raw `bytes` — serialize your protobuf yourself) under `event_name`.

- `event_id` — defaults to a per-client sequence number.
- `override_address` — publish to *another* node's address instead of your own. `source_address`
  stays your `node_uri`, so the receiver can still tell who sent it. This is how mission-control
  sends commands: it publishes `command` with `override_address="Helios.FALCON.SRAD_Telemetry"`,
  and the telemetry node — subscribed to its own address — picks it up.

### `await get_event(*, address, event_name) -> Event`

One-shot read of the current stored event. Useful to seed state at startup before subscribing.

> **Wrap this in `asyncio.wait_for`.** If the address or event does not exist yet, the core replies
> with an `EventError`, which the SDK only logs — the pending future is never resolved and the await
> hangs forever.

```python
event = await asyncio.wait_for(
    client.get_event(address=ADDR, event_name="telemetry"), timeout=2.0
)
```

### `subscribe_event(*, address, event_name, queue_maxlen=None)`

Async context manager yielding an async iterator of `Event`. Unsubscribes on exit.

- `queue_maxlen=None` — unbounded backlog (default).
- `queue_maxlen=0` — keep only the newest undelivered event; good for high-rate telemetry where
  stale frames are useless.
- `queue_maxlen=N` — at most `N` pending; oldest dropped when full.

Multiple concurrent subscriptions are fine — hold them open with an `AsyncExitStack`:

```python
async with contextlib.AsyncExitStack() as stack:
    for address in addresses:
        events = await stack.enter_async_context(
            client.subscribe_event(address=address, event_name="command")
        )
        tasks.append(asyncio.create_task(handle(events)))
```

### `await disconnect()`

Cancels pending requests, closes all subscriptions, tears down the background tasks and socket.

## Event payloads

`Event` (from `helios.generated.helios.transport`) carries `id`, `event_name`, `timestamp`,
`source_address`, and `data: bytes`. The SDK is payload-agnostic — encode/decode `data` with your
own protos:

```python
from helios.generated.helios.transport import Event, AprsPacket
from src.generated import TelemetryPacket        # your project's protos

await client.publish_event(event_name="telemetry", data=bytes(TelemetryPacket(...)))
packet = TelemetryPacket.parse(event.data)
```

The SDK ships `AprsPacket` / `AprsPosition` in addition to the transport messages, since APRS is
part of the shared proto set. Project-specific protos (e.g. `falcon-protos`) are compiled in the
consuming repo, typically via a `make protos` target.

## Errors

All from `helios.errors`:

| Exception | Raised when |
| --- | --- |
| `HeliosError` | Base class. |
| `ConnectionError` | Not connected, already connected, socket failure, frame too large. |
| `HandshakeError` | Handshake rejected or protocol version mismatch (subclass of `ConnectionError`). |
| `EventError` | Wraps an `EventError` proto (`address`, `event_name`, `error_code`, `request_id`). |

`EventError` messages from the core are **logged, not raised** — the SDK writes a warning on the
`helios.client` logger. Enable logging to see them:

```python
logging.basicConfig(level=logging.INFO)
```

## Operational notes

- **The core drops your subscriptions when you disconnect.** After a reconnect you must
  re-subscribe. The telemetry nodes handle this by running a connection manager task that signals
  `ready` / `connection_lost` events, with the subscription loop re-entering on each reconnect.
- **`publish_event` failures are your reconnect signal.** It raises `ConnectionError` when the
  transport is down; nodes typically catch it, mark the link dead, and let the manager reconnect
  rather than blocking the data path.
- **Do the same on the publish path.** A failed publish should never take down the reader loop that
  produced the packet.
- Max frame size is 16 MB in both the SDK and the core.

## Development

Regenerating protos manually (requires `protoc` and the `betterproto2` plugin in `.venv`):

```bash
make protos        # generate into src/helios/generated
make clean-protos
```

Bumping the proto submodule:

```bash
git submodule update --remote --merge helios-protos
```

## Repository layout

```
src/helios/
  client.py         HeliosClient — the public API
  transport.py      framed TCP I/O, background reader/writer tasks
  request.py        pending get_event futures keyed by request_id
  subscription.py   per-subscription buffered queues
  errors.py         exception types
  generated/        protoc output (gitignored, built on install)
helios-protos/      submodule: shared .proto definitions
```
