# CORVORE Passive Collection

## Role

Passive collection is a one-way ingress path from the radio
observation layer into the unprivileged CORVORE core.

The core runtime does not control Bettercap directly.

Bettercap output is treated as hostile upstream input even when
Bettercap itself is running locally on the appliance.

## Trust boundary

The passive collection boundary accepts only explicitly supported
Wi-Fi observation event types.

The initial allowlist is:

- `wifi.ap.new`
- `wifi.ap.lost`
- `wifi.client.new`
- `wifi.client.lost`
- `wifi.client.probe`
- `wifi.client.deauthentication`
- `wifi.client.handshake`

An observed deauthentication event is evidence of RF activity. It does
not authorize CORVORE to transmit a deauthentication frame.

Unknown event types are rejected rather than persisted implicitly.

## Input limits

Each source event is limited to 64 KiB.

Nested data, container sizes, object-key lengths, string lengths,
integer ranges and floating-point values are bounded before the event
can reach durable persistence.

Only finite JSON numbers are accepted.

The top-level Bettercap event envelope must contain exactly:

- `tag`
- `time`
- `data`

## Time semantics

The Bettercap `time` field is preserved as upstream evidence.

It is not promoted to `observed_at_utc` and it does not set
`clock_accuracy_verified`.

Verified UTC time remains the responsibility of the CORVORE
TimeProvider.

## Observation identity

Each independently emitted Bettercap event receives a fresh, canonical
UUIDv4 `delivery_id` at the trusted event bridge. The bridge MUST retain
the original ID and exact event bytes for any retransmission after a
missing acknowledgement. It MUST NOT mint a new ID inside a retry loop.

The observation identifier is UUIDv5 over Linux boot identity, collector
source instance, and `delivery_id`. The canonical event SHA-256 remains
part of the observation payload and participates in the persistence
integrity check; it is **not** the event identity.

Two distinct emissions with byte-identical contents therefore persist
as two observations. A retry of the same emission maps to the original
`ingest_seq`. Reuse of one `delivery_id` with different content is
rejected, without terminating the ingress daemon.

A bounded, durable bridge-side retry spool is available as a library.
Its live Bettercap source adapter and production service wiring have not
been implemented; end-to-end delivery is **not** yet validated. The spool
persists each new emission before delivery and retains exact event bytes
and `delivery_id` until the core returns a matching committed ACK.
The current ingress version 2 derives identity from the *receiving*
Linux boot and cannot safely replay records from an earlier boot. The
spool explicitly retains and blocks those records until the protocol
carries original boot/monotonic provenance. Do not discard them silently.

## Process separation

The target runtime architecture is:

Bettercap -> passive event bridge -> AF_UNIX ingress -> corvored

`corvored` remains without radio capabilities and without network
access.

The passive event bridge does not expose operator commands.

Active or transmitting radio operations belong to the separate
authenticated radio-command path and are not part of passive
collection.

## Transport

The passive transport uses a local Unix-domain `SOCK_SEQPACKET`
message boundary with bounded frames. The protocol schema is version 2.
Its exact JSON header contains `message_type`, `schema_version`,
`source_instance`, and canonical UUIDv4 `delivery_id`, followed by a
newline and the raw event bytes. Version 1 senders are rejected.

The core validates the connecting process identity through Linux
`SO_PEERCRED` and accepts collection traffic only from the configured
collector UID.

A connection carries one source event. This bounds the lifetime of one
ingress session and prevents a faulty collector from holding a single
connection indefinitely.

The shared ingress runtime directory is owned by the core service and
grouped to `corvore-ingress`. The collector receives only the group
access required to connect to the ingress socket.

An acknowledgement with status `committed` is emitted only after the
SQLite observation append transaction returns successfully.

`observations.db` retains its configured SQLite durability policy. A
transport acknowledgement is therefore a commit acknowledgement, not
a claim that every event forced an immediate physical-media flush.

If an acknowledgement is lost after commit, retransmission is safe:
reuse of the original delivery identity maps the event back to the
existing `ingest_seq` rather than creating a second observation. The
original persisted monotonic receive timestamp remains unchanged.

Transport failure must not cause the core to fabricate observations.

## Bridge retry spool (library; not field-ready)

`corvore.bridge_spool.DurableEventSpool` runs under the future dedicated
collector UID. It is neither started by `corvored` nor activated by the
presence of a radio. It does not run Bettercap or change radio state.

- SQLite WAL with `synchronous=FULL` records each validated source emission
  before the first ingress send; a UUIDv4 `delivery_id` is created exactly
  once at enqueue time.
- Its private directory and files are restricted to their owning UID.
  A nonblocking advisory lock permits one spool owner per directory.
- Default *pending event payload* limits: 2,048 items and 16 MiB.
  These are **not** an overall filesystem-write or WAL-size budget.
- FIFO delivery removes a record only after a committed ACK with the
  expected observation identifier and positive ingestion sequence.
  Transport errors, invalid ACKs, and rejections retain the record.
- Process crash/restart in the **same Linux boot** preserves queued
  events and original delivery IDs. A crash between the core commit and
  queue deletion results in idempotent retransmission in that boot.
- With ingress protocol v2, an unacknowledged record from a *previous*
  Linux boot MUST NOT be retransmitted: the core would compute a
  different observation identifier and could duplicate or misattribute
  an observation. The spool detects this and fails closed without deleting.
- A full spool raises an error; a future live source must stop/pause or
  apply an explicitly documented backpressure strategy. No automatic
  eviction, dropping, or synthetic replacement is permitted.

The queue alone cannot guarantee events emitted upstream before enqueue,
whole-device reboot replay, or absolute physical-media durability. The
Bettercap version, live event-source framing, collector service isolation,
regulatory controls, and real-radio operation still require acceptance.

## Runtime policy

Normal field operation depends on live radio collection.

Replay data, synthetic observations and test fixtures are not runtime
dependencies.

## Live Bettercap WebSocket bridge

`corvore.bridge_source` provides a live-only adapter for Bettercap's
`/api/events` WebSocket. It accepts only the loopback endpoint
`ws://127.0.0.1:<configured-port>/api/events`, uses HTTP Basic
authentication from a collector-owned private credentials file, and
does not expose Bettercap command execution.

Only allowlisted passive Wi-Fi events are validated and enqueued.
Other module events are ignored, not stored. A message is bounded to
64 KiB, and the WebSocket client uses a one-frame receive queue.

At startup, the bridge attempts to drain older queued events before
opening the live source. Every newly received passive event is
persisted to the durable spool before attempting ingress delivery.
A failed delivery retains the queued event and terminates the process.

This is a source adapter and bridge process implementation, not a
validated field deployment. It is not installed or started
automatically. The optional `bettercap-bridge` Python dependency,
dedicated UID and network namespace, secret provisioning, pinned
Bettercap version, service supervision, and compatible RF adapter
still require deployment and acceptance.

Bettercap's WebSocket does not acknowledge each event after CORVORE's
durable enqueue. Source-side disconnection, upstream buffer overflow,
and the initial-buffer/listener transition may lose events before
they reach the CORVORE spool. Do not claim end-to-end lossless capture.

Ingress v2 also remains unable to replay events across a whole-device
reboot. The spool retains and blocks such records rather than
silently duplicating or discarding them.
