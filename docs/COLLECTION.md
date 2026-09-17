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

A deterministic observation identifier is derived from:

- Linux boot identity
- collector source instance
- canonical source event hash

This allows repeated delivery of the same Bettercap event within one
boot to map to the same observation identity.

Events observed on different boots remain distinct.

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

The production transport will use a local Unix-domain message boundary
with bounded frames and an acknowledgement only after durable
persistence.

Transport failure must not cause the core to fabricate observations.

## Runtime policy

Normal field operation depends on live radio collection.

Replay data, synthetic observations and test fixtures are not runtime
dependencies.
