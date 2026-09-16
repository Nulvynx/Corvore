# CORVORE Core Runtime

## Process role

`corvored` is the unprivileged long-running core service.

Its responsibilities are deliberately narrow:

- initialize and migrate the persistence stores
- maintain the core process lifecycle
- expose ephemeral local runtime state
- provide the future execution boundary for deterministic processing of
  durable observations

`corvored` does not own radio privileges.

## Privilege boundary

The core service runs as the dedicated `corvore` system user.

It must not require:

- root privileges
- Linux capabilities
- raw packet sockets
- direct radio control
- access to physical input devices

Privileged radio operations belong to a separate constrained service.

## Startup

At startup the core service:

1. creates or validates its runtime directory;
2. records a `starting` runtime state;
3. initializes or migrates persistence explicitly;
4. verifies the durable processing checkpoint;
5. enters the `running` state.

Startup does not fabricate observations and does not advance the durable
processing checkpoint.

## Shutdown

`SIGTERM` and `SIGINT` request an orderly stop.

The service records `stopping` and then `stopped` before exiting
successfully.

Unexpected initialization or persistence failures result in a non-zero
exit and, when possible, an ephemeral `failed` runtime state.

## Runtime state

Ephemeral service state is stored as:

`/run/corvore/corvored.json`

The file contains:

- schema version
- service identifier
- lifecycle state
- process ID
- Linux boot ID
- durable processing checkpoint

This is operational state, not durable evidence.

It must not be stored in `core.db` or `observations.db`.

## systemd

The production service unit provides:

- dedicated unprivileged identity
- systemd-managed state and runtime directories
- empty capability sets
- restricted address families
- private device namespace
- read-only system filesystem
- restart on unexpected failure

The service intentionally has no network or radio privileges at this
stage.

## Physical interaction

The physical display remains output-only.

`corvored` does not introduce local buttons, touch input, interactive
menus or other on-device operator controls.

## Native-image filesystem layout

The core runtime uses the following image layout:

- `/opt/corvore/app` — immutable Python application payload
- `/usr/local/bin/corvored` — core service launcher
- `/usr/local/bin/corvorectl` — local administration launcher
- `/usr/lib/systemd/system/corvored.service` — service definition
- `/usr/lib/sysusers.d/corvore.conf` — dedicated system identity
- `/usr/share/doc/corvore/RUNTIME.md` — installed runtime contract

Persistent and ephemeral writable paths remain separate:

- `/var/lib/corvore` — durable application state
- `/run/corvore` — ephemeral service state

The runtime overlay is built offline and does not require `pip` or network
access on the field device.
