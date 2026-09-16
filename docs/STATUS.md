# CORVORE implementation status

## Branch policy

- `dev` contains active development.
- `main` is reserved for accepted stable/release states.

## Reference specification

Implementation is governed by the CORVORE Master Product Plan and
Engineering Plan.

The specification defines intended behavior. Functionality is considered
implemented only when code exists and its acceptance evidence passes.

## Completed engineering gate

### Hardware and clean-native base-system qualification

Status: **PASS**

Reference platform:

- Raspberry Pi Zero 2 W Rev 1.0
- Raspberry Pi OS Lite ARM64
- 64 GB class SD storage
- clean-native development image

Reference baseline report SHA-256:

`06fe79dbcb72382f692e1af50f473eace0fd4982fe52db17e6ef6bdfce088d98`

Verified measurements:

- boot: 26.651 s
- CPU busy: 0.031263%
- CPU idle: 99.968737%
- CPU iowait: 0%
- minimum MemAvailable: 280.012 MiB
- swap used: 0 MiB
- average CPU temperature: 37.681 C
- maximum CPU temperature: 38.090 C
- observed write rate: 1706.57 B/s
- short-window extrapolation: 0.137321 GiB/day
- available root storage: 53.347 GiB
- failed systemd units: 0

The filesystem passed a read-only `e2fsck` consistency check with exit
code 0 after controlled shutdown.

Resource limits are recorded in:

- `docs/RESOURCE_BUDGETS.md`
- `config/resource-budgets.json`

Clock accuracy was not verified during the reference measurement.

Onboard Wi-Fi and Bluetooth were deliberately disabled during the
clean-native baseline, so radio-runtime resource consumption remains
unvalidated.

## Current development focus

### Persistence and runtime foundation

Status: **IN PROGRESS**

Current work establishes:

- canonical observation contracts
- `core.db`
- `observations.db`
- schema migration integrity
- SQLite durability policy
- CLI persistence administration
- durable processing checkpoint
- local transactional outbox
- restart and recovery semantics
- separation of production runtime from engineering validation tools

## Implemented foundation

The repository currently contains:

- dependency-light Python package foundation
- local administration CLI
- read-only system diagnostics
- bounded Linux CPU and block-write measurement
- native first-boot acceptance tooling
- persistent journal configuration for acceptance work
- clean-native resource-baseline tooling
- frozen reference resource budgets

## Not yet implemented as production components

- long-running `corvored`
- live radio collector
- Bettercap integration
- Radio Service / Command Broker
- hostile-input normalization
- BSS identity and inferred network grouping
- pairing and mTLS management channel
- WebUI
- Vault
- GPS / TimeProvider
- Evidence Engine
- Workbench
- update and rollback mechanism
- final reproducible native image

## Repository safety

Local operational material is excluded from version control.

This includes:

- packet captures
- generated databases
- private keys
- local evidence
- location datasets
- generated OS images
- device-specific working data

`.gitignore` does not replace manual review or secret scanning.

## Publication work still open

Before a formal public release:

- select and document the project license
- review third-party dependency licenses
- establish reproducible dependency locking
- add automated quality and security checks
- complete the remaining engineering acceptance gates

Completed engineering gates: **1 / 11**

## Physical interaction model

The device display is output-only.

CORVORE does not depend on:

- a pointer
- physical control buttons
- touch input
- on-device interactive menus

The field device may present status and telemetry locally, but operator
control is not performed through the display.

Operator-initiated active assessment functions are exposed only through
the authenticated local management interface when the device is
connected to a workstation.

Standalone device operation remains focused on passive collection and
local persistence.

## Core runtime boundary

The current runtime work introduces `corvored` as the unprivileged core
service.

Its intended boundary is:

- persistence lifecycle and migrations
- durable processing coordination
- local ephemeral health state
- no radio privileges
- no physical-input control
- no active assessment actions

Privileged radio control remains a separate process boundary.
