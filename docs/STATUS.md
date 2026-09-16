# CORVORE implementation status

## Reference specification

The implementation is governed by the CORVORE Master Product Plan and
CORVORE Engineering Plan.

Those documents define intended requirements. Functionality is considered
implemented only when it exists in the repository and its acceptance
evidence has passed.

## Milestone state

### M0 — Hardware and clean-native resource baseline

**CLOSED — PASS**

Reference platform:

- Raspberry Pi Zero 2 W Rev 1.0
- Raspberry Pi OS Lite ARM64
- 64 GB class SD storage
- clean-native development image

Reference baseline report SHA-256:

`06fe79dbcb72382f692e1af50f473eace0fd4982fe52db17e6ef6bdfce088d98`

Verified reference measurements:

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

The 120-second write-rate measurement is a short-window extrapolation,
not a measured 24-hour endurance figure.

The filesystem passed a read-only `e2fsck` consistency check with exit
code 0 after the controlled acceptance shutdown.

Resource budgets are frozen in:

- `docs/M0_RESOURCE_BUDGETS.md`
- `config/m0-resource-budgets-v1.json`

Clock accuracy was not verified during M0 and remains a separate
TimeProvider concern.

Wi-Fi and Bluetooth were intentionally disabled during the clean-native
M0 baseline. Radio-runtime resource use is therefore not validated by M0.

### M1 — Data and runtime foundation

**NEXT — NOT YET ACCEPTED**

M1 will establish:

- canonical data contracts,
- `core.db`,
- `observations.db`,
- schema initialization and migrations,
- SQLite durability policy,
- CLI/runtime foundation,
- systemd runtime integration,
- test harness separation from production runtime,
- restart and persistence acceptance tests.

## Implemented foundation

Current repository functionality includes:

- Python package foundation without runtime third-party dependencies,
- development CLI entry point,
- read-only local diagnostics,
- bounded Linux CPU and block-write measurement,
- native first-boot acceptance tooling,
- persistent diagnostic journal configuration for acceptance work,
- M0 resource-baseline tooling,
- frozen M0 resource budgets.

The native first-boot acceptance tooling is development and acceptance
infrastructure. It is not the production CORVORE runtime.

## Not implemented yet

The following major components are not yet production implementations:

- `corvored`,
- split production databases,
- live radio collector,
- Bettercap integration,
- Radio Service / Command Broker,
- WebUI,
- pairing and mTLS management channel,
- Vault,
- GPS / TimeProvider,
- Evidence Engine,
- Workbench,
- update / rollback system,
- final reproducible native image.

## M0 acceptance tooling

The repository retains the tooling needed to reproduce and inspect the
M0 acceptance process.

The original unconditional first-boot shutdown unit was removed after
testing because shutdown must only occur after explicit acceptance
conditions are satisfied.

The full vendor Raspberry Pi `config.txt` is not stored as CORVORE
configuration. Only the M0-specific offline test delta is retained as:

`tools/m0/config-offline.fragment`

## Repository safety

Local operational material is excluded from version control.

This includes, among other things:

- packet captures,
- generated databases,
- private keys,
- local evidence,
- location datasets,
- generated OS images,
- device-specific working data.

`.gitignore` is not a substitute for review or secret scanning.

No real credentials, private captures or location datasets should be
committed.

## Publication work still open

Before a formal public release:

- select and document the project license,
- review third-party dependency licenses,
- establish exact reproducible dependency locking,
- add automated quality and security checks,
- complete the remaining engineering milestones.

## Roadmap progress

Closed milestones: **1 / 11**

- M0: PASS
- M1-M10: not yet accepted
