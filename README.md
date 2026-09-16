# CORVORE

CORVORE is a local-first wireless assessment platform designed to build
a persistent model of observed wireless environments and support
authorized security assessment workflows.

## Project status

CORVORE is in early development.

- M0 — Hardware and clean-native resource baseline: **PASS**
- M1 — Data contracts, split persistence, CLI/runtime foundation: next
- Production runtime: not yet implemented
- Release v1.0: not yet available

The current codebase is an engineering foundation, not a finished
security appliance.

## Design direction

CORVORE is designed around:

- local-first operation,
- offline-by-default field use,
- persistent structured observations,
- strict separation of privileged radio control from unprivileged
  knowledge and presentation services,
- passive discovery within available radio capabilities,
- explicitly operator-started active operations through an authenticated
  local management session and only on owned or otherwise authorized
  networks.

## Repository hygiene

Operational captures, credentials, location datasets, local databases,
device evidence, generated images and development-host artifacts do not
belong in this repository.

See `docs/STATUS.md` for the implementation state and
`docs/M0_RESOURCE_BUDGETS.md` for the frozen M0 baseline.
