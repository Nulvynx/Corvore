# CORVORE

CORVORE is a local-first wireless assessment platform designed to build
a persistent model of observed wireless environments and support
authorized security assessment workflows.

## Project status

CORVORE is under active development.

The reference hardware and clean-native operating-system baseline have
completed engineering acceptance. The persistence and runtime foundation
is currently being implemented.

Production runtime and the first public release are not yet available.

## Architecture direction

CORVORE is designed around:

- local-first operation
- offline-by-default field use
- persistent structured observations
- separate ingress and durable application-state databases
- strict separation of privileged radio control from unprivileged
  knowledge and presentation services
- passive discovery within available radio capabilities
- explicitly operator-started active operations through an authenticated
  local management session and only on owned or otherwise authorized
  networks

## Repository hygiene

Operational captures, credentials, private keys, location datasets,
generated databases, device evidence, generated images and
development-host artifacts do not belong in this repository.

See:

- `docs/STATUS.md` for implementation state
- `docs/RESOURCE_BUDGETS.md` for reference hardware limits
- `docs/PERSISTENCE.md` for persistence architecture
