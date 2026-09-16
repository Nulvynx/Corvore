# CORVORE Resource Budgets

Status: FROZEN — reference baseline v1

## Reference platform

- Raspberry Pi Zero 2 W Rev 1.0
- Raspberry Pi OS Lite ARM64
- 64 GB class SD storage
- clean-native development image
- Wi-Fi and Bluetooth disabled during the reference measurement

Reference report SHA-256:

`06fe79dbcb72382f692e1af50f473eace0fd4982fe52db17e6ef6bdfce088d98`

## Clean-native base-system budgets

| Resource | Budget |
|---|---:|
| Boot time | <= 45 s |
| Idle CPU busy | <= 5% |
| Idle CPU iowait | <= 1% |
| Minimum MemAvailable | >= 240 MiB |
| Idle swap usage | 0 MiB |
| Idle maximum CPU temperature | <= 55 C |
| Short-window idle write-rate estimate | <= 0.5 GiB/day |
| Failed systemd units | 0 |
| Available root storage on reference 64 GB card | >= 50 GiB |

## Reference measurement

- Boot: 26.651 s
- CPU busy: 0.031263%
- CPU idle: 99.968737%
- CPU iowait: 0%
- Minimum MemAvailable: 280.012 MiB
- Swap used: 0 MiB
- Temperature average: 37.681 C
- Temperature maximum: 38.090 C
- Write rate: 1706.57 B/s
- Extrapolated write volume: 0.137321 GiB/day
- Available root storage: 53.347 GiB
- Failed systemd units: 0

## Interpretation

These limits constrain the clean-native base system.

They are not production-runtime performance claims.

The SD write-volume value is a linear extrapolation from a 120-second
idle observation. It must be revalidated during longer soak tests and
with the production collector/runtime.

Clock accuracy was not verified during this measurement.

Resource consumption introduced by application services, radio
collection, the WebUI, persistence processing and evidence handling must
be measured against these reference guardrails in later validation.
