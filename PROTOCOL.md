# Wire protocol

Length-prefixed TCP frames (`u32` BE length + UTF-8 body).

Bot frames are `0` + event + payload. User frames are `1` + event + payload.
Coordinator → bot frames have **no** source prefix: event + payload.

## Shared codes (same meaning both directions)

| Code | Meaning | Bot → coord payload | Coord → bot payload |
|------|---------|---------------------|---------------------|
| `5` | in_game_event | `{bot}:{lobby}:{event}` | `{event}` (e.g. `0` = pause/disconnect) |
| `7` | game_aborted | `{bot}:{lobby}` | often just `0` |
| `8` | game_ended | `{bot}:{lobby}:{dota_id}:{match_duration_secs}` | often just `0` |

## Bot → coordinator only

| Code | Meaning | Payload |
|------|---------|---------|
| `0` | reconnect_handshake | `{bot}:{lobby}:{side}` |
| `1` | lobby_found | `{bot}:{lobby}:{side}` |
| `2` | in_game_parameters | `{bot}:{lobby}:{side}:{dota_id}` |
| `6` | connect_handshake | `{bot}` |

## Coordinator → bot only

| Code | Meaning | Payload |
|------|---------|---------|
| `0` | terminate | — |
| `1` | lobby_verdict | `0` reject / `1` accept |
| `2` | roles | comma-separated role ids |
| `3` | starter_replay_number | number |
| `4` | ongoing_win_team | team id |
| `6` | pre_in_game_disconnect | often `0` |

Note: bot → coord `6` is `connect_handshake` (different namespace).

## Roles

| Code | Role |
|------|------|
| `0` | MidLine |
| `1` | HardLine |
| `2` | EasyLine |