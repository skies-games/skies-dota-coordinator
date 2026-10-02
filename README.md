# skies-dota-coordinator

Central TCP server (Rust/Tokio) that coordinates Dota 2 bot clients. Receives bot events (lobby found, in-game state, game ended), assigns roles, tracks statistics, and sends commands back to bots.

## Requirements

- Rust (edition 2024)
- Linux server (deployed via systemd)

## Setup

```bash
cargo build --release
```

Create a `.env` file:

```env
COORDINATOR_SERVER_IP=0.0.0.0
COORDINATOR_SERVER_PORT=8080
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
VK_TOKEN=
VK_PEER_ID=

# Telemetry (OTLP/HTTP direct — no otel-collector)
OTEL_SERVICE_NAME=skiesdota-coordinator
OTEL_TRACES_ENABLED=true
OTEL_METRICS_ENABLED=true
OTEL_LOGS_ENABLED=true
APP_VERSION=0.1.0
DEPLOYMENT_ENV=dev

# Logs → OpenObserve. Credentials = base64(email:password)
OPENOBSERVE_ENDPOINT=https://openobserve-test.skiesdota.com/api/default/v1/logs
OPENOBSERVE_CREDENTIALS=

# Traces → Tempo. Credentials = base64(user:pass) for Traefik basic auth
TEMPO_ENDPOINT=https://tempo.skiesdota.com/v1/traces
TEMPO_CREDENTIALS=

# Metrics → VictoriaMetrics OTLP (not /prometheus). Credentials = base64(user:pass)
VICTORIA_METRICS_ENDPOINT=https://victoria-metrics.skiesdota.com/insert/0/opentelemetry/v1/metrics
VICTORIA_METRICS_CREDENTIALS=
```

## Run

```bash
cargo run --release
cargo run --release -d   # debug logging
```

Deployed to `/root/skies-dota-coordinator` as `coordinator.service` (see `.github/workflows/deploy.yaml`).

## Protocol

Length-prefixed TCP messages. Source codes: `0` = bot, `1` = user.

**Bot events:** reconnect_handshake, lobby_found, in_game_parameters, game_ended, game_aborted, in_game_event, connect_handshake

**Outbound commands:** terminate, lobby_verdict, roles, starter_replay_number, ongoing_win_team, pre_in_game_disconnect, in_game_event, game_aborted

**Roles:** `0` MidLine, `1` HardLine, `2` EasyLine

Full reference in `src/main.rs` header comment.

## Persistence

- `bots_statistics.json` — per-bot match results
- `bots_play_time.json` — cumulative play time per account

## Related repos

- [skies-dota](../skies-dota) — bot client that connects to this server
- [skies-dota-bot-automations](../skies-dota-bot-automations) — pre-game setup scripts
