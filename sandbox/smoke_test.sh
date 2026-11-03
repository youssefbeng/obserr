#!/usr/bin/env bash
set -euo pipefail

KEEP_RUNNING=false
if [[ "${1:-}" == "--keep-running" ]]; then
  KEEP_RUNNING=true
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d -t obserr-smoke-XXXXXX)"
OBSERR_PORT="${OBSERR_PORT:-18080}"
WEBHOOK_PORT="${WEBHOOK_PORT:-18081}"
WEBHOOK_LOG="$TMP_DIR/slack_webhooks.jsonl"
SERVER_PID=""
WEBHOOK_PID=""

# Arrête proprement les processus d'arrière-plan et supprime les fichiers temporaires à la sortie.
cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ -n "$WEBHOOK_PID" ]] && kill -0 "$WEBHOOK_PID" 2>/dev/null; then
    kill "$WEBHOOK_PID" 2>/dev/null || true
    wait "$WEBHOOK_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# Vérifie qu'une chaîne attendue est présente dans le texte fourni, sinon échoue avec un message clair.
assert_contains() {
  local haystack="$1"
  local needle="$2"
  local label="$3"
  if [[ "$haystack" != *"$needle"* ]]; then
    echo "❌ ÉCHEC [$label] : '$needle' introuvable" >&2
    exit 1
  fi
}

# Vérifie qu'une chaîne est absente du texte fourni, sinon échoue avec un message clair.
assert_not_contains() {
  local haystack="$1"
  local needle="$2"
  local label="$3"
  if [[ "$haystack" == *"$needle"* ]]; then
    echo "❌ ÉCHEC [$label] : '$needle' ne devrait pas être présent" >&2
    exit 1
  fi
}

# Vérifie l'égalité entre deux valeurs scalaires pour valider un compteur ou un code HTTP.
assert_eq() {
  local actual="$1"
  local expected="$2"
  local label="$3"
  if [[ "$actual" != "$expected" ]]; then
    echo "❌ ÉCHEC [$label] : obtenu '$actual', attendu '$expected'" >&2
    exit 1
  fi
}

# Démarre un récepteur HTTP local de webhooks Slack enregistrant chaque requête JSON dans WEBHOOK_LOG.
start_webhook_receiver() {
  touch "$WEBHOOK_LOG"
  python3 - "$WEBHOOK_PORT" "$WEBHOOK_LOG" <<'PY' &
import http.server
import sys

port = int(sys.argv[1])
log_path = sys.argv[2]

class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8", errors="replace")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(body.replace("\n", "\\n") + "\n")
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")
    def log_message(self, format, *args):
        pass

http.server.HTTPServer(("127.0.0.1", port), Handler).serve_forever()
PY
  WEBHOOK_PID=$!
}

# Attend que l'endpoint /health du binaire obserr serve réponde 200 OK.
wait_for_server() {
  local attempts=0
  while ! curl -fsS "http://127.0.0.1:${OBSERR_PORT}/health" >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [[ $attempts -ge 50 ]]; then
      echo "❌ ÉCHEC : obserr serve n'a pas démarré sur le port ${OBSERR_PORT}" >&2
      exit 1
    fi
    sleep 0.1
  done
}

echo "==> 1. Compilation du binaire obserr..."
cargo build --manifest-path "$ROOT_DIR/Cargo.toml" --quiet

echo "==> 2. Démarrage du récepteur Slack local (port ${WEBHOOK_PORT}) et d'obserr serve (port ${OBSERR_PORT})..."
start_webhook_receiver

cat >"$TMP_DIR/obserr.toml" <<EOF
[server]
listen = "127.0.0.1:${OBSERR_PORT}"
database_path = "${TMP_DIR}/obserr.db"
public_url = "http://127.0.0.1:${OBSERR_PORT}"
timezone = "UTC"

[channel]
webhook_url = "http://127.0.0.1:${WEBHOOK_PORT}/slack"

[sources.dbt-prod]
token = "secret-dbt-token"

[sources.airflow-prod]
token = "secret-airflow-token"

[jobs.dbt-prod.compta_nightly]
schedule = "0 2 * * *"
grace = "30m"
timezone = "Europe/Paris"
owner = "equipe-finance"
tags = ["finance", "nightly"]

[jobs.airflow-prod.adhoc_sync]
owner = "equipe-data"
tags = ["adhoc"]
EOF

"$ROOT_DIR/target/debug/obserr" serve --config "$TMP_DIR/obserr.toml" &
SERVER_PID=$!
wait_for_server

echo "==> 3. Vérification de /health (200), du rejet d'authentification (401) et des Jobs pré-déclarés dans le TOML..."
health_code="$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${OBSERR_PORT}/health")"
assert_eq "$health_code" "200" "GET /health"

unauth_code="$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  -H "Content-Type: application/json" \
  -d '{"job":"test","event":"started"}' \
  "http://127.0.0.1:${OBSERR_PORT}/api/v1/events")"
assert_eq "$unauth_code" "401" "POST /api/v1/events sans token"

initial_dash="$(curl -fsS "http://127.0.0.1:${OBSERR_PORT}/")"
assert_contains "$initial_dash" "compta_nightly" "Job pré-déclaré compta_nightly sur /"
assert_contains "$initial_dash" "OK" "Statut OK pour Job avec Schedule"
assert_contains "$initial_dash" "0 2 * * *" "Expression cron sur /"
assert_contains "$initial_dash" "equipe-finance" "Owner sur /"
assert_contains "$initial_dash" "finance, nightly" "Tags sur /"
assert_contains "$initial_dash" "adhoc_sync" "Job sans Schedule sur /"
assert_contains "$initial_dash" "Sans Schedule" "Statut Sans Schedule sur /"

compta_detail="$(curl -fsS "http://127.0.0.1:${OBSERR_PORT}/jobs/dbt-prod/compta_nightly")"
assert_contains "$compta_detail" "Europe/Paris" "Fuseau horaire sur page détail Job"
assert_contains "$compta_detail" "30m" "Délai grace sur page détail Job"

echo "==> 4. Enregistrement d'un Job sain (airflow-prod / alpha_healthy)..."
curl -fsS -X POST "http://127.0.0.1:${OBSERR_PORT}/api/v1/events" \
  -H "Authorization: Bearer secret-airflow-token" \
  -H "Content-Type: application/json" \
  -d '{"job":"alpha_healthy","event":"started","run_id":"ok-1","timestamp":"2026-11-03T08:00:00Z"}' >/dev/null

curl -fsS -X POST "http://127.0.0.1:${OBSERR_PORT}/api/v1/events" \
  -H "Authorization: Bearer secret-airflow-token" \
  -H "Content-Type: application/json" \
  -d '{"job":"alpha_healthy","event":"finished","run_id":"ok-1","status":"succeeded","timestamp":"2026-11-03T08:01:30Z"}' >/dev/null

echo "==> 5. Premier Failed Run sur dbt-prod / orders_mart -> ouverture d'Incident + 1 webhook Slack..."
curl -fsS -X POST "http://127.0.0.1:${OBSERR_PORT}/api/v1/events" \
  -H "Authorization: Bearer secret-dbt-token" \
  -H "Content-Type: application/json" \
  -d '{
    "job": "orders_mart",
    "event": "finished",
    "run_id": "fail-1",
    "status": "failed",
    "step": "stg_orders",
    "error_excerpt": "Database Error in model stg_orders",
    "external_url": "https://ci.internal/dbt/runs/fail-1",
    "timestamp": "2026-11-03T10:00:00Z"
  }' >/dev/null

webhook_count="$(wc -l <"$WEBHOOK_LOG" | tr -d ' ')"
assert_eq "$webhook_count" "1" "1 seul webhook Slack à l'ouverture"
first_webhook="$(sed -n '1p' "$WEBHOOK_LOG")"
assert_contains "$first_webhook" "orders_mart" "nom du Job dans l'alerte Slack"
assert_contains "$first_webhook" "dbt-prod" "Source dans l'alerte Slack"
assert_contains "$first_webhook" "Failed Run" "cause dans l'alerte Slack"
assert_contains "$first_webhook" "stg_orders" "Step dans l'alerte Slack"
assert_contains "$first_webhook" "Database Error in model stg_orders" "extrait d'erreur dans l'alerte Slack"
assert_contains "$first_webhook" "https://ci.internal/dbt/runs/fail-1" "lien Source dans l'alerte Slack"
assert_contains "$first_webhook" "http://127.0.0.1:${OBSERR_PORT}/jobs/dbt-prod/orders_mart" "lien obserr dans l'alerte Slack"

echo "==> 6. Second Failed Run consécutif sur orders_mart -> déduplication (0 webhook supplémentaire)..."
curl -fsS -X POST "http://127.0.0.1:${OBSERR_PORT}/api/v1/events" \
  -H "Authorization: Bearer secret-dbt-token" \
  -H "Content-Type: application/json" \
  -d '{
    "job": "orders_mart",
    "event": "finished",
    "run_id": "fail-2",
    "status": "failed",
    "step": "stg_orders",
    "error_excerpt": "Second failure excerpt on stg_orders",
    "external_url": "https://ci.internal/dbt/runs/fail-2",
    "timestamp": "2026-11-03T10:05:00Z"
  }' >/dev/null

webhook_count="$(wc -l <"$WEBHOOK_LOG" | tr -d ' ')"
assert_eq "$webhook_count" "1" "aucune alerte Slack supplémentaire sur échec consécutif"

echo "==> 7. Vérification de GET / (Incident ouvert en tête) et GET /incidents..."
dash_html="$(curl -fsS "http://127.0.0.1:${OBSERR_PORT}/")"
assert_contains "$dash_html" "Incident ouvert" "badge Incident ouvert sur /"

inc_html="$(curl -fsS "http://127.0.0.1:${OBSERR_PORT}/incidents")"
assert_contains "$inc_html" "orders_mart" "orders_mart listé sur /incidents"
assert_contains "$inc_html" "Second failure excerpt on stg_orders" "dernier extrait d'erreur sur /incidents"
assert_not_contains "$inc_html" "alpha_healthy" "Job sain absent de /incidents"

echo "==> 8. Run succeeded sur orders_mart -> fermeture de l'Incident + alerte Slack '✅ Résolu'..."
curl -fsS -X POST "http://127.0.0.1:${OBSERR_PORT}/api/v1/events" \
  -H "Authorization: Bearer secret-dbt-token" \
  -H "Content-Type: application/json" \
  -d '{
    "job": "orders_mart",
    "event": "finished",
    "run_id": "ok-recover",
    "status": "succeeded",
    "timestamp": "2026-11-03T10:20:00Z"
  }' >/dev/null

webhook_count="$(wc -l <"$WEBHOOK_LOG" | tr -d ' ')"
assert_eq "$webhook_count" "2" "alerte Slack de résolution envoyée"
second_webhook="$(sed -n '2p' "$WEBHOOK_LOG")"
assert_contains "$second_webhook" "✅ Résolu" "mention Résolu dans Slack"
assert_contains "$second_webhook" "1200s" "durée totale dans Slack"

inc_after_html="$(curl -fsS "http://127.0.0.1:${OBSERR_PORT}/incidents")"
assert_not_contains "$inc_after_html" "orders_mart" "Incident résolu retiré de /incidents"

detail_html="$(curl -fsS "http://127.0.0.1:${OBSERR_PORT}/jobs/dbt-prod/orders_mart")"
assert_contains "$detail_html" "Historique des Incidents" "section Incidents sur détail Job"
assert_contains "$detail_html" "closed" "statut closed dans l'historique du Job"
assert_contains "$detail_html" "1200s" "durée de l'Incident dans l'historique du Job"

echo "==> 9. Rechargement à chaud du TOML (modification Schedule, owner, tags et rotation de token de Source)..."
cat >"$TMP_DIR/obserr.toml" <<EOF
[server]
listen = "127.0.0.1:${OBSERR_PORT}"
database_path = "${TMP_DIR}/obserr.db"
public_url = "http://127.0.0.1:${OBSERR_PORT}"
timezone = "UTC"

[channel]
webhook_url = "http://127.0.0.1:${WEBHOOK_PORT}/slack"

[sources.dbt-prod]
token = "secret-dbt-token"

[sources.airflow-prod]
token = "rotated-airflow-token"

[jobs.dbt-prod.compta_nightly]
schedule = "30 3 * * *"
grace = "45m"
timezone = "Europe/Paris"
owner = "equipe-compta-v2"
tags = ["finance", "nightly", "hot-reload"]

[jobs.airflow-prod.adhoc_sync]
schedule = "0 6 * * *"
grace = "15m"
owner = "equipe-sre"
tags = ["sla", "critical"]
EOF

reloaded_dash="$(curl -fsS "http://127.0.0.1:${OBSERR_PORT}/")"
assert_contains "$reloaded_dash" "30 3 * * *" "Schedule mis à jour à chaud sur /"
assert_contains "$reloaded_dash" "equipe-compta-v2" "owner mis à jour à chaud sur /"
assert_contains "$reloaded_dash" "hot-reload" "tag mis à jour à chaud sur /"
assert_contains "$reloaded_dash" "0 6 * * *" "nouveau Schedule sur adhoc_sync"

adhoc_detail="$(curl -fsS "http://127.0.0.1:${OBSERR_PORT}/jobs/airflow-prod/adhoc_sync")"
assert_contains "$adhoc_detail" "OK" "adhoc_sync passé au statut OK après ajout de Schedule"
assert_not_contains "$adhoc_detail" "Sans Schedule" "adhoc_sync n'est plus Sans Schedule"

old_token_code="$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  -H "Authorization: Bearer secret-airflow-token" \
  -H "Content-Type: application/json" \
  -d '{"job":"finance_hourly","event":"started"}' \
  "http://127.0.0.1:${OBSERR_PORT}/api/v1/events")"
assert_eq "$old_token_code" "401" "ancien token révoqué à chaud"

echo "==> 10. Simulation d'un Run abandonné avec le nouveau token rechargé à chaud sur airflow-prod / finance_hourly..."
curl -fsS -X POST "http://127.0.0.1:${OBSERR_PORT}/api/v1/events" \
  -H "Authorization: Bearer rotated-airflow-token" \
  -H "Content-Type: application/json" \
  -d '{"job":"finance_hourly","event":"started","run_id":"dagrun-stuck","external_url":"https://airflow.internal/dags/finance_hourly/grid?dag_run_id=dagrun-stuck","timestamp":"2026-11-03T09:00:00Z"}' >/dev/null

curl -fsS -X POST "http://127.0.0.1:${OBSERR_PORT}/api/v1/events" \
  -H "Authorization: Bearer rotated-airflow-token" \
  -H "Content-Type: application/json" \
  -d '{"job":"finance_hourly","event":"started","run_id":"dagrun-next","timestamp":"2026-11-03T10:00:00Z"}' >/dev/null

webhook_count="$(wc -l <"$WEBHOOK_LOG" | tr -d ' ')"
assert_eq "$webhook_count" "3" "alerte Slack sur Run abandonné"

echo "✅ Smoke test local réussi (binaire obserr serve + SQLite sur disque + Schedules/Hot-reload + 3 webhooks Slack vérifiés)."

if [[ "$KEEP_RUNNING" == "true" ]]; then
  echo ""
  echo "🌐 Mode interactif actif :"
  echo "   - Tableau de bord  : http://127.0.0.1:${OBSERR_PORT}/"
  echo "   - Incidents ouverts: http://127.0.0.1:${OBSERR_PORT}/incidents"
  echo "   - Détail Job résolu: http://127.0.0.1:${OBSERR_PORT}/jobs/dbt-prod/compta_nightly"
  echo "   - Webhooks capturés: $WEBHOOK_LOG"
  echo "Appuyez sur Ctrl+C pour arrêter le serveur."
  wait "$SERVER_PID"
fi
