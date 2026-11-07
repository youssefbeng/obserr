#!/usr/bin/env bash
# Scénario de bout en bout : dbt Core (uv) + Airflow (Docker Compose) face à obserr natif.
# Prérequis : cargo, uv, python3, docker (daemon actif), curl.
set -euo pipefail

export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
SANDBOX_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SANDBOX_DIR")"
RUN_DIR="$SANDBOX_DIR/.run"
BIN="$ROOT_DIR/target/debug/obserr"
URL="http://127.0.0.1:18090"
AUTH="obserr:sandbox"
PIDS=()

# Arrête obserr, le récepteur Slack et Airflow à la sortie.
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  docker compose -f "$SANDBOX_DIR/docker-compose.yml" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Échoue clairement si un prérequis manque.
need() { command -v "$1" >/dev/null || { echo "❌ prérequis manquant : $1" >&2; exit 1; }; }
# Vérifie qu'une page contient une chaîne attendue.
expect() { [[ "$1" == *"$2"* ]] || { echo "❌ ÉCHEC [$3] : '$2' introuvable" >&2; exit 1; }; }
# Interroge une page authentifiée du dashboard.
page() { curl -fsS -u "$AUTH" "$URL$1"; }

for t in cargo uv python3 docker curl; do need "$t"; done
docker info >/dev/null 2>&1 || { echo "❌ le daemon Docker ne répond pas" >&2; exit 1; }

rm -rf "$RUN_DIR"; mkdir -p "$RUN_DIR"
cargo build --manifest-path "$ROOT_DIR/Cargo.toml" --quiet

echo "==> 1. Récepteur Slack + obserr serve"
python3 "$SANDBOX_DIR/slack_receiver.py" 18091 "$RUN_DIR/slack.jsonl" >/dev/null & PIDS+=($!)
(cd "$RUN_DIR" && exec "$BIN" serve --config "$SANDBOX_DIR/obserr.demo.toml") & PIDS+=($!)
for _ in $(seq 50); do curl -fsS "$URL/health" >/dev/null 2>&1 && break; sleep 0.2; done
curl -fsS "$URL/health" >/dev/null

echo "==> 2. dbt Core via uv : succès puis échec (Python 3.12)"
export OBSERR_URL="$URL" OBSERR_TOKEN=sandbox-dbt-token
cd "$SANDBOX_DIR/dbt"
export DBT_PROFILES_DIR="$PWD"
"$BIN" run --job dbt_sandbox -- uv run dbt run
set +e
SANDBOX_DBT_FAIL=1 "$BIN" run --job dbt_sandbox -- uv run dbt run >/dev/null 2>&1
code=$?
set -e
[[ $code -ne 0 ]] || { echo "❌ le run dbt en échec devait retourner un code non nul" >&2; exit 1; }
expect "$(page /incidents)" "dbt_sandbox" "Incident ouvert (dbt)"
expect "$(cat "$RUN_DIR/slack.jsonl")" "dbt_sandbox" "alerte Slack dbt"
"$BIN" run --job dbt_sandbox -- uv run dbt run
if page /incidents | grep -q "dbt_sandbox"; then
  echo "❌ l'Incident dbt_sandbox devrait être fermé" >&2; exit 1
fi
expect "$(page /jobs/dbt-sandbox/dbt_sandbox)" "closed" "Incident dbt fermé dans l'historique"
expect "$(cat "$RUN_DIR/slack.jsonl")" "✅ Résolu" "alerte Slack de résolution dbt"

echo "==> 3. Airflow sous Docker Compose"
DC=(docker compose -f "$SANDBOX_DIR/docker-compose.yml")
"${DC[@]}" up -d
for _ in $(seq 90); do
  curl -fsS http://localhost:8080/health >/dev/null 2>&1 && break; sleep 2
done
"${DC[@]}" exec -T airflow airflow dags trigger sandbox_ok
"${DC[@]}" exec -T airflow airflow dags trigger sandbox_fail
for _ in $(seq 60); do
  if page /incidents | grep -q "sandbox_fail" && page /jobs/airflow-prod/sandbox_ok | grep -q "succeeded"; then
    break
  fi
  sleep 2
done
expect "$(page /jobs/airflow-prod/sandbox_ok)" "succeeded" "Run Airflow réussi"
fail_page="$(page /jobs/airflow-prod/sandbox_fail)"
expect "$fail_page" "boom" "Step Airflow en échec"
expect "$fail_page" "localhost:8080" "lien vers l'UI Airflow"
expect "$(cat "$RUN_DIR/slack.jsonl")" "sandbox_fail" "alerte Slack Airflow"

echo "✅ Bac à sable de bout en bout réussi (dbt Core + Airflow + Incidents + Slack)."
if [[ "${1:-}" == "--keep-running" ]]; then
  echo "🌐 Dashboard : $URL (obserr / sandbox) — Airflow : http://localhost:8080 — Ctrl+C pour arrêter"
  wait "${PIDS[0]}"
fi
