# obserr

## Interface web

`obserr serve --config obserr.toml` sert le tableau de bord (`/`), le détail
d'un Job (`/jobs/{source}/{name}`) et les incidents ouverts (`/incidents`).

```toml
[server]
# Omettre web_password pour laisser les pages accessibles sans authentification.
web_password = "remplacer-par-un-secret"
retention_days = 90

[sources.dbt-prod]
token = "remplacer-par-un-jeton"
```

L'authentification HTTP Basic accepte un nom d'utilisateur quelconque et le mot
de passe configuré. `/health` reste public et `/api/v1/events` utilise uniquement
les jetons Bearer des Sources. Le mot de passe et la rétention sont rechargeables
à chaud. Utiliser HTTPS via le proxy de déploiement pour protéger les identifiants
sur le réseau.

Les filtres État, Source et Tag du tableau de bord se combinent. Les tags sont
comparés exactement. Exemple : `/?status=incident&source=dbt-prod&tag=finance`.
Les états acceptés sont `incident`, `ok` et `unscheduled` (vide = tous).
Les trois vues se rafraîchissent toutes les 30 secondes avec htmx ; les filtres
courants sont conservés. Sans JavaScript, le bouton **Filtrer** reste utilisable.

À chaque `tick(now)` (toutes les 30 secondes en production), après l'évaluation
des Schedules, la purge retire les Runs terminés, les Missed Runs détectés et
les Incidents fermés strictement antérieurs à `now - retention_days`.
La valeur par défaut est 90 jours ; 0 conserve uniquement l'instant courant et
les exceptions suivantes : les Incidents ouverts, les Runs en cours et le dernier
Run reçu de chaque Job restent conservés. Les références aux anciens Runs purgés
sont effacées sans supprimer les Incidents encore ouverts.

## Vérification locale

```sh
cargo test
./sandbox/smoke_test.sh
# Laisser le serveur de démonstration accessible après les vérifications :
./sandbox/smoke_test.sh --keep-running
```

Le smoke test affiche le mot de passe de démonstration lorsqu'il reste actif.

## Bac à sable de bout en bout (dbt Core + Airflow)

Le dossier `sandbox/` fournit un environnement 100 % local (sans cloud) combinant :
- un projet **dbt Core** (`sandbox/dbt/`, Python 3.12 via `uv`, `dbt-duckdb` local) avec un modèle qui réussit (`ok_model`) et un modèle qui échoue à la demande (`flaky_model` avec `SANDBOX_DBT_FAIL=1`), exécuté via `obserr run --job dbt_sandbox -- uv run dbt run` ;
- une instance **Airflow** locale (`sandbox/docker-compose.yml`) avec le plugin listener monté dans `plugins/` et deux DAGs (`sandbox_ok` et `sandbox_fail`) poussant leurs événements vers `obserr serve` sur l'hôte ;
- un fichier de configuration de démonstration (`sandbox/obserr.demo.toml`) et un récepteur de webhooks Slack local (`sandbox/slack_receiver.py`).

```sh
./sandbox/e2e.sh
# Laisser le Dashboard (http://127.0.0.1:18090, obserr / sandbox) et Airflow (http://localhost:8080) actifs :
./sandbox/e2e.sh --keep-running
```

## Dépendance web embarquée

htmx 2.0.11 est embarqué dans `src/web/static/htmx.min.js`, avec sa licence
`htmx.LICENSE`. Aucun CDN n'est utilisé par le navigateur.
Source : https://cdn.jsdelivr.net/npm/htmx.org@2.0.11/dist/htmx.min.js
Documentation : https://htmx.org/docs/
