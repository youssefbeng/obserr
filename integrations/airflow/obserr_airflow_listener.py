"""Plugin Airflow (2.5+ et 3.x) qui envoie les DAG runs à obserr (`started` / `finished`).

À déposer tel quel dans le dossier `plugins/` d'Airflow. Configuration par variables
d'environnement : `OBSERR_URL`, `OBSERR_TOKEN` (et `OBSERR_AIRFLOW_BASE_URL` en option pour
forcer l'URL publique de l'interface Airflow). Seule la bibliothèque standard est utilisée ;
toute erreur (réseau, HTTP, Airflow) est journalisée en simple avertissement (ADR-0004).
"""

import json
import logging
import os
import sys
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

log = logging.getLogger("obserr.airflow_listener")

TIMEOUT_SECONDS = 3.0
EXCERPT_MAX_CHARS = 500
LOG_TAIL_LINES = 10

# Échecs de tâches vus dans ce processus, par (dag_id, run_id) -> (task_id, extrait).
_failed_tasks: Dict[Tuple[str, str], Tuple[str, Optional[str]]] = {}

try:  # Airflow absent lors des tests unitaires : le décorateur devient neutre.
    from airflow.listeners import hookimpl
except Exception:  # pragma: no cover - dépend de l'environnement

    def hookimpl(func):  # type: ignore[no-redef]
        return func


def _airflow_major() -> int:
    """Retourne la version majeure d'Airflow (2 par défaut) pour adapter les signatures de hooks."""
    try:
        import airflow

        return int(str(airflow.__version__).split(".")[0])
    except Exception:
        return 2


def _truncate(text: Any) -> Optional[str]:
    """Garde la fin du message d'erreur (la plus informative) et le borne à une taille courte."""
    if text is None:
        return None
    value = str(text).strip()
    if not value:
        return None
    return value[-EXCERPT_MAX_CHARS:]


def _airflow_base_url() -> str:
    """Détermine l'URL publique de l'interface Airflow (variable d'env, sinon configuration Airflow)."""
    override = os.environ.get("OBSERR_AIRFLOW_BASE_URL")
    if override:
        return override.rstrip("/")
    try:
        from airflow.configuration import conf

        for section, key in (("api", "base_url"), ("webserver", "base_url")):
            try:
                value = conf.get(section, key, fallback=None)
            except Exception:
                value = None
            if value:
                return str(value).rstrip("/")
    except Exception:
        pass
    return "http://localhost:8080"


def build_dag_run_url(dag_id: str, run_id: str) -> str:
    """Construit le lien direct vers le DAG run (chemin différent entre Airflow 2.x et 3.x)."""
    base = _airflow_base_url()
    dag = quote(str(dag_id), safe="")
    run = quote(str(run_id), safe="")
    if _airflow_major() >= 3:
        return "%s/dags/%s/runs/%s" % (base, dag, run)
    return "%s/dags/%s/grid?dag_run_id=%s" % (base, dag, run)


def send_event(payload: Dict[str, Any]) -> bool:
    """Poste un événement à obserr ; ne lève jamais d'exception et retourne si l'envoi a réussi."""
    try:
        url = os.environ.get("OBSERR_URL")
        if not url:
            log.warning("obserr: OBSERR_URL absent, événement ignoré")
            return False
        token = os.environ.get("OBSERR_TOKEN")
        if not token:
            log.warning("obserr: OBSERR_TOKEN absent, événement ignoré")
            return False
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + token,
        }
        request = urllib.request.Request(
            url.rstrip("/") + "/api/v1/events",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS):
            pass
        return True
    except urllib.error.HTTPError as exc:
        log.warning("obserr: réponse HTTP %s, événement ignoré", exc.code)
    except Exception as exc:
        log.warning("obserr: envoi impossible (%s), événement ignoré", exc)
    return False


def _ids(dag_run: Any) -> Tuple[str, str]:
    """Extrait `(dag_id, run_id)` d'un objet DAG run."""
    return str(dag_run.dag_id), str(dag_run.run_id)


def _log_folder() -> Optional[str]:
    """Retourne le dossier des logs de tâches d'Airflow (None si indisponible)."""
    try:
        from airflow.configuration import conf

        return conf.get("logging", "base_log_folder", fallback=None)
    except Exception:
        return None


def _read_task_log_excerpt(dag_id: str, run_id: str, task_id: str) -> Optional[str]:
    """Lit la fin du dernier log local de la tâche (le scheduler n'a pas l'erreur en mémoire).

    Ne fonctionne que si les logs sont lisibles depuis ce processus (système de fichiers partagé).
    """
    try:
        base = _log_folder()
        if not base:
            return None
        folder = os.path.join(
            base, "dag_id=%s" % dag_id, "run_id=%s" % run_id, "task_id=%s" % task_id
        )
        attempts = sorted(
            (n for n in os.listdir(folder) if n.startswith("attempt=") and n.endswith(".log")),
            key=lambda n: int(n[len("attempt="):-len(".log")] or 0),
        )
        if not attempts:
            return None
        with open(os.path.join(folder, attempts[-1]), "r", encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
        return _truncate("\n".join(lines[-LOG_TAIL_LINES:]))
    except Exception:
        return None


def _find_failed_task(dag_run: Any, dag_id: str, run_id: str) -> Tuple[Optional[str], Optional[str]]:
    """Retrouve la tâche en échec : mémoire du processus, sinon interrogation du DAG run."""
    known = _failed_tasks.pop((dag_id, run_id), None)
    if known:
        return known
    try:
        for ti in dag_run.get_task_instances():
            state = getattr(ti, "state", "")
            if str(getattr(state, "value", state)) == "failed":
                task_id = str(ti.task_id)
                return task_id, _read_task_log_excerpt(dag_id, run_id, task_id)
    except Exception as exc:
        log.warning("obserr: tâche en échec introuvable (%s)", exc)
    return None, None


def _on_running(dag_run: Any) -> None:
    """Envoie `started` pour le DAG run (job = dag_id, run_id = run_id)."""
    try:
        dag_id, run_id = _ids(dag_run)
        send_event({"job": dag_id, "event": "started", "run_id": run_id})
    except Exception as exc:
        log.warning("obserr: hook started ignoré (%s)", exc)


def _on_finished(dag_run: Any, status: str) -> None:
    """Envoie `finished` avec le statut, le lien Airflow et, en échec, le `Step` fautif."""
    try:
        dag_id, run_id = _ids(dag_run)
        payload: Dict[str, Any] = {
            "job": dag_id,
            "event": "finished",
            "run_id": run_id,
            "status": status,
            "external_url": build_dag_run_url(dag_id, run_id),
        }
        if status == "failed":
            task_id, excerpt = _find_failed_task(dag_run, dag_id, run_id)
            if task_id:
                payload["failed_step"] = task_id
            if excerpt:
                payload["error_excerpt"] = excerpt
        else:
            _failed_tasks.pop((dag_id, run_id), None)
        send_event(payload)
    except Exception as exc:
        log.warning("obserr: hook finished ignoré (%s)", exc)


def _remember_failure(task_instance: Any, error: Any) -> None:
    """Mémorise la tâche en échec (et son erreur) pour l'envoi de `finished` du DAG run."""
    try:
        key = (str(task_instance.dag_id), str(task_instance.run_id))
        _failed_tasks.setdefault(key, (str(task_instance.task_id), _truncate(error)))
    except Exception as exc:
        log.warning("obserr: échec de tâche non mémorisé (%s)", exc)


@hookimpl
def on_dag_run_running(dag_run, msg=None):
    """Hook Airflow : un DAG run démarre."""
    _on_running(dag_run)


@hookimpl
def on_dag_run_success(dag_run, msg=None):
    """Hook Airflow : un DAG run se termine avec succès."""
    _on_finished(dag_run, "succeeded")


@hookimpl
def on_dag_run_failed(dag_run, msg=None):
    """Hook Airflow : un DAG run se termine en échec."""
    _on_finished(dag_run, "failed")


# Airflow 3 transmet `error` à ce hook ; Airflow 2.5+ n'en a pas (`session` ignoré). Les arguments
# sans défaut sont ceux que pluggy fournit, d'où deux signatures selon la version.
if _airflow_major() >= 3:

    @hookimpl
    def on_task_instance_failed(previous_state, task_instance, error):  # type: ignore[no-redef]
        """Hook Airflow 3 : une tâche échoue, on retient son `task_id` et l'erreur."""
        _remember_failure(task_instance, error)

else:

    @hookimpl
    def on_task_instance_failed(previous_state, task_instance):  # type: ignore[no-redef]
        """Hook Airflow 2.5+ : une tâche échoue, on retient son `task_id`."""
        _remember_failure(task_instance, None)


try:
    from airflow.plugins_manager import AirflowPlugin

    class ObserrListenerPlugin(AirflowPlugin):
        """Enregistre ce module comme listener Airflow."""

        name = "obserr_listener"
        listeners = [sys.modules[__name__]]

except Exception:  # pragma: no cover - Airflow absent (tests)
    pass
