import http.server
import json
import threading
from types import SimpleNamespace

import pytest

import obserr_airflow_listener as L


class _Server:
    """Récepteur HTTP local enregistrant les requêtes reçues."""

    def __init__(self, status=200, delay=0.0):
        self.requests = []
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                outer.requests.append(
                    (self.path, self.headers.get("Authorization"), json.loads(body))
                )
                if delay:
                    import time

                    time.sleep(delay)
                self.send_response(status)
                self.end_headers()

            def log_message(self, *a):
                pass

        self.httpd = http.server.HTTPServer(("127.0.0.1", 0), H)
        self.url = "http://127.0.0.1:%d" % self.httpd.server_port
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()


@pytest.fixture
def server(monkeypatch):
    s = _Server()
    monkeypatch.setenv("OBSERR_URL", s.url)
    monkeypatch.setenv("OBSERR_TOKEN", "tok")
    monkeypatch.setenv("OBSERR_AIRFLOW_BASE_URL", "http://af:8080/")
    L._failed_tasks.clear()
    yield s
    s.close()


def dag_run(task_states=()):
    return SimpleNamespace(
        dag_id="etl",
        run_id="manual__2026",
        get_task_instances=lambda: [
            SimpleNamespace(task_id=t, state=st) for t, st in task_states
        ],
    )


def test_started_event_is_authenticated(server):
    L.on_dag_run_running(dag_run(), "")
    path, auth, body = server.requests[0]
    assert path == "/api/v1/events"
    assert auth == "Bearer tok"
    assert body == {"job": "etl", "event": "started", "run_id": "manual__2026"}


def test_success_event_has_url(server):
    L.on_dag_run_success(dag_run(), "")
    body = server.requests[0][2]
    assert body["event"] == "finished" and body["status"] == "succeeded"
    assert body["run_id"] == "manual__2026"
    assert body["external_url"].startswith("http://af:8080/dags/etl/")
    assert "manual__2026" in body["external_url"]
    assert "failed_step" not in body


def test_failure_uses_remembered_task_and_error(server):
    ti = SimpleNamespace(dag_id="etl", run_id="manual__2026", task_id="load")
    L.on_task_instance_failed(None, ti, *([ValueError("boom")] if L._airflow_major() >= 3 else []))
    L.on_dag_run_failed(dag_run(), "")
    body = server.requests[0][2]
    assert body["status"] == "failed" and body["failed_step"] == "load"
    if L._airflow_major() >= 3:
        assert "boom" in body["error_excerpt"]


def test_failure_falls_back_to_dag_run_task_instances(server):
    L.on_dag_run_failed(dag_run([("a", "success"), ("transform", "failed")]), "")
    assert server.requests[0][2]["failed_step"] == "transform"


def test_missing_url_does_not_raise(monkeypatch):
    monkeypatch.delenv("OBSERR_URL", raising=False)
    L.on_dag_run_running(dag_run(), "")
    assert L.send_event({"job": "x"}) is False


def test_unreachable_server_does_not_raise(monkeypatch):
    monkeypatch.setenv("OBSERR_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("OBSERR_TOKEN", "tok")
    L.on_dag_run_success(dag_run(), "")


def test_http_error_does_not_raise(monkeypatch):
    s = _Server(status=500)
    monkeypatch.setenv("OBSERR_URL", s.url)
    monkeypatch.setenv("OBSERR_TOKEN", "tok")
    try:
        L.on_dag_run_failed(dag_run(), "")
        assert len(s.requests) == len(L.RETRY_BACKOFFS)
    finally:
        s.close()


def test_http_401_is_not_retried(monkeypatch):
    s = _Server(status=401)
    monkeypatch.setenv("OBSERR_URL", s.url)
    monkeypatch.setenv("OBSERR_TOKEN", "bad")
    try:
        L.on_dag_run_failed(dag_run(), "")
        assert len(s.requests) == 1
    finally:
        s.close()


def test_truncate_keeps_last_20_lines():
    raw = "\n".join("line-%d" % i for i in range(1, 31))
    excerpt = L._truncate(raw)
    assert excerpt is not None
    lines = excerpt.splitlines()
    assert len(lines) == 20
    assert lines[0] == "line-11"
    assert lines[-1] == "line-30"


def test_failed_tasks_cache_is_bounded(monkeypatch):
    monkeypatch.setattr(L, "MAX_TRACKED_FAILURES", 3)
    L._failed_tasks.clear()
    for idx in range(5):
        ti = SimpleNamespace(dag_id="etl", run_id="run_%d" % idx, task_id="t_%d" % idx)
        L._remember_failure(ti, "err")
    assert len(L._failed_tasks) == 3
    assert ("etl", "run_0") not in L._failed_tasks
    assert ("etl", "run_4") in L._failed_tasks


def test_slow_server_times_out(monkeypatch):
    s = _Server(delay=1.0)
    monkeypatch.setenv("OBSERR_URL", s.url)
    monkeypatch.setenv("OBSERR_TOKEN", "tok")
    monkeypatch.setattr(L, "TIMEOUT_SECONDS", 0.2)
    try:
        assert L.send_event({"job": "x"}) is False
    finally:
        s.close()


def test_broken_dag_run_does_not_raise(server):
    L.on_dag_run_running(object(), "")
    L.on_dag_run_success(object(), "")


def test_missing_token_skips_request(server, monkeypatch):
    monkeypatch.delenv("OBSERR_TOKEN")
    assert L.send_event({"job": "x"}) is False
    assert server.requests == []


def test_enum_like_failed_state_is_detected(server):
    state = SimpleNamespace(value="failed")
    L.on_dag_run_failed(dag_run([("a", "success"), ("b", state)]), "")
    assert server.requests[0][2]["failed_step"] == "b"


def test_fallback_reads_error_excerpt_from_task_log(server, tmp_path, monkeypatch):
    d = tmp_path / "dag_id=etl" / "run_id=manual__2026" / "task_id=transform"
    d.mkdir(parents=True)
    (d / "attempt=1.log").write_text("old attempt\n")
    (d / "attempt=2.log").write_text("start\nValueError: bad column\n")
    monkeypatch.setattr(L, "_log_folder", lambda: str(tmp_path))
    L.on_dag_run_failed(dag_run([("transform", "failed")]), "")
    body = server.requests[0][2]
    assert body["failed_step"] == "transform"
    assert "ValueError: bad column" in body["error_excerpt"]
    assert "old attempt" not in body["error_excerpt"]


def test_missing_task_log_is_ignored(server, tmp_path, monkeypatch):
    monkeypatch.setattr(L, "_log_folder", lambda: str(tmp_path))
    L.on_dag_run_failed(dag_run([("transform", "failed")]), "")
    assert "error_excerpt" not in server.requests[0][2]


def test_fallback_uses_loaded_task_instances_when_new_session_commit_is_prohibited(server):
    def forbidden_new_session():
        raise RuntimeError("UNEXPECTED COMMIT - THIS WILL BREAK HA LOCKS!")

    dr = SimpleNamespace(
        dag_id="etl",
        run_id="manual__2026",
        task_instances=[SimpleNamespace(task_id="boom", state="failed")],
        get_task_instances=forbidden_new_session,
    )
    L.on_dag_run_failed(dr, "")
    assert server.requests[0][2]["failed_step"] == "boom"


def test_python_deliverables_have_docstrings():
    import ast
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[3]
    for rel in (
        "integrations/airflow/obserr_airflow_listener.py",
        "sandbox/slack_receiver.py",
        "sandbox/airflow/dags/sandbox_dags.py",
    ):
        tree = ast.parse((root / rel).read_text(encoding="utf-8"), filename=rel)
        assert ast.get_docstring(tree), f"{rel}: docstring de module manquante"
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                assert ast.get_docstring(node), f"{rel}:{node.lineno} {node.name} sans docstring"
                if isinstance(node, ast.ClassDef):
                    for item in node.body:
                        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                            assert ast.get_docstring(item), (
                                f"{rel}:{item.lineno} {node.name}.{item.name} sans docstring"
                            )


