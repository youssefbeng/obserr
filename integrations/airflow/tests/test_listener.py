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
    L.on_dag_run_success(dag_run(), "")


def test_http_error_does_not_raise(monkeypatch):
    s = _Server(status=500)
    monkeypatch.setenv("OBSERR_URL", s.url)
    try:
        L.on_dag_run_failed(dag_run(), "")
        assert len(s.requests) == 1
    finally:
        s.close()


def test_slow_server_times_out(monkeypatch):
    s = _Server(delay=1.0)
    monkeypatch.setenv("OBSERR_URL", s.url)
    monkeypatch.setattr(L, "TIMEOUT_SECONDS", 0.2)
    try:
        assert L.send_event({"job": "x"}) is False
    finally:
        s.close()


def test_broken_dag_run_does_not_raise(server):
    L.on_dag_run_running(object(), "")
    L.on_dag_run_success(object(), "")
