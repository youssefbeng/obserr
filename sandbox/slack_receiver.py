"""Récepteur local de webhooks Slack : affiche et journalise chaque payload JSON reçu."""
import http.server
import sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 18091
LOG = sys.argv[2] if len(sys.argv) > 2 else "slack_webhooks.jsonl"


class Handler(http.server.BaseHTTPRequestHandler):
    """Gestionnaire HTTP minimal simulant un Incoming Webhook Slack."""

    def do_POST(self):
        """Enregistre le corps de la requête POST puis répond 200 comme Slack."""
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8", "replace")
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(body.replace("\n", "\\n") + "\n")
        print("[slack]", body, flush=True)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        """Silence les journaux d'accès HTTP standard."""
        pass


http.server.HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
