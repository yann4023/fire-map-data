#!/usr/bin/env python3
"""
Collecte en continu les positions d'avions (1 point/s) ET sert ces données directement
via un petit serveur HTTP intégré, pour contourner le cache d'environ 5 minutes imposé
par raw.githubusercontent.com sur les URLs GitHub.

- GitHub reste l'archive persistante 49h (poussée toutes les BATCH_SECONDS secondes).
- Ce serveur HTTP local sert les données "fraîches" en mémoire, sans ce délai de cache.

Endpoints :
  GET /recent  -> liste JSON des instantanés des 10 dernières minutes (mémoire, pas de cache)
  GET /latest  -> le tout dernier instantané uniquement

À lancer comme service systemd (voir instructions fournies séparément).
"""

import json
import os
import time
import subprocess
import threading
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LAT = 44.84
LON = -0.58
RADIUS_NM = 150

DATA_FILE = "data/planes_history.json"
MAX_AGE_SECONDS = 49 * 3600
POLL_SECONDS = 1
BATCH_SECONDS = 3       # regroupe les commits/push GitHub
IN_MEMORY_WINDOW_SECONDS = 10 * 60  # 10 min gardées en mémoire pour le serveur HTTP direct
HTTP_PORT = 8080

# --- État partagé entre la boucle de collecte et le serveur HTTP ---
lock = threading.Lock()
recent_snapshots = []  # liste des instantanés des IN_MEMORY_WINDOW_SECONDS dernières secondes


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")  # nécessaire pour que le navigateur puisse lire
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        with lock:
            snapshots = list(recent_snapshots)
        if self.path.startswith("/latest"):
            self._send_json(snapshots[-1] if snapshots else {})
        elif self.path.startswith("/recent"):
            self._send_json(snapshots)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # silence les logs HTTP par défaut, on a déjà nos propres messages


def run_http_server():
    server = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Handler)
    print(f"Serveur HTTP direct démarré sur le port {HTTP_PORT} (/recent, /latest)")
    server.serve_forever()


def fetch_planes():
    url = f"https://api.airplanes.live/v2/point/{LAT}/{LON}/{RADIUS_NM}"
    req = urllib.request.Request(url, headers={"User-Agent": "fire-map-collector/1.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data.get("ac", [])


def load_history():
    if not os.path.exists(DATA_FILE):
        return []
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return []


def save_history(history):
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, separators=(",", ":"))


def git_push():
    subprocess.run(["git", "add", DATA_FILE], check=False)
    result = subprocess.run(["git", "commit", "-m", "Auto update (1s)", "--quiet"], check=False)
    if result.returncode == 0:
        subprocess.run(["git", "push", "--quiet"], check=False)


def make_snapshot(now, aircraft):
    return {
        "ts": now,
        "ac": [
            {
                "hex": a.get("hex"),
                "flight": (a.get("flight") or "").strip(),
                "r": a.get("r"),
                "t": a.get("t"),
                "lat": a.get("lat"),
                "lon": a.get("lon"),
                "alt_baro": a.get("alt_baro"),
                "gs": a.get("gs"),
                "track": a.get("track"),
                "dbFlags": a.get("dbFlags"),
            }
            for a in aircraft
            if isinstance(a.get("lat"), (int, float)) and isinstance(a.get("lon"), (int, float))
        ],
    }


def main():
    threading.Thread(target=run_http_server, daemon=True).start()

    history = load_history()
    last_flush = time.time()
    backoff = POLL_SECONDS

    print(f"Démarrage collecte 1s (commits GitHub groupés toutes les {BATCH_SECONDS}s, HTTP direct port {HTTP_PORT})...")

    while True:
        loop_start = time.time()
        try:
            aircraft = fetch_planes()
            now = int(time.time())
            snap = make_snapshot(now, aircraft)
            history.append(snap)
            with lock:
                recent_snapshots.append(snap)
                cutoff_mem = now - IN_MEMORY_WINDOW_SECONDS
                while recent_snapshots and recent_snapshots[0]["ts"] < cutoff_mem:
                    recent_snapshots.pop(0)
            backoff = POLL_SECONDS
        except urllib.error.HTTPError as e:
            print(f"Erreur HTTP {e.code} — ralentissement (backoff {backoff}s -> {min(backoff * 2, 60)}s)")
            backoff = min(backoff * 2, 60)
        except Exception as e:
            print(f"Erreur : {e} — nouvelle tentative dans {backoff}s")

        if time.time() - last_flush >= BATCH_SECONDS:
            cutoff = int(time.time()) - MAX_AGE_SECONDS
            history = [s for s in history if s["ts"] >= cutoff]
            save_history(history)
            git_push()
            print(f"Flush GitHub : historique = {len(history)} instantanés | mémoire directe = {len(recent_snapshots)}")
            last_flush = time.time()

        elapsed = time.time() - loop_start
        time.sleep(max(0, backoff - elapsed))


if __name__ == "__main__":
    main()
