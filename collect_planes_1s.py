#!/usr/bin/env python3
"""
Collecte en continu les positions d'avions autour de la Gironde via airplanes.live,
à raison d'un point par seconde, avec purge automatique au-delà de 49h.

Contrairement à collect_planes.py (conçu pour cron), ce script tourne en boucle infinie
et doit être lancé comme service systemd (voir instructions fournies séparément).

Garde-fous ajoutés :
- Les envois vers GitHub sont groupés toutes les BATCH_SECONDS secondes (pas un commit
  par seconde), pour éviter de déclencher les limites anti-abus de GitHub sur les pushs.
- En cas d'erreur HTTP (ex: quota dépassé côté airplanes.live), le script ralentit
  automatiquement (backoff) au lieu d'insister au même rythme.
"""

import json
import os
import time
import subprocess
import urllib.request
import urllib.error

LAT = 44.84
LON = -0.58
RADIUS_NM = 150

DATA_FILE = "data/planes_history.json"
MAX_AGE_SECONDS = 49 * 3600
POLL_SECONDS = 1
BATCH_SECONDS = 10  # regroupe les commits/push toutes les 10s


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
    result = subprocess.run(
        ["git", "commit", "-m", "Auto update (1s)", "--quiet"], check=False
    )
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
    history = load_history()
    last_flush = time.time()
    backoff = POLL_SECONDS

    print(f"Démarrage collecte 1s (commits groupés toutes les {BATCH_SECONDS}s)...")

    while True:
        loop_start = time.time()
        try:
            aircraft = fetch_planes()
            now = int(time.time())
            history.append(make_snapshot(now, aircraft))
            backoff = POLL_SECONDS  # succès : on revient au rythme normal
        except urllib.error.HTTPError as e:
            print(f"Erreur HTTP {e.code} — ralentissement temporaire (backoff {backoff}s -> {min(backoff * 2, 60)}s)")
            backoff = min(backoff * 2, 60)
        except Exception as e:
            print(f"Erreur : {e} — nouvelle tentative dans {backoff}s")

        if time.time() - last_flush >= BATCH_SECONDS:
            cutoff = int(time.time()) - MAX_AGE_SECONDS
            history = [snap for snap in history if snap["ts"] >= cutoff]
            save_history(history)
            git_push()
            print(f"Flush : historique = {len(history)} instantanés")
            last_flush = time.time()

        elapsed = time.time() - loop_start
        time.sleep(max(0, backoff - elapsed))


if __name__ == "__main__":
    main()
