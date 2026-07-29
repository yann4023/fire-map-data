#!/usr/bin/env python3
"""
Collecte les positions d'avions autour de la Gironde via airplanes.live,
les ajoute à un historique JSON, et purge automatiquement tout ce qui a plus de 49h.

Ce script est prévu pour être exécuté périodiquement par une GitHub Action
(voir .github/workflows/collect-planes.yml), pas manuellement en continu.
"""

import json
import os
import time
import urllib.request

# Centre approximatif de la zone surveillée (Bordeaux / Gironde) et rayon en milles nautiques.
LAT = 44.84
LON = -0.58
RADIUS_NM = 150

DATA_FILE = "data/planes_history.json"
MAX_AGE_SECONDS = 49 * 3600  # purge au-delà de 49h


def fetch_planes():
    url = f"https://api.airplanes.live/v2/point/{LAT}/{LON}/{RADIUS_NM}"
    req = urllib.request.Request(url, headers={"User-Agent": "fire-map-collector/1.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
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


def main():
    now = int(time.time())
    aircraft = fetch_planes()

    history = load_history()

    # Purge des entrées trop anciennes.
    cutoff = now - MAX_AGE_SECONDS
    history = [snap for snap in history if snap["ts"] >= cutoff]

    # Nouvel instantané : uniquement les champs utiles, pour garder le fichier léger.
    snapshot = {
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
    history.append(snapshot)

    save_history(history)
    print(f"OK : {len(snapshot['ac'])} appareils, historique = {len(history)} instantanés")


if __name__ == "__main__":
    main()
