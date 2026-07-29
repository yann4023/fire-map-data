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
  GET /wind    -> vent ponctuel via met.no (repli indépendant d'Open-Meteo, mis en cache 30 min)

À lancer comme service systemd (voir instructions fournies séparément).
"""

import json
import os
import time
import subprocess
import threading
import urllib.request
import urllib.error
import urllib.parse
import base64
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LAT = 44.84
LON = -0.58
RADIUS_NM = 150

DATA_FILE = "data/planes_history.json"
MAX_AGE_SECONDS = 49 * 3600
POLL_SECONDS = 1
BATCH_SECONDS = 3       # regroupe les commits/push GitHub
IN_MEMORY_WINDOW_SECONDS = 60  # 1 min de tampon — le client fait son propre historique de session + backfill GitHub, inutile de renvoyer plus
HTTP_PORT = 8080

MET_NO_LAT = 44.84
MET_NO_LON = -0.58
MET_NO_USER_AGENT = "fire-map-personal-project (contact: yann4023@hotmail.com)"
MET_NO_CACHE_SECONDS = 30 * 60  # 30 min : les prévisions met.no ne changent pas assez vite pour justifier plus fréquent

_wind_cache = {"data": None, "fetched_at": 0}


def fetch_met_no_wind():
    now = time.time()
    if _wind_cache["data"] is not None and now - _wind_cache["fetched_at"] < MET_NO_CACHE_SECONDS:
        return _wind_cache["data"]

    url = f"https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={MET_NO_LAT}&lon={MET_NO_LON}"
    req = urllib.request.Request(url, headers={"User-Agent": MET_NO_USER_AGENT})
    with urllib.request.urlopen(req, timeout=10) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    instant = payload["properties"]["timeseries"][0]["data"]["instant"]["details"]
    result = {
        "speed_ms": instant.get("wind_speed"),
        "direction_deg": instant.get("wind_from_direction"),
        "updated_at": int(now),
    }
    _wind_cache["data"] = result
    _wind_cache["fetched_at"] = now
    return result


# --- EUMETSAT / Meteosat détection rapide (Active Fire Monitoring, MSG - 0 degré) ---
# Complète NASA FIRMS (VIIRS/MODIS, 2-4 passages/jour) avec un satellite géostationnaire :
# résolution plus grossière (~3-4 km/pixel) mais cycle de répétition bien plus rapide (5-15 min).
# Remplace TON_CONSUMER_KEY / TON_CONSUMER_SECRET par les identifiants récupérés sur
# https://api.eumetsat.int/api-key/ avant de déployer.
EUMETSAT_CONSUMER_KEY = os.environ.get("EUMETSAT_CONSUMER_KEY", "")
EUMETSAT_CONSUMER_SECRET = os.environ.get("EUMETSAT_CONSUMER_SECRET", "")
EUMETSAT_TOKEN_URL = "https://api.eumetsat.int/token"
EUMETSAT_BROWSE_URL = "https://api.eumetsat.int/data/search-products/1.0.0/os"  # OpenSearch — confirmé sans authentification requise
EUMETSAT_DOWNLOAD_BASE = "https://api.eumetsat.int/data/download/collections"
EUMETSAT_COLLECTION_ID = "EO:EUM:DAT:0801"  # Active Fire Monitoring (CAP) - MTG - 0 degree — le seul confirmé en format CAP ; EO:EUM:DAT:MSG:FIR est en réalité en GRIB, incompatible avec le parsing XML ci-dessous
EUMETSAT_BBOX = "-5.5,41,10,51.5"  # même emprise que FRANCE_BBOX (west,south,east,north)
EUMETSAT_CACHE_SECONDS = 5 * 60  # produit généré toutes les ~15 min ; 5 min de cache est prudent

_eumetsat_token_cache = {"token": None, "expires_at": 0}
_eumetsat_fires_cache = {"data": None, "fetched_at": 0}


def eumetsat_request(req, timeout, label):
    """Exécute une requête et journalise l'URL + le corps de la réponse d'erreur en cas d'échec —
    sans ça, un 404/403/etc. générique ne dit pas laquelle des 3 requêtes (jeton/recherche/
    téléchargement) est en cause ni pourquoi."""
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:500]
        print(f"EUMETSAT [{label}] échec HTTP {e.code} sur {req.full_url}\nCorps de la réponse : {body}")
        raise
    except urllib.error.URLError as e:
        print(f"EUMETSAT [{label}] échec réseau sur {req.full_url} : {e.reason}")
        raise


def get_eumetsat_token():
    now = time.time()
    if not EUMETSAT_CONSUMER_KEY or not EUMETSAT_CONSUMER_SECRET:
        raise RuntimeError("EUMETSAT_CONSUMER_KEY / EUMETSAT_CONSUMER_SECRET non définies (variables d'environnement manquantes — voir configuration du service systemd).")
    # Marge de 60s avant l'expiration réelle pour éviter d'utiliser un jeton tout juste périmé.
    if _eumetsat_token_cache["token"] and now < _eumetsat_token_cache["expires_at"] - 60:
        return _eumetsat_token_cache["token"]

    credentials = base64.b64encode(f"{EUMETSAT_CONSUMER_KEY}:{EUMETSAT_CONSUMER_SECRET}".encode()).decode()
    req = urllib.request.Request(
        EUMETSAT_TOKEN_URL,
        data=b"grant_type=client_credentials",
        headers={"Authorization": f"Basic {credentials}", "Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    raw = eumetsat_request(req, 15, "token")
    payload = json.loads(raw.decode("utf-8"))
    _eumetsat_token_cache["token"] = payload["access_token"]
    _eumetsat_token_cache["expires_at"] = now + payload.get("expires_in", 3600)
    return _eumetsat_token_cache["token"]


def parse_cap_polygon(text):
    if not text:
        return None
    coords = []
    for pair in text.strip().split():
        try:
            lat, lon = pair.split(",")
            coords.append([float(lat), float(lon)])
        except ValueError:
            continue
    return coords if len(coords) >= 3 else None


def parse_cap_circle(text):
    if not text:
        return None
    try:
        point, radius = text.strip().split()
        lat, lon = point.split(",")
        return {"lat": float(lat), "lon": float(lon), "radius_km": float(radius)}
    except ValueError:
        return None


def parse_cap_fires(raw_bytes):
    """Extrait les zones de feu d'un fichier CAP (Common Alert Protocol, XML, standard OASIS 1.2).
    Analyse défensive : le schéma exact de ce produit précis n'a pas pu être vérifié avant
    déploiement (compte EUMETSAT créé pendant cette session) — si rien n'est trouvé, un message
    est journalisé pour faciliter l'ajustement une fois un vrai fichier observé."""
    fires = []
    try:
        root = ET.fromstring(raw_bytes)
    except ET.ParseError as e:
        print(f"CAP EUMETSAT : échec de parsing XML ({e}) — le fichier n'est peut-être pas au format attendu.")
        return fires

    ns = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}
    for info in root.findall(".//cap:info", ns):
        headline = info.findtext("cap:headline", default="", namespaces=ns)
        severity = info.findtext("cap:severity", default="", namespaces=ns)
        for area in info.findall("cap:area", ns):
            area_desc = area.findtext("cap:areaDesc", default="", namespaces=ns)
            for poly_el in area.findall("cap:polygon", ns):
                coords = parse_cap_polygon(poly_el.text)
                if coords:
                    fires.append({"type": "polygon", "coords": coords, "headline": headline, "severity": severity, "areaDesc": area_desc})
            for circle_el in area.findall("cap:circle", ns):
                cr = parse_cap_circle(circle_el.text)
                if cr:
                    fires.append({"type": "circle", **cr, "headline": headline, "severity": severity, "areaDesc": area_desc})

    if not fires:
        print("CAP EUMETSAT : aucune zone de feu trouvée dans ce cycle (normal si pas de détection en cours, ou schéma CAP à ajuster).")
    return fires


def fetch_eumetsat_fires():
    now = time.time()
    if _eumetsat_fires_cache["data"] is not None and now - _eumetsat_fires_cache["fetched_at"] < EUMETSAT_CACHE_SECONDS:
        return _eumetsat_fires_cache["data"]

    # L'API OpenSearch ne nécessite pas d'authentification (confirmé par la documentation
    # EUMETSAT) — seul le téléchargement du produit en a besoin. Paramètres volontairement
    # limités à ceux confirmés (pi, bbox, format) : les autres (tri, pagination) n'ont pas pu
    # être vérifiés avant déploiement et risquaient de provoquer une autre erreur 404/400.
    search_url = (
        f"{EUMETSAT_BROWSE_URL}?format=json&pi={urllib.parse.quote(EUMETSAT_COLLECTION_ID)}"
        f"&bbox={EUMETSAT_BBOX}"
    )
    sreq = urllib.request.Request(search_url)
    search_raw = eumetsat_request(sreq, 15, "recherche")
    search_result = json.loads(search_raw.decode("utf-8"))

    features = search_result.get("features", [])
    if not features:
        result = {"fires": [], "product_time": None}
        _eumetsat_fires_cache["data"] = result
        _eumetsat_fires_cache["fetched_at"] = now
        return result

    # Pas de tri serveur confirmé : on prend l'entrée la plus récente selon la date de
    # publication/acquisition présente dans les propriétés (le nom exact du champ peut varier
    # selon la collection, plusieurs candidats sont donc essayés).
    def feature_time(f):
        props = f.get("properties", {})
        return props.get("date") or props.get("published") or props.get("updated") or ""
    features.sort(key=feature_time, reverse=True)

    latest = features[0]
    product_id = latest["properties"]["identifier"]
    product_time = feature_time(latest)

    token = get_eumetsat_token()
    download_url = f"{EUMETSAT_DOWNLOAD_BASE}/{urllib.parse.quote(EUMETSAT_COLLECTION_ID, safe='')}/products/{urllib.parse.quote(product_id, safe='')}"
    dreq = urllib.request.Request(download_url, headers={"Authorization": f"Bearer {token}"})
    raw = eumetsat_request(dreq, 20, "téléchargement")

    fires = parse_cap_fires(raw)
    result = {"fires": fires, "product_time": product_time}
    _eumetsat_fires_cache["data"] = result
    _eumetsat_fires_cache["fetched_at"] = now
    return result


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
        elif self.path.startswith("/wind"):
            try:
                self._send_json(fetch_met_no_wind())
            except Exception as e:
                print(f"Erreur proxy met.no : {e}")
                self.send_response(502)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
        elif self.path.startswith("/meteosat-fires"):
            try:
                self._send_json(fetch_eumetsat_fires())
            except Exception as e:
                print(f"Erreur proxy EUMETSAT : {e}")
                self.send_response(502)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # silence les logs HTTP par défaut, on a déjà nos propres messages


def run_http_server():
    server = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Handler)
    print(f"Serveur HTTP direct démarré sur le port {HTTP_PORT} (/recent, /latest, /wind)")
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
                "category": a.get("category"),
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
