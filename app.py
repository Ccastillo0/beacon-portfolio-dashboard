"""
Beacon Portfolio Overview - backend para Azure Container Apps.

Sirve el HTML y expone /api/data (mismo shape que el dashboard) consultando el
SQL Warehouse de Databricks EN VIVO via la Statement Execution REST API, con cache.
Auth: OAuth M2M (service principal). El navegador nunca ve el token.
Si el warehouse esta frio o falla, /api/data responde rapido con el snapshot seed.json.
"""
import base64
import json
import os
import pathlib
import threading
import time

import requests
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

BASE = pathlib.Path(__file__).parent
HTML = BASE / "portfolio_overview.html"
SEED = json.loads((BASE / "seed.json").read_text(encoding="utf-8"))

DBX_HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
WAREHOUSE_ID = os.environ.get("DATABRICKS_HTTP_PATH", "").rstrip("/").split("/")[-1]
CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
CACHE_TTL = int(os.environ.get("CACHE_TTL_SECONDS", "1200"))   # 20 min
SQL_WAIT = os.environ.get("SQL_WAIT_TIMEOUT", "45s")

app = FastAPI(title="Beacon Portfolio Overview", docs_url=None, redoc_url=None)

_cache = {"data": None, "ts": 0.0}
_token = {"value": None, "exp": 0.0}
_lock = threading.Lock()


def _get_token():
    now = time.time()
    if _token["value"] and now < _token["exp"] - 60:
        return _token["value"]
    creds = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    r = requests.post(
        f"{DBX_HOST}/oidc/v1/token",
        headers={"Authorization": f"Basic {creds}",
                 "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        timeout=20,
    )
    if r.status_code != 200:
        raise RuntimeError(f"token HTTP {r.status_code}: {r.text[:200]} | url={DBX_HOST}/oidc/v1/token")
    j = r.json()
    _token["value"] = j["access_token"]
    _token["exp"] = now + j.get("expires_in", 3600)
    return _token["value"]


def _sql(query: str):
    """Ejecuta SQL via Statement Execution API. Devuelve lista de dicts."""
    tok = _get_token()
    r = requests.post(
        f"{DBX_HOST}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {tok}"},
        json={"warehouse_id": WAREHOUSE_ID, "statement": query,
              "wait_timeout": SQL_WAIT, "format": "JSON_ARRAY", "disposition": "INLINE"},
        timeout=90,
    )
    r.raise_for_status()
    res = r.json()
    state = res.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(f"statement state={state}: {res.get('status')}")
    cols = [c["name"] for c in res["manifest"]["schema"]["columns"]]
    rows = res.get("result", {}).get("data_array", []) or []
    return [dict(zip(cols, row)) for row in rows]


def _to_float(v):
    try:
        return round(float(v), 1)
    except (TypeError, ValueError):
        return None


def _build_occ():
    rows = _sql("""
        WITH latest AS (
          SELECT * FROM cat_prod.gold_analytics.gld_ydi_projected_occupancy
          WHERE report_date = (SELECT MAX(report_date)
                               FROM cat_prod.gold_analytics.gld_ydi_projected_occupancy)
            AND property_code IN ('12001','12002','12003','12005','13002','13003','13005',
                '13006','13007','13008r','37001','37002','37003','37004','37005','45001','45002')
        )
        SELECT property_name AS p,
               ROUND(MAX(CASE WHEN week_num=1  THEN occ_pct_current END)*100,1) AS cur,
               ROUND(MAX(CASE WHEN week_num=5  THEN occ_pct_end     END)*100,1) AS d30,
               ROUND(MAX(CASE WHEN week_num=9  THEN occ_pct_end     END)*100,1) AS d60,
               ROUND(MAX(CASE WHEN week_num=13 THEN occ_pct_end     END)*100,1) AS d90
        FROM latest GROUP BY property_name ORDER BY p
    """)
    return [{"p": r["p"], "cur": _to_float(r["cur"]), "d30": _to_float(r["d30"]),
             "d60": _to_float(r["d60"]), "d90": _to_float(r["d90"])} for r in rows]


def _build_data():
    data = dict(SEED)
    live = {}
    try:
        occ = _build_occ()
        if occ:
            data["occ"] = occ
            live["occ"] = "live"
        else:
            live["occ"] = "seed (sin filas)"
    except Exception as e:
        live["occ"] = f"seed (error: {str(e)[:80]})"
    data["_meta"] = {"updated_at": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
                     "sections": live}
    return data


@app.get("/health", response_class=PlainTextResponse)
def health():
    return "ok"


@app.get("/api/data")
def api_data():
    now = time.time()
    with _lock:
        if _cache["data"] is None or (now - _cache["ts"]) > CACHE_TTL:
            try:
                _cache["data"] = _build_data()
            except Exception as e:
                fallback = dict(SEED)
                fallback["_meta"] = {"updated_at": "snapshot",
                                     "sections": {"all": f"seed (error: {str(e)[:80]})"}}
                _cache["data"] = fallback
            _cache["ts"] = now
        payload = _cache["data"]
    return JSONResponse(payload)


@app.get("/")
def index():
    return FileResponse(HTML, media_type="text/html")
