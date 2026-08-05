"""
Beacon Portfolio Overview - backend para Azure Container Apps.

Sirve el HTML y expone /api/data, que arma el objeto D (mismo shape que el
dashboard) consultando el SQL Warehouse de Databricks EN VIVO, con cache TTL.

Autenticacion a Databricks: OAuth M2M (service principal sp-beacon-portfolio-app).
El navegador NUNCA ve el token: solo llama a /api/data en este backend.

Estado de las secciones (2026-08-05):
  - occ  -> EN VIVO desde cat_prod.gold_analytics.gld_ydi_projected_occupancy (validado)
  - resto (ren, wo, deld, fun, turn, roll, units, short, months) -> por ahora del
    snapshot seed.json; se iran cableando una por una (cada query se valida vs Yardi).
"""
import json
import os
import pathlib
import threading
import time

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

BASE = pathlib.Path(__file__).parent
HTML = BASE / "portfolio_overview.html"
SEED = json.loads((BASE / "seed.json").read_text(encoding="utf-8"))

# --- config Databricks (viene de variables de entorno del Container App) ---
DBX_HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
DBX_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "")          # /sql/1.0/warehouses/<id>
DBX_CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID", "")
DBX_CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
CACHE_TTL = int(os.environ.get("CACHE_TTL_SECONDS", "1200"))        # 20 min

app = FastAPI(title="Beacon Portfolio Overview", docs_url=None, redoc_url=None)

_cache = {"data": None, "ts": 0.0, "source": "none"}
_lock = threading.Lock()


def _sql(query: str):
    """Ejecuta SQL en el warehouse via OAuth M2M. Devuelve lista de dicts."""
    from databricks import sql
    with sql.connect(
        server_hostname=DBX_HOST.replace("https://", ""),
        http_path=DBX_HTTP_PATH,
        credentials_provider=lambda: _oauth_provider(),
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def _oauth_provider():
    from databricks.sdk.core import Config, oauth_service_principal
    cfg = Config(
        host=DBX_HOST,
        client_id=DBX_CLIENT_ID,
        client_secret=DBX_CLIENT_SECRET,
    )
    return oauth_service_principal(cfg)


def _build_occ():
    """Occupancy cur/30/60/90 por propiedad (validado vs Yardi Projected Occupancy)."""
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
    return [{"p": r["p"], "cur": r["cur"], "d30": r["d30"],
             "d60": r["d60"], "d90": r["d90"]} for r in rows]


def _build_data():
    """Arma D: secciones cableadas en vivo + resto del snapshot semilla."""
    data = dict(SEED)               # base con el shape completo
    live = {}
    try:
        data["occ"] = _build_occ()
        live["occ"] = "live"
    except Exception as e:          # si el warehouse falla, cae al snapshot
        live["occ"] = f"seed (error: {str(e)[:60]})"
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
            _cache["data"] = _build_data()
            _cache["ts"] = now
        payload = _cache["data"]
    return JSONResponse(payload)


@app.get("/")
def index():
    return FileResponse(HTML, media_type="text/html")
