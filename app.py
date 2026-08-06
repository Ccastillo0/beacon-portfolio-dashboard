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


def _f(v, nd=1):
    try:
        return round(float(v), nd)
    except (TypeError, ValueError):
        return 0.0


def _i(v):
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return 0


# ---- Codigo de propiedad -> nombre que usa el dashboard (17 propiedades) ----
PROP_NAME = {
    "12001": "The Pointe at Clearwater", "12002": "The Pointe at Carrollwood",
    "12003": "The Oceanaire", "12005": "Harper Grove",
    "13002": "Gateway at Cedar Brook", "13003": "Domain at Cedar Creek",
    "13005": "53 West", "13006": "The Exchange", "13007": "The Pointe at Concord",
    "13008r": "Enterprise Mill Apartments", "37001": "Cary Pines",
    "37002": "Lofts at North Hills", "37003": "The Pointe at Heritage",
    "37004": "NorthCity 6", "37005": "Cambridge Apartments",
    "45001": "Vinings at Laurel Creek", "45002": "Jamison Park",
}
CODES_SQL = ",".join(f"'{c}'" for c in PROP_NAME)
UNITS = SEED["units"]        # nombre -> total de unidades


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


def _build_wo():
    """Work orders abiertos por tipo, desde el backlog de SuiteSpot (EN VIVO)."""
    rows = _sql(f"""
      WITH cat AS (
        SELECT property_code, open_workorders AS o, avg_days_open, aging_bucket,
          CASE WHEN category='Pest Control' THEN 'pest'
               WHEN category='HVAC' THEN 'hvac'
               WHEN category='Plumbing' THEN 'plumb'
               WHEN category='Locks / Keys' THEN 'doors'
               WHEN category IN ('Appliance','Electrical','Unit Interior','Building Interior',
                    'Common Area','Building Exterior','Make Ready','Preventive Maintenance',
                    'Property','Fire Safety','Move-out Pre-inspection') THEN 'gen'
               ELSE 'other' END AS bucket
        FROM cat_prod.gold_analytics.gld_ssp_workorder_backlog
        WHERE property_code IN ({CODES_SQL}))
      SELECT property_code,
        SUM(CASE WHEN bucket='pest'  THEN o ELSE 0 END) pest,
        SUM(CASE WHEN bucket='hvac'  THEN o ELSE 0 END) hvac,
        SUM(CASE WHEN bucket='gen'   THEN o ELSE 0 END) gen,
        SUM(CASE WHEN bucket='doors' THEN o ELSE 0 END) doors,
        SUM(CASE WHEN bucket='plumb' THEN o ELSE 0 END) plumb,
        SUM(CASE WHEN bucket='other' THEN o ELSE 0 END) other,
        SUM(o) tot,
        ROUND(SUM(avg_days_open*o)/NULLIF(SUM(o),0),1) avgopen,
        ROUND(100.0*SUM(CASE WHEN aging_bucket<>'0-7 days' THEN o ELSE 0 END)/NULLIF(SUM(o),0),0) p3
      FROM cat GROUP BY property_code""")
    by = {PROP_NAME[r["property_code"]]: r for r in rows if r["property_code"] in PROP_NAME}
    out = []
    for base in SEED["wo"]:
        r = dict(base)
        live = by.get(base["p"])
        if live:
            for k in ("pest", "hvac", "gen", "doors", "plumb", "other", "tot"):
                r[k] = _i(live[k])
            r["avgopen"] = _f(live["avgopen"])
            r["p3"] = _f(live["p3"], 0)
        else:  # sin backlog = cero abiertas
            for k in ("pest", "hvac", "gen", "doors", "plumb", "other", "tot"):
                r[k] = 0
            r["avgopen"] = 0.0
            r["p3"] = 0.0
        out.append(r)
    return out


def _build_deld():
    """Morosidad por antiguedad, ultimo corte mensual de Yardi (EN VIVO)."""
    rows = _sql(f"""
      WITH latest AS (
        SELECT * FROM cat_prod.gold_analytics.gld_ydi_delinquency_month
        WHERE report_date=(SELECT MAX(report_date)
                           FROM cat_prod.gold_analytics.gld_ydi_delinquency_month)
          AND property IN ({CODES_SQL}))
      SELECT property property_code,
        ROUND(SUM(owed_0_30)) b1, ROUND(SUM(owed_31_60)) b2,
        ROUND(SUM(owed_61_90)) b3, ROUND(SUM(owed_over_90)) b4,
        ROUND(SUM(total_owed)) totd,
        COUNT(DISTINCT CASE WHEN owed_0_30>0   THEN resident_code END) n1,
        COUNT(DISTINCT CASE WHEN owed_31_60>0  THEN resident_code END) n2,
        COUNT(DISTINCT CASE WHEN owed_61_90>0  THEN resident_code END) n3,
        COUNT(DISTINCT CASE WHEN owed_over_90>0 THEN resident_code END) n4,
        COUNT(DISTINCT CASE WHEN (owed_31_60+owed_61_90+owed_over_90)>0
                            THEN resident_code END) n30
      FROM latest GROUP BY property""")
    by = {PROP_NAME[r["property_code"]]: r for r in rows if r["property_code"] in PROP_NAME}
    out = []
    for base in SEED["deld"]:
        r = dict(base)
        live = by.get(base["p"])
        if live:
            for k in ("b1", "b2", "b3", "b4", "totd", "n1", "n2", "n3", "n4", "n30"):
                r[k] = _i(live[k])
            units = UNITS.get(base["p"], r.get("units", 0)) or 0
            r["units"] = units
            r["pct30"] = round(r["n30"] / units * 100, 2) if units else 0.0
        out.append(r)
    return out


def _build_turn():
    """Unidades en giro (no listas) y dias vacias, desde gld_ydi_unit_vacancy (EN VIVO)."""
    rows = _sql(f"""
      SELECT property_code, COUNT(*) turned,
             ROUND(AVG(days_vacant),1) avg, MAX(days_vacant) maxnr
      FROM cat_prod.gold_analytics.gld_ydi_unit_vacancy
      WHERE report_date=(SELECT MAX(report_date)
                         FROM cat_prod.gold_analytics.gld_ydi_unit_vacancy)
        AND property_code IN ({CODES_SQL})
        AND vacancy_status IN ('Vacant Unrented Not Ready','Vacant Rented Not Ready')
        AND days_vacant IS NOT NULL
      GROUP BY property_code""")
    by = {PROP_NAME[r["property_code"]]: r for r in rows if r["property_code"] in PROP_NAME}
    out = []
    for base in SEED["turn"]:
        r = dict(base)
        live = by.get(base["p"])
        if live:
            r["turned"] = _i(live["turned"])
            r["avg"] = _f(live["avg"])
            r["maxnr"] = _i(live["maxnr"])
        else:  # sin unidades en giro
            r["turned"] = 0
            r["avg"] = 0.0
            r["maxnr"] = 0
        out.append(r)
    return out


def _derive_roll(data):
    """Recalcula los KPIs superiores a partir de las secciones en vivo; ren/embudo del seed."""
    roll = dict(SEED["roll"])
    occ, wo, deld, turn = data["occ"], data["wo"], data["deld"], data["turn"]
    tu = sum(UNITS.values())
    roll["props"] = len(occ)
    roll["units"] = tu
    wsum = sum(UNITS.get(r["p"], 0) * (r["cur"] or 0) for r in occ)
    roll["occ"] = round(wsum / tu, 1) if tu else roll["occ"]
    roll["d30count"] = sum(_i(r["n30"]) for r in deld)
    roll["del30pct"] = round(roll["d30count"] / tu * 100, 2) if tu else roll["del30pct"]
    roll["deldollars"] = sum(_i(r["totd"]) for r in deld)
    roll["wo_open"] = sum(_i(r["tot"]) for r in wo)
    roll["wo3"] = round(sum(_i(r["tot"]) * _f(r["p3"]) / 100 for r in wo))
    tturned = sum(_i(r["turned"]) for r in turn)
    roll["turn_tot"] = tturned
    roll["turn_max"] = max((_i(r["maxnr"]) for r in turn), default=0)
    roll["turn_avg"] = round(sum(_f(r["avg"]) * _i(r["turned"]) for r in turn) / tturned, 1) if tturned else 0.0
    return roll


def _build_data():
    data = dict(SEED)
    live = {}
    for key, fn, label in (("occ", _build_occ, "Occupancy (Yardi)"),
                           ("wo", _build_wo, "Work orders (SuiteSpot)"),
                           ("deld", _build_deld, "Delinquency (Yardi)"),
                           ("turn", _build_turn, "Turnover (Yardi)")):
        try:
            res = fn()
            if res:
                data[key] = res
                live[key] = "live"
            else:
                live[key] = "seed (sin filas)"
        except Exception as e:
            live[key] = f"seed (error: {str(e)[:80]})"
    # ren (trade-out mensual) y fun (embudo) se quedan en snapshot por ahora
    live["ren"] = "seed (pendiente validar vs Yardi)"
    live["fun"] = "seed (fuente conversion_ratios por depurar)"
    try:
        data["roll"] = _derive_roll(data)
    except Exception as e:
        live["roll"] = f"seed (error: {str(e)[:80]})"
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
