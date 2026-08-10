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


def _build_ren():
    """Renewal & trade-out (EN VIVO, calibrado vs el reporte real: REN% t12 MAE 2.6,
    NL TO MAE 1.5, REN TO MAE 1.8):
      - rate: trailing-12m (Lease Renewal + Month to Month) / (+ Move Out) x100
      - nl:   mensual, renta del move-in vs renta del residente ANTERIOR en la unidad
      - gr:   mensual, renta de la renovacion vs renta previa del MISMO residente
    La propiedad se resuelve VIA TENANT (tenant_history.property_id viene NULL en
    Move In/Out)."""
    rows = _sql(f"""
      WITH ev AS (
        SELECT th.history_id, th.tenant_id, coalesce(th.unit_id, t.unit_id) unit_id,
               th.event_type, cast(th.event_date AS date) event_date,
               date_format(th.event_date,'yyyy-MM') mes, th.rent_amount,
               trim(p.property_code) property_code
        FROM cat_prod.silver_core.ydi_tenant_history th
        JOIN cat_prod.silver_core.ydi_tenant t ON t.tenant_id = th.tenant_id
        JOIN cat_prod.silver_core.ydi_property p
          ON p.property_id = coalesce(th.property_id, t.property_id)
        WHERE trim(p.property_code) IN ({CODES_SQL})
          AND cast(th.event_date AS date) >= add_months(date_trunc('year', current_date()), -25)
          AND cast(th.event_date AS date) <= current_date()
          AND th.event_type IN ('Lease Renewal','Month to Month','Move Out','Move In',
                                'Rent Change','Lease Signed')),
      m AS (
        SELECT property_code, mes,
          SUM(CASE WHEN event_type IN ('Lease Renewal','Month to Month') THEN 1 ELSE 0 END) ren,
          SUM(CASE WHEN event_type='Move Out' THEN 1 ELSE 0 END) mo
        FROM ev GROUP BY 1,2),
      rate AS (
        SELECT property_code, mes,
          round(100.0*SUM(ren) OVER w12 / nullif(SUM(ren) OVER w12 + SUM(mo) OVER w12,0),1) rate
        FROM m
        WINDOW w12 AS (PARTITION BY property_code ORDER BY mes ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)),
      -- NL v2 (verificado vs plataforma 2026-08-10): leases FIRMADOS por fecha de
      -- move-in del tenant (como cuenta el Resident Activity de Yardi), no por el
      -- evento posteado. Incluye futuros firmados (status 2/6) — su renta ya existe.
      mi AS (
        SELECT t.tenant_id, t.unit_id, trim(p.property_code) property_code,
               date_format(t.move_in_at,'yyyy-MM') mes,
               cast(t.move_in_at AS date) move_in, t.rent_amount rent_new
        FROM cat_prod.silver_core.ydi_tenant t
        JOIN cat_prod.silver_core.ydi_property p ON p.property_id = t.property_id
        WHERE trim(p.property_code) IN ({CODES_SQL})
          AND cast(t.status AS int) NOT IN (7,9)
          AND t.rent_amount > 0 AND t.unit_id IS NOT NULL
          AND cast(t.move_in_at AS date) >= date_trunc('year', current_date())),
      mi_prev AS (
        SELECT mi.tenant_id, mi.mes, max_by(mo.rent_amount, mo.event_date) rent_prev
        FROM mi JOIN ev mo ON mo.unit_id = mi.unit_id AND mo.event_type='Move Out'
            AND mo.event_date < mi.move_in AND mo.rent_amount > 0
        GROUP BY mi.tenant_id, mi.mes),
      nl AS (
        SELECT mi.property_code, mi.mes,
          round(avg((mi.rent_new - pv.rent_prev)/pv.rent_prev*100),1) nl
        FROM mi JOIN mi_prev pv ON pv.tenant_id = mi.tenant_id AND pv.mes = mi.mes
        GROUP BY 1,2),
      lr AS (
        SELECT history_id, tenant_id, property_code, mes, event_date, rent_amount rent_new
        FROM ev WHERE event_type='Lease Renewal' AND rent_amount > 0
          AND mes >= date_format(date_trunc('year', current_date()),'yyyy-MM')),
      lr_prev AS (
        SELECT lr.history_id, max_by(e.rent_amount, e.event_date) rent_prev
        FROM lr JOIN ev e ON e.tenant_id = lr.tenant_id AND e.event_date < lr.event_date
            AND e.rent_amount > 0
            AND e.event_type IN ('Move In','Lease Renewal','Rent Change','Lease Signed','Month to Month')
        GROUP BY lr.history_id),
      gr AS (
        SELECT lr.property_code, lr.mes,
          round(avg((lr.rent_new - pv.rent_prev)/pv.rent_prev*100),1) gr
        FROM lr JOIN lr_prev pv ON pv.history_id = lr.history_id
        WHERE pv.rent_prev > 0
        GROUP BY 1,2)
      SELECT r.property_code, r.mes, r.rate, nl.nl, gr.gr
      FROM rate r
      LEFT JOIN nl ON nl.property_code=r.property_code AND nl.mes=r.mes
      LEFT JOIN gr ON gr.property_code=r.property_code AND gr.mes=r.mes
      WHERE r.mes >= date_format(date_trunc('year', current_date()),'yyyy-MM')
      ORDER BY r.property_code, r.mes
    """)
    if not rows:
        return None, None
    meses = sorted({r["mes"] for r in rows})
    labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    months = [labels[int(m.split("-")[1]) - 1] for m in meses]
    by = {}
    for r in rows:
        name = PROP_NAME.get(r["property_code"])
        if name:
            by.setdefault(name, {})[r["mes"]] = r
    out = []
    for base in SEED["ren"]:
        rowmap = by.get(base["p"], {})
        rate, nl, gr = [], [], []
        for m in meses:
            rr = rowmap.get(m, {})
            rate.append(_to_float(rr.get("rate")))
            nl.append(_to_float(rr.get("nl")))
            gr.append(_to_float(rr.get("gr")))
        out.append({"p": base["p"], "rate": rate, "nl": nl, "gr": gr})
    return out, months


def _build_fun():
    """Embudo de leasing (EN VIVO) desde los flags de Yardi:
      leads = prospect_history.is_first_contact (excluyendo is_invalid_lead)
      tours = prospect_history.is_first_show
      apps  = tenant_history 'Submit Application' (el prospecto se vuelve tenant al aplicar)
    Ventanas WTD/MTD/QTD. OJO booleanos Yardi = -1 -> comparar <> 0."""
    rows = _sql(f"""
      WITH ph AS (
        SELECT trim(p.property_code) property_code,
          count(CASE WHEN cast(ph.is_first_contact AS int) <> 0
                      AND NOT coalesce(cast(ph.is_invalid_lead AS boolean), false)
                      AND cast(ph.event_date AS date) >= date_trunc('week', current_date()) THEN 1 END) wtd_leads,
          count(CASE WHEN cast(ph.is_first_show AS int) <> 0
                      AND cast(ph.event_date AS date) >= date_trunc('week', current_date()) THEN 1 END) wtd_tours,
          count(CASE WHEN cast(ph.is_first_contact AS int) <> 0
                      AND NOT coalesce(cast(ph.is_invalid_lead AS boolean), false)
                      AND cast(ph.event_date AS date) >= date_trunc('month', current_date()) THEN 1 END) mtd_leads,
          count(CASE WHEN cast(ph.is_first_show AS int) <> 0
                      AND cast(ph.event_date AS date) >= date_trunc('month', current_date()) THEN 1 END) mtd_tours,
          count(CASE WHEN cast(ph.is_first_contact AS int) <> 0
                      AND NOT coalesce(cast(ph.is_invalid_lead AS boolean), false)
                      AND cast(ph.event_date AS date) >= date_trunc('quarter', current_date()) THEN 1 END) qtd_leads,
          count(CASE WHEN cast(ph.is_first_show AS int) <> 0
                      AND cast(ph.event_date AS date) >= date_trunc('quarter', current_date()) THEN 1 END) qtd_tours
        FROM cat_prod.silver_core.ydi_prospect_history ph
        JOIN cat_prod.silver_core.ydi_property p ON p.property_id = ph.property_id
        WHERE trim(p.property_code) IN ({CODES_SQL})
          AND cast(ph.event_date AS date) >= date_trunc('quarter', current_date())
          AND cast(ph.event_date AS date) <= current_date()
        GROUP BY 1),
      apps AS (
        SELECT trim(p.property_code) property_code,
          count(CASE WHEN cast(th.event_date AS date) >= date_trunc('week', current_date()) THEN 1 END) wtd_apps,
          count(CASE WHEN cast(th.event_date AS date) >= date_trunc('month', current_date()) THEN 1 END) mtd_apps,
          count(*) qtd_apps
        FROM cat_prod.silver_core.ydi_tenant_history th
        JOIN cat_prod.silver_core.ydi_tenant t ON t.tenant_id = th.tenant_id
        JOIN cat_prod.silver_core.ydi_property p
          ON p.property_id = coalesce(th.property_id, t.property_id)
        WHERE trim(p.property_code) IN ({CODES_SQL})
          AND th.event_type = 'Submit Application'
          AND cast(th.event_date AS date) >= date_trunc('quarter', current_date())
          AND cast(th.event_date AS date) <= current_date()
        GROUP BY 1)
      SELECT coalesce(ph.property_code, a.property_code) property_code,
        ph.wtd_leads, ph.wtd_tours, a.wtd_apps,
        ph.mtd_leads, ph.mtd_tours, a.mtd_apps,
        ph.qtd_leads, ph.qtd_tours, a.qtd_apps
      FROM ph FULL OUTER JOIN apps a ON a.property_code = ph.property_code
    """)
    by = {PROP_NAME[r["property_code"]]: r for r in rows if r["property_code"] in PROP_NAME}
    out = []
    for base in SEED["fun"]:
        r = {"p": base["p"], "s": base["s"]}
        live = by.get(base["p"], {})
        for w in ("wtd", "mtd", "qtd"):
            r[w] = {"leads": _i(live.get(f"{w}_leads")),
                    "tours": _i(live.get(f"{w}_tours")),
                    "apps": _i(live.get(f"{w}_apps"))}
        out.append(r)
    return out


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
    fun = data.get("fun") or []
    if fun:
        roll["mtd_leads"] = sum(_i(r["mtd"]["leads"]) for r in fun)
        roll["mtd_tours"] = sum(_i(r["mtd"]["tours"]) for r in fun)
        roll["mtd_apps"] = sum(_i(r["mtd"]["apps"]) for r in fun)
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
    # ren: renewal & trade-out EN VIVO (calibrado vs el reporte real)
    try:
        ren, months = _build_ren()
        if ren:
            data["ren"] = ren
            data["months"] = months
            live["ren"] = "live"
        else:
            live["ren"] = "seed (sin filas)"
    except Exception as e:
        live["ren"] = f"seed (error: {str(e)[:80]})"
    # fun: embudo EN VIVO desde prospect_history (flags is_first_contact/is_first_show)
    try:
        fun = _build_fun()
        if fun:
            data["fun"] = fun
            live["fun"] = "live"
        else:
            live["fun"] = "seed (sin filas)"
    except Exception as e:
        live["fun"] = f"seed (error: {str(e)[:80]})"
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
