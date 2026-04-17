from typing import Optional
from fastapi import FastAPI, UploadFile, File, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import io, csv, requests

import os, secrets
from fastapi import Header

from datetime import datetime
from dash.exceptions import PreventUpdate

import plotly.io as pio

from dash import dcc, html, dash_table, Input, Output, State
from dash_extensions import EventListener

from fastapi.responses import RedirectResponse, Response

from .config import APP_TITLE
from .db import Base, engine, SessionLocal
from .models import Supply, Opportunity, SupplyMonthly, OpportunityMonthly
from .crud import (
    create_supply, list_supply, create_opportunity, list_opportunities,
    get_supply_monthly, get_opportunity_monthly, get_gap_monthly
)

import dash_bootstrap_components as dbc

Base.metadata.create_all(bind=engine)

app = FastAPI(title=APP_TITLE)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


@app.get("/")
def root():
    return RedirectResponse(url="/dash/")

@app.get("/favicon.ico")
def favicon():
    return RedirectResponse(url="/dash/assets/favicon.ico")


def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

def _norm_region(region):
    # Treat 'Global' (or None) as aggregate across all regions
    if region is None or (isinstance(region, str) and region.strip().lower() == "global"):
        return None
    return region

# ADD: configure the delete password via env var
'''DELETE_GUARD_PASSWORD = os.environ.get("Ronaldo7")

#from typing import Optional
def _is_delete_pw_valid(provided: Optional[str]) -> bool:
    """Constant-time compare. Returns True only if env var is set and matches."""
    if not DELETE_GUARD_PASSWORD:
        return False
    if not provided:
        return False
    return secrets.compare_digest(provided, DELETE_GUARD_PASSWORD)'''

# ---- Delete password config + helpers ----
DELETE_PASSWORD = os.environ.get("DELETE_PASSWORD", "secret123")  # <- set env in prod

def _is_delete_pw_valid(provided: Optional[str]) -> bool:
    return bool(provided) and provided.strip() == (DELETE_PASSWORD or "").strip()

def _provided_pw(delete_password_query: Optional[str], x_delete_password_header: Optional[str]) -> Optional[str]:
    # accept either ?delete_password=... or X-Delete-Password header
    return (delete_password_query or x_delete_password_header or None)

# in each delete endpoint:
x_delete_password: Optional[str] = Header(None, alias="X-Delete-Password"),

'''@app.post("/api/supply")
def api_create_supply(payload: dict, db: Session = Depends(get_db)):
    row = create_supply(db, payload)
    return {"id": row.id, "normalized": {"mtpa": row.mtpa, "mmbtu": row.mmbtu, "cargoes": row.cargoes,
                                         "equity_mtpa": row.equity_mtpa, "equity_mmbtu": row.equity_mmbtu, "equity_cargoes": row.equity_cargoes}}'''


update_modal = dbc.Modal(
    [
        dbc.ModalHeader(
            dbc.ModalTitle("Update Supply / Opportunity"),
            className="drag-handle"
        ),
        dbc.ModalBody(
            [
                html.Div("Place your inputs here")  # <- replace with your real inputs
            ]
        ),
        dbc.ModalFooter(
            [
                dbc.Button("Close", id="close-modal", n_clicks=0)
            ]
        ),
    ],
    id="update-modal",
    is_open=False,
    centered=False,
    backdrop=True,
    keyboard=True,
    className="draggable-modal",
)



'''dash_table.DataTable(
    id="tbl_supply",
    page_size=10,
    row_selectable="single",        
    selected_rows=[],               
    style_table={"overflowX":"auto"}
)

dash_table.DataTable(
    id="tbl_opp",
    page_size=10,
    style_table={"overflowX":"auto"},
    row_selectable="single",
    selected_rows=[]
)'''

from fastapi import HTTPException  # already imported above, keep it once

@app.post("/api/supply")
def api_create_supply(
    payload: dict,
    delete_password: Optional[str] = Query(None),
    x_delete_password: Optional[str] = Header(None, alias="X-Delete-Password"),
    db: Session = Depends(get_db),
):
    try:
        data = dict(payload or {})

        # ---- normalize required basics ----
        # scenario default + validation
        scenario = (data.get("scenario") or "P50").strip().upper()
        if scenario not in {"P50", "P90"}:
            raise ValueError("scenario must be P50 or P90")
        data["scenario"] = scenario

        # year (coerce float->int if needed)
        if data.get("year") is None:
            raise ValueError("year is required")
        data["year"] = int(float(data["year"]))

        # optional: region/source checks (uncomment to enforce)
        # if not data.get("region"): raise ValueError("region is required")
        # if not data.get("source"): raise ValueError("source is required")

        # equity_fraction clamped to [0,1]
        eq = data.get("equity_fraction")
        if eq is not None:
            eq = float(eq)
            if eq < 0 or eq > 1:
                raise ValueError("equity_fraction must be between 0 and 1")
        else:
            eq = 1.0
        data["equity_fraction"] = eq

        # month fields (optional)
        if data.get("start_month") is not None:
            sm = int(float(data["start_month"]))
            if sm < 1 or sm > 12:
                raise ValueError("start_month must be 1..12")
            data["start_month"] = sm
        if data.get("months_active") is not None:
            ma = int(float(data["months_active"]))
            if ma < 1 or ma > 12:
                raise ValueError("months_active must be 1..12")
            data["months_active"] = ma

        # optional conversions metadata
        if data.get("ghv_mmbtu_per_tonne") is not None:
            data["ghv_mmbtu_per_tonne"] = float(data["ghv_mmbtu_per_tonne"])
        if data.get("cargo_mmbtu") is not None:
            data["cargo_mmbtu"] = float(data["cargo_mmbtu"])
        if data.get("cargo_tonnes") is not None:
            data["cargo_tonnes"] = float(data["cargo_tonnes"])

        # ---- normalize numeric volume fields ----
        mtpa   = float(data.get("mtpa")   or 0.0)
        mmbtu  = float(data.get("mmbtu")  or 0.0)
        cargoes= float(data.get("cargoes")or 0.0)

        # fallback: derive from unit/value if provided by client
        unit_field = (data.get("unit") or "").strip().lower()
        if (mtpa == 0.0 and mmbtu == 0.0 and cargoes == 0.0) and unit_field and (data.get("value") is not None):
            v = float(data["value"])
            if unit_field == "mtpa":
                mtpa = v
            elif unit_field == "mmbtu":
                mmbtu = v
            elif unit_field == "cargoes":
                cargoes = v

        data["mtpa"], data["mmbtu"], data["cargoes"] = mtpa, mmbtu, cargoes

        # ---- ensure original_unit / original_value exist ----
        allowed = {"mtpa", "mmbtu", "cargoes"}
        orig_unit = (data.get("original_unit") or "").strip().lower()
        orig_value = data.get("original_value")

        if not orig_unit or orig_unit not in allowed or orig_value in (None, ""):
            # derive from client unit/value if present
            if unit_field in allowed and (data.get("value") is not None):
                orig_unit = unit_field
                orig_value = float(data["value"])
            else:
                # otherwise derive from first non-zero numeric field
                if mtpa > 0:
                    orig_unit, orig_value = "mtpa", mtpa
                elif mmbtu > 0:
                    orig_unit, orig_value = "mmbtu", mmbtu
                elif cargoes > 0:
                    orig_unit, orig_value = "cargoes", cargoes
                else:
                    # safe default for testing
                    orig_unit, orig_value = "mtpa", 0.0

        data["original_unit"] = orig_unit
        data["original_value"] = float(orig_value)

        # optional notes
        data["notes"] = data.get("notes") or ""

        # Supply status: firm / negotiation / option (default negotiation)
        status = (data.get("status") or "negotiation").strip().lower() 
        if status not in {"firm", "negotiation", "option"}:
            raise ValueError("status must be firm/negotiation/option")
        data["status"] = status

        # ADD THIS:
        if status == "firm": # negotiation firm
            provided = _provided_pw(delete_password, x_delete_password)
            if not _is_delete_pw_valid(provided):
                raise HTTPException(status_code=401, detail="Creating a firm supply requires a valid password.")

        # ---- create ----
        row = create_supply(db, data)

        # Build a robust response (works even if model doesn't precompute equity_*)
        try:
            eq_frac = float(getattr(row, "equity_fraction", eq) or eq)
            r_mtpa = float(getattr(row, "mtpa", mtpa) or 0.0)
            r_mmbtu = float(getattr(row, "mmbtu", mmbtu) or 0.0)
            r_cargo = float(getattr(row, "cargoes", cargoes) or 0.0)
            equity_mtpa = float(getattr(row, "equity_mtpa", r_mtpa * eq_frac))
            equity_mmbtu = float(getattr(row, "equity_mmbtu", r_mmbtu * eq_frac))
            equity_cargoes = float(getattr(row, "equity_cargoes", r_cargo * eq_frac))
        except Exception:
            eq_frac = eq
            equity_mtpa, equity_mmbtu, equity_cargoes = mtpa*eq_frac, mmbtu*eq_frac, cargoes*eq_frac

        return {
            "id": row.id,
            "normalized": {
                "mtpa": r_mtpa if 'r_mtpa' in locals() else mtpa,
                "mmbtu": r_mmbtu if 'r_mmbtu' in locals() else mmbtu,
                "cargoes": r_cargo if 'r_cargo' in locals() else cargoes,
                "equity_mtpa": equity_mtpa,
                "equity_mmbtu": equity_mmbtu,
                "equity_cargoes": equity_cargoes,
                "original_unit": data["original_unit"],
                "original_value": data["original_value"],
            }
        }

    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))

'''@app.post("/api/opportunities")
def api_create_opp(payload: dict, db: Session = Depends(get_db)):
    row = create_opportunity(db, payload)
    return {"id": row.id, "normalized": {"mtpa": row.mtpa, "mmbtu": row.mmbtu, "cargoes": row.cargoes}}'''

from fastapi import HTTPException

@app.post("/api/opportunities")
def api_create_opp(payload: dict, db: Session = Depends(get_db)):
    try:
        data = dict(payload or {})

        # ---- normalize basic fields ----
        status = (data.get("status") or "negotiation").strip().lower()
        if status not in {"firm", "negotiation", "option"}:
            raise ValueError(f"Invalid status '{status}' (expected firm/negotiation/option)")
        data["status"] = status

        fob_des = (data.get("fob_des") or "FOB").strip().upper()
        if fob_des not in {"FOB", "DES"}:
            raise ValueError(f"Invalid fob_des '{fob_des}' (expected FOB/DES)")
        data["fob_des"] = fob_des

        if data.get("probability") is None:
            data["probability"] = 1.0 if status == "firm" else 0.5
        else:
            data["probability"] = float(data["probability"])
            if not (0.0 <= data["probability"] <= 1.0):
                raise ValueError("probability must be between 0 and 1")

        if data.get("year") is None:
            raise ValueError("year is required")
        data["year"] = int(float(data["year"]))

        # Normalize numeric volume fields (default 0.0)
        mtpa = float(data.get("mtpa") or 0.0)
        mmbtu = float(data.get("mmbtu") or 0.0)
        cargoes = float(data.get("cargoes") or 0.0)

        # Fallback: if client sent unit/value instead of mtpa/mmbtu/cargoes
        unit_field = (data.get("unit") or "").strip().lower()
        if (mtpa == 0.0 and mmbtu == 0.0 and cargoes == 0.0) and unit_field and (data.get("value") is not None):
            v = float(data["value"])
            if unit_field == "mtpa":
                mtpa = v
            elif unit_field == "mmbtu":
                mmbtu = v
            elif unit_field == "cargoes":
                cargoes = v

        data["mtpa"], data["mmbtu"], data["cargoes"] = mtpa, mmbtu, cargoes

        # Optional text fields
        data["pricing_index"] = data.get("pricing_index") or ""
        data["notes"] = data.get("notes") or ""

        # ---- Ensure original_unit / original_value exist ----
        allowed = {"mtpa", "mmbtu", "cargoes"}

        orig_unit = (data.get("original_unit") or "").strip().lower()
        orig_value = data.get("original_value")

        if not orig_unit or orig_unit not in allowed or orig_value in (None, ""):
            # derive from provided unit/value if present
            if unit_field in allowed and (data.get("value") is not None):
                orig_unit = unit_field
                orig_value = float(data["value"])
            else:
                # otherwise derive from the non-zero of mtpa/mmbtu/cargoes (pick first by priority)
                if mtpa > 0:
                    orig_unit, orig_value = "mtpa", mtpa
                elif mmbtu > 0:
                    orig_unit, orig_value = "mmbtu", mmbtu
                elif cargoes > 0:
                    orig_unit, orig_value = "cargoes", cargoes
                else:
                    # still nothing? set a safe default to unblock testing
                    orig_unit, orig_value = "mtpa", 0.0

        data["original_unit"] = orig_unit
        data["original_value"] = float(orig_value)

        # ---- Create row ----
        row = create_opportunity(db, data)

        return {
            "id": row.id,
            "normalized": {
                "mtpa": row.mtpa,
                "mmbtu": row.mmbtu,
                "cargoes": row.cargoes,
                "probability": getattr(row, "probability", data["probability"]),
                "original_unit": data["original_unit"],
                "original_value": data["original_value"],
            },
        }

    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))

from fastapi import HTTPException

# ---------- Supply: UPDATE & DELETE ----------


from fastapi import HTTPException
import traceback, sys

# ---------- BULK UPDATE: Supply (with per-year plan) ----------
@app.patch("/api/supply/bulk")
def api_bulk_update_supply(
    payload: dict,
    year_from: Optional[int] = Query(None),
    year_to: Optional[int]   = Query(None),
    region: Optional[str]    = None,
    scenario: Optional[str]  = None,
    source: Optional[str]    = None,
    delete_password: Optional[str] = Query(None),     # ← NEW
    x_delete_password: Optional[str] = Header(None),  # ← NEW
    db: Session = Depends(get_db),
):
    try:
        data = dict(payload or {})
        plan = data.pop("plan", None)

            # --- do NOT allow bulk to change identity keys ---
        IDENTITY_KEYS = {"source","source_type","region","scenario","year"}
        SAFE_BULK_FIELDS = {
            "mtpa","mmbtu","cargoes","equity_fraction",
            "start_month","months_active","ghv_mmbtu_per_tonne",
            "cargo_mmbtu","cargo_tonnes","notes","original_unit","original_value",
            "status"
        }

        unit = (data.get("unit") or "").strip().lower()
        if "value" in data and unit in {"mtpa","mmbtu","cargoes"}:
            val = float(data["value"])
            if unit == "mtpa":      data["mtpa"] = val
            elif unit == "mmbtu":   data["mmbtu"] = val
            elif unit == "cargoes": data["cargoes"] = val
            data.setdefault("original_unit", unit)
            data.setdefault("original_value", val)

        # coerce numeric fields safely
        for k in ["mtpa","mmbtu","cargoes","equity_fraction","ghv_mmbtu_per_tonne",
                  "cargo_mmbtu","cargo_tonnes","original_value"]:
            if k in data and data[k] not in (None, ""):
                data[k] = float(data[k])
        for k in ["start_month","months_active"]:
            if k in data and data[k] not in (None, ""):
                data[k] = int(float(data[k]))

        if "scenario" in data and data["scenario"]:
            sc = str(data["scenario"]).upper()
            if sc not in {"P50","P90"}:
                raise HTTPException(status_code=400, detail="scenario must be P50 or P90")
            data["scenario"] = sc

        if "status" in data and data["status"]:
            st = str(data["status"]).lower()
            if st not in {"firm","negotiation","option"}:
                raise HTTPException(status_code=400, detail="status must be firm/negotiation/option")
            data["status"] = st

        base_q = db.query(Supply)
        if scenario: base_q = base_q.filter(Supply.scenario == str(scenario).upper())
        if source:   base_q = base_q.filter(Supply.source == source)
        reg = _norm_region(region)
        if reg is not None: base_q = base_q.filter(Supply.region == reg)
        if year_from is not None: base_q = base_q.filter(Supply.year >= int(float(year_from)))
        if year_to   is not None: base_q = base_q.filter(Supply.year <= int(float(year_to)))

        # <<< INSERT PASSWORD HERE >>>
        count = base_q.count()
        firm_count = base_q.filter(Supply.status == "firm").count()
        making_firm = (str((data.get("status") or "")).lower() == "firm")

        if count > 0 and (firm_count > 0 or making_firm):
            provided = _provided_pw(delete_password, x_delete_password)
            if not _is_delete_pw_valid(provided):
                raise HTTPException(status_code=401, detail="Firm supplies bulk update (or making firm) requires a valid password.")
        # <<< END PASSWORD INSERT >>>
        
        # Remove any identity keys that slipped into the payload (prevents UNIQUE collisions)
        for k in list(data.keys()):
            if k in IDENTITY_KEYS:
                data.pop(k, None)

        #allowed = {"source","source_type","region","scenario","mtpa","mmbtu","cargoes",
        #           "equity_fraction","start_month","months_active","ghv_mmbtu_per_tonne",
        #           "cargo_mmbtu","cargo_tonnes","notes","original_unit","original_value"}
        
        allowed = SAFE_BULK_FIELDS

        updated = 0

        if plan and isinstance(plan, list):
            for item in plan:
                try:
                    y = int(float(item.get("year")))
                    v = float(item.get("value"))
                except Exception:
                    continue
                u = unit
                if not u:
                    if "mtpa" in item: u, v = "mtpa", float(item["mtpa"])
                    elif "mmbtu" in item: u, v = "mmbtu", float(item["mmbtu"])
                    elif "cargoes" in item: u, v = "cargoes", float(item["cargoes"])
                    else:
                        u = "mtpa"

                rows = base_q.filter(Supply.year == y).all()
                for row in rows:
                    for k, val in data.items():
                        if k in allowed:
                            setattr(row, k, val)
                    if u == "mtpa":      row.mtpa = v
                    elif u == "mmbtu":   row.mmbtu = v
                    elif u == "cargoes": row.cargoes = v
                    row.original_unit = u
                    row.original_value = v
                    db.add(row); updated += 1
            db.commit()
            return {"status":"ok","updated":updated}

        rows = base_q.all()
        if not rows: return {"status":"ok","updated":0}
        for row in rows:
            for k, v in data.items():
                if k in allowed:
                    setattr(row, k, v)
            db.add(row); updated += 1
        db.commit()
        return {"status":"ok","updated":updated}

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        raise HTTPException(status_code=400, detail=f"bulk supply failed: {e}")

# ---------- BULK CREATE: Supply ----------
@app.post("/api/supply/bulk")
def api_bulk_create_supply(
    payload: dict,
    delete_password: Optional[str] = Query(None),
    x_delete_password: Optional[str] = Header(None, alias="X-Delete-Password"),
    db: Session = Depends(get_db),
):
    try:
        data = dict(payload or {})
        plan = data.pop("plan", None)
        if not plan or not isinstance(plan, list):
            raise HTTPException(status_code=400, detail="plan (list of {year,value}) is required")

        # Required identity/shape for creation
        source      = (data.get("source") or "").strip()
        region_raw  = data.get("region")
        scenario    = (data.get("scenario") or "P50").strip().upper()
        unit        = (data.get("unit") or "").strip().lower()

        if scenario not in {"P50", "P90"}:
            raise HTTPException(status_code=400, detail="scenario must be P50 or P90")
        if not source:
            raise HTTPException(status_code=400, detail="source is required")
        if unit not in {"mtpa","mmbtu","cargoes"}:
            raise HTTPException(status_code=400, detail="unit must be mtpa/mmbtu/cargoes")

        region = _norm_region(region_raw)
        source_type = data.get("source_type")

        status = (data.get("status") or "negotiation").strip().lower()
        if status not in {"firm", "negotiation", "option"}:
            raise HTTPException(status_code=400, detail="status must be firm/negotiation/option")

        if status == "firm": #negotiation firm
            provided = _provided_pw(delete_password, x_delete_password)
            if not _is_delete_pw_valid(provided):
                raise HTTPException(status_code=401, detail="Bulk creating firm supplies requires a valid password.")

        # Optional numeric fields to carry on all rows
        def _to_float(x):
            try:
                return float(x) if x not in (None, "") else None
            except:
                return None

        equity_fraction       = _to_float(data.get("equity_fraction")) if data.get("equity_fraction") is not None else 1.0
        start_month           = int(float(data["start_month"]))       if data.get("start_month") not in (None, "") else None
        months_active         = int(float(data["months_active"]))     if data.get("months_active") not in (None, "") else None
        ghv_mmbtu_per_tonne   = _to_float(data.get("ghv_mmbtu_per_tonne"))
        cargo_mmbtu           = _to_float(data.get("cargo_mmbtu"))
        cargo_tonnes          = _to_float(data.get("cargo_tonnes"))
        notes                 = data.get("notes") or ""

        created = 0
        skipped_existing = 0
        errors = 0

        for item in plan:
            try:
                # year + value (or explicit mtpa/mmbtu/cargoes in the row)
                if unit in item and item.get(unit) not in (None, ""):
                    value = float(item[unit])
                else:
                    value = float(item.get("value"))
                year = int(float(item.get("year")))
            except Exception:
                errors += 1
                continue

            # Check existence — avoid duplicate unique collisions
            q = db.query(Supply).filter(
                Supply.source == source,
                Supply.scenario == scenario,
                Supply.year == year
            )
            if region is None:
                q = q.filter(Supply.region == None)  # noqa: E711
            else:
                q = q.filter(Supply.region == region)
            if source_type:
                q = q.filter(Supply.source_type == source_type)

            if q.first():
                skipped_existing += 1
                continue

            row_payload = {
                "source": source,
                "source_type": source_type,
                "region": region,
                "scenario": scenario,
                "year": year,
                "equity_fraction": equity_fraction,
                "start_month": start_month,
                "months_active": months_active,
                "ghv_mmbtu_per_tonne": ghv_mmbtu_per_tonne,
                "cargo_mmbtu": cargo_mmbtu,
                "cargo_tonnes": cargo_tonnes,
                "notes": notes,
                "unit": unit,
                "value": value,
                "original_unit": unit,
                "original_value": value,
                "status": status,
            }

            create_supply(db, row_payload)
            created += 1

        db.commit()
        return {"status": "ok", "created": created, "skipped_existing": skipped_existing, "errors": errors}

    except HTTPException:
        raise
    except Exception as e:
        import traceback, sys
        traceback.print_exc(file=sys.stderr)
        raise HTTPException(status_code=400, detail=f"bulk create supply failed: {e}")


# ---------- BULK CREATE: Opportunities ----------
@app.post("/api/opportunities/bulk")
def api_bulk_create_opportunities(payload: dict, db: Session = Depends(get_db)):
    try:
        data = dict(payload or {})
        plan = data.pop("plan", None)
        if not plan or not isinstance(plan, list):
            raise HTTPException(status_code=400, detail="plan (list of {year,value}) is required")

        # Required identity/shape for creation
        contract_name = (data.get("contract_name") or "").strip()
        counterparty  = (data.get("counterparty") or "").strip()
        region        = _norm_region(data.get("region"))
        status        = (data.get("status") or "negotiation").strip().lower()
        fob_des       = (data.get("fob_des") or "FOB").strip().upper()
        unit          = (data.get("unit") or "").strip().lower()

        if not contract_name or not counterparty:
            raise HTTPException(status_code=400, detail="contract_name and counterparty are required")
        if status not in {"firm", "negotiation", "option"}:
            raise HTTPException(status_code=400, detail="status must be firm/negotiation/option")
        if fob_des not in {"FOB", "DES"}:
            raise HTTPException(status_code=400, detail="fob_des must be FOB/DES")
        if unit not in {"mtpa","mmbtu","cargoes"}:
            raise HTTPException(status_code=400, detail="unit must be mtpa/mmbtu/cargoes")

        pricing_index = data.get("pricing_index") or ""
        notes         = data.get("notes") or ""
        probability   = float(data.get("probability")) if data.get("probability") not in (None, "") else (1.0 if status=="firm" else 0.5)
        if not (0.0 <= probability <= 1.0):
            raise HTTPException(status_code=400, detail="probability must be between 0 and 1")

        created = 0
        skipped_existing = 0
        errors = 0

        for item in plan:
            try:
                if unit in item and item.get(unit) not in (None, ""):
                    value = float(item[unit])
                else:
                    value = float(item.get("value"))
                year = int(float(item.get("year")))
            except Exception:
                errors += 1
                continue

            q = db.query(Opportunity).filter(
                Opportunity.contract_name == contract_name,
                Opportunity.counterparty == counterparty,
                Opportunity.year == year
            )
            if region is None:
                q = q.filter(Opportunity.region == None)  # noqa: E711
            else:
                q = q.filter(Opportunity.region == region)

            if q.first():
                skipped_existing += 1
                continue

            row_payload = {
                "contract_name": contract_name,
                "counterparty": counterparty,
                "status": status,
                "region": region,
                "fob_des": fob_des,
                "pricing_index": pricing_index,
                "probability": probability,
                "year": year,
                "unit": unit,
                "value": value,
                "original_unit": unit,
                "original_value": value,
                "notes": notes,
            }

            create_opportunity(db, row_payload)
            created += 1

        db.commit()
        return {"status": "ok", "created": created, "skipped_existing": skipped_existing, "errors": errors}

    except HTTPException:
        raise
    except Exception as e:
        import traceback, sys
        traceback.print_exc(file=sys.stderr)
        raise HTTPException(status_code=400, detail=f"bulk create opportunities failed: {e}")




#### Bulk Supply Delete

@app.delete("/api/supply/bulk")
def api_bulk_delete_supply(
    year_from: Optional[int] = Query(None),
    year_to: Optional[int]   = Query(None),
    region: Optional[str]    = None,
    scenario: Optional[str]  = None,
    source: Optional[str]    = None,
    source_type: Optional[str] = None,
    dry_run: bool = Query(False),
    confirm_count: Optional[int] = Query(None),
    threshold: int = Query(10),
    # NEW: accept pw from query or header
    delete_password: Optional[str] = Query(None),
    x_delete_password: Optional[str] = Header(None, alias="X-Delete-Password"),
    db: Session = Depends(get_db),
):
    q = db.query(Supply)
    if scenario:     q = q.filter(Supply.scenario == scenario)
    if source:       q = q.filter(Supply.source == source)
    if source_type:  q = q.filter(Supply.source_type == source_type)
    reg = _norm_region(region)
    if reg is not None:
        q = q.filter(Supply.region == reg)
    if year_from is not None:
        q = q.filter(Supply.year >= int(float(year_from)))
    if year_to is not None:
        q = q.filter(Supply.year <= int(float(year_to)))

    count = q.count()
    if dry_run:
        return {"status": "preview", "count": count}

    # Password is ALWAYS required for supply bulk delete
    if count > 0:
        provided = _provided_pw(delete_password, x_delete_password)
        if not _is_delete_pw_valid(provided):
            raise HTTPException(status_code=401, detail="Supply bulk delete requires a valid password.")

    if count == 0:
        return {"status": "deleted", "deleted": 0}

    if count > threshold and confirm_count != count:
        raise HTTPException(
            status_code=409,
            detail=f"Bulk delete would remove {count} rows. Re-issue with confirm_count={count} (or call with dry_run=1 first)."
        )

    deleted = 0
    for r in q.all():
        db.delete(r)
        deleted += 1
    db.commit()
    return {"status": "deleted", "deleted": deleted}

    

# ---------- BULK UPDATE: Opportunity (with per-year plan) ----------
@app.patch("/api/opportunities/bulk")
def api_bulk_update_opportunities(
    payload: dict,
    year_from: Optional[int] = Query(None),
    year_to: Optional[int]   = Query(None),
    region: Optional[str]    = None,
    delete_password: Optional[str] = Query(None),     # ← NEW
    x_delete_password: Optional[str] = Header(None),  # ← NEW
    db: Session = Depends(get_db),
):
    try:
        data = dict(payload or {})
        plan = data.pop("plan", None)

        # --- Identity keys for Opportunity that must NOT be changed in bulk ---
        # (Changing these across many rows can create UNIQUE collisions.)
        IDENTITY_KEYS = {"contract_name", "counterparty", "region", "year"}

        # Fields that are safe to bulk update
        SAFE_BULK_FIELDS = {
            "mtpa", "mmbtu", "cargoes",
            "probability",
            "status", "fob_des", "pricing_index",
            "notes",
            "original_unit", "original_value"
        }

        unit = (data.get("unit") or "").strip().lower()
        if "value" in data and unit in {"mtpa","mmbtu","cargoes"}:
            val = float(data["value"])
            if unit == "mtpa":      data["mtpa"] = val
            elif unit == "mmbtu":   data["mmbtu"] = val
            elif unit == "cargoes": data["cargoes"] = val
            data.setdefault("original_unit", unit)
            data.setdefault("original_value", val)

        for k in ["mtpa","mmbtu","cargoes","probability","original_value"]:
            if k in data and data[k] not in (None, ""):
                data[k] = float(data[k])
        if "probability" in data and data["probability"] not in (None, ""):
            if not (0.0 <= float(data["probability"]) <= 1.0):
                raise HTTPException(status_code=400, detail="probability must be 0..1")

        if "status" in data and data["status"]:
            st = str(data["status"]).lower()
            if st not in {"firm","negotiation","option"}:
                raise HTTPException(status_code=400, detail="status must be firm/negotiation/option")
            data["status"] = st
        if "fob_des" in data and data["fob_des"]:
            fd = str(data["fob_des"]).upper()
            if fd not in {"FOB","DES"}:
                raise HTTPException(status_code=400, detail="fob_des must be FOB/DES")
            data["fob_des"] = fd

        base_q = db.query(Opportunity)
        reg = _norm_region(region)
        if reg is not None: base_q = base_q.filter(Opportunity.region == reg)
        if year_from is not None: base_q = base_q.filter(Opportunity.year >= int(float(year_from)))
        if year_to   is not None: base_q = base_q.filter(Opportunity.year <= int(float(year_to)))

        # <<< INSERT PASSWORD HERE >>>
        count = base_q.count()
        firm_count = base_q.filter(Opportunity.status == "firm").count()
        making_firm = (str(data.get("status") or "").lower() == "firm")
        if count > 0 and (firm_count > 0 or making_firm):
            provided = _provided_pw(delete_password, x_delete_password)
            if not _is_delete_pw_valid(provided):
                raise HTTPException(status_code=401, detail="Firm opportunities bulk update requires a valid password.")
        # <<< END PASSWORD INSERT >>>

        # Do not allow identity keys to be changed in bulk updates
        for k in list(data.keys()):
            if k in IDENTITY_KEYS:
                data.pop(k, None)

        #allowed = {"contract_name","counterparty","status","fob_des","pricing_index","region",
        #           "mtpa","mmbtu","cargoes","probability","notes","original_unit","original_value","year"}

        allowed = SAFE_BULK_FIELDS

        updated = 0

        if plan and isinstance(plan, list):
            for item in plan:
                try:
                    y = int(float(item.get("year")))
                    v = float(item.get("value"))
                except Exception:
                    continue
                u = unit
                if not u:
                    if "mtpa" in item: u, v = "mtpa", float(item["mtpa"])
                    elif "mmbtu" in item: u, v = "mmbtu", float(item["mmbtu"])
                    elif "cargoes" in item: u, v = "cargoes", float(item["cargoes"])
                    else:
                        u = "mtpa"

                rows = base_q.filter(Opportunity.year == y).all()
                for row in rows:
                    for k, val in data.items():
                        if k in allowed:
                            setattr(row, k, val)
                    if u == "mtpa":      row.mtpa = v
                    elif u == "mmbtu":   row.mmbtu = v
                    elif u == "cargoes": row.cargoes = v
                    row.original_unit = u
                    row.original_value = v
                    db.add(row); updated += 1
            db.commit()
            return {"status":"ok","updated":updated}

        rows = base_q.all()
        if not rows: return {"status":"ok","updated":0}
        for row in rows:
            for k, v in data.items():
                if k in allowed:
                    setattr(row, k, v)
            db.add(row); updated += 1
        db.commit()
        return {"status":"ok","updated":updated}

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        raise HTTPException(status_code=400, detail=f"bulk opportunities failed: {e}")

### Bulk Opportunities

@app.delete("/api/opportunities/bulk")
def api_bulk_delete_opportunities(
    year_from: Optional[int] = Query(None),
    year_to: Optional[int]   = Query(None),
    region: Optional[str]    = None,
    counterparty: Optional[str] = None,
    contract_name: Optional[str] = None,
    status: Optional[str]    = None,
    dry_run: bool = Query(False),
    confirm_count: Optional[int] = Query(None),
    threshold: int = Query(10),
     # NEW:
    delete_password: Optional[str] = Query(None),
    x_delete_password: Optional[str] = Header(None, alias="X-Delete-Password"),
    db: Session = Depends(get_db),
):
    q = db.query(Opportunity)
    reg = _norm_region(region)
    if reg is not None: q = q.filter(Opportunity.region == reg)
    if counterparty:    q = q.filter(Opportunity.counterparty == counterparty)
    if contract_name:   q = q.filter(Opportunity.contract_name == contract_name)
    if status:          q = q.filter(Opportunity.status == status)
    if year_from is not None: q = q.filter(Opportunity.year >= int(float(year_from)))
    if year_to   is not None: q = q.filter(Opportunity.year <= int(float(year_to)))

    count = q.count()
    if dry_run:
        return {"status": "preview", "count": count}

    # If ANY of the rows to be deleted are firm, require the password
    firm_count = q.filter(Opportunity.status == "firm").count()
    if firm_count > 0:
        provided = _provided_pw(delete_password, x_delete_password)
        if not _is_delete_pw_valid(provided):
            raise HTTPException(status_code=401, detail="Firm opportunities bulk delete requires a valid password.")

    if count == 0:
        return {"status":"deleted", "deleted": 0}

    if count > threshold and confirm_count != count:
        raise HTTPException(
            status_code=409,
            detail=f"Bulk delete would remove {count} rows. Re-issue with confirm_count={count} (or call with dry_run=1 first)."
        )

    deleted = 0
    for r in q.all():
        db.delete(r)
        deleted += 1
    db.commit()
    return {"status":"deleted","deleted": deleted}

# ---------- Supply: single UPDATE / DELETE ----------
@app.patch("/api/supply/{row_id}")
def api_update_supply(
    row_id: int,
    payload: dict,
    delete_password: Optional[str] = Query(None),     # ← NEW
    x_delete_password: Optional[str] = Header(None),  # ← NEW
    db: Session = Depends(get_db),
):
    #row = db.query(Supply).get(row_id)
    row = db.get(Supply, row_id)
    if not row:
        raise HTTPException(status_code=404, detail="Supply not found")

    # <<< INSERT PASSWORD HERE >>>
    incoming_status = (str((payload or {}).get("status") or "") or "").lower()
    current_is_firm = (str(row.status or "") == "firm")
    will_be_firm     = (incoming_status == "firm")

    if current_is_firm or will_be_firm:
        provided = _provided_pw(delete_password, x_delete_password)
        if not _is_delete_pw_valid(provided):
            raise HTTPException(status_code=401, detail="Updating a firm supply (or making it firm) requires a valid password.")
    # <<< END PASSWORD INSERT >>>
    
    SAFE_FIELDS = {
        "mtpa","mmbtu","cargoes","equity_fraction",
        "start_month","months_active","ghv_mmbtu_per_tonne",
        "cargo_mmbtu","cargo_tonnes","notes","original_unit","original_value",
        "status"
    }

    data = dict(payload or {})

    # unit/value shortcut
    unit = (data.get("unit") or "").strip().lower()
    if "value" in data and unit in {"mtpa","mmbtu","cargoes"}:
        val = float(data["value"])
        if unit == "mtpa":      row.mtpa = val
        elif unit == "mmbtu":   row.mmbtu = val
        elif unit == "cargoes": row.cargoes = val
        row.original_unit = unit
        row.original_value = val

    if "status" in data and data["status"]:
        st = str(data["status"]).lower()
        if st not in {"firm","negotiation","option"}:
            raise HTTPException(status_code=400, detail="status must be firm/negotiation/option")
        row.status = st    

    # coerce numerics where present
    for k in ["mtpa","mmbtu","cargoes","equity_fraction","ghv_mmbtu_per_tonne","cargo_mmbtu","cargo_tonnes","original_value"]:
        if k in data and data[k] not in (None,""):
            setattr(row, k, float(data[k]))
    for k in ["start_month","months_active"]:
        if k in data and data[k] not in (None,""):
            setattr(row, k, int(float(data[k])))

    # apply other safe fields
    for k,v in data.items():
        if k in SAFE_FIELDS:
            setattr(row, k, v)

    db.add(row); db.commit(); db.refresh(row)
    return {"status":"ok","id": row.id}

@app.delete("/api/supply/{row_id}")
def api_delete_supply(
    row_id: int,
    # NEW:
    delete_password: Optional[str] = Query(None),
    x_delete_password: Optional[str] = Header(None, alias="X-Delete-Password"),
    db: Session = Depends(get_db),
):
    row = db.get(Supply, row_id)
    if not row:
        raise HTTPException(status_code=404, detail="Supply not found")

    # NEW: require password for supply deletes
    provided = _provided_pw(delete_password, x_delete_password)
    if not _is_delete_pw_valid(provided):
        raise HTTPException(status_code=401, detail="Supply delete requires a valid password.")

    db.delete(row); db.commit()
    return {"status":"deleted","id": row_id}


# ---------- Opportunity: single UPDATE / DELETE ----------
@app.patch("/api/opportunities/{row_id}")
def api_update_opportunity(
    row_id: int,
    payload: dict,
    delete_password: Optional[str] = Query(None),     # ← NEW
    x_delete_password: Optional[str] = Header(None),  # ← NEW
    db: Session = Depends(get_db),
):
    #row = db.query(Opportunity).get(row_id)
    row = db.get(Opportunity, row_id)
    if not row:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    SAFE_FIELDS = {
        "mtpa","mmbtu","cargoes","probability",
        "status","fob_des","pricing_index",
        "notes","original_unit","original_value"
    }

    data = dict(payload or {})

    # <<< INSERT PASSWORD HERE >>>
    current_firm = (row.status or "").lower() == "firm"
    will_be_firm = (str(data.get("status") or "").lower() == "firm")
    if current_firm or will_be_firm:
        provided = _provided_pw(delete_password, x_delete_password)
        if not _is_delete_pw_valid(provided):
            raise HTTPException(status_code=401, detail="Firm opportunity update requires a valid password.")
    # <<< END PASSWORD INSERT >>>

    # unit/value shortcut
    unit = (data.get("unit") or "").strip().lower()
    if "value" in data and unit in {"mtpa","mmbtu","cargoes"}:
        val = float(data["value"])
        if unit == "mtpa":      row.mtpa = val
        elif unit == "mmbtu":   row.mmbtu = val
        elif unit == "cargoes": row.cargoes = val
        row.original_unit = unit
        row.original_value = val

    # probability/status/fob_des validation
    if "probability" in data and data["probability"] not in (None,""):
        p = float(data["probability"])
        if not (0.0 <= p <= 1.0):
            raise HTTPException(status_code=400, detail="probability must be 0..1")
        row.probability = p
    if "status" in data and data["status"]:
        st = str(data["status"]).lower()
        if st not in {"firm","negotiation","option"}:
            raise HTTPException(status_code=400, detail="status must be firm/negotiation/option")
        row.status = st
    if "fob_des" in data and data["fob_des"]:
        fd = str(data["fob_des"]).upper()
        if fd not in {"FOB","DES"}:
            raise HTTPException(status_code=400, detail="fob_des must be FOB/DES")
        row.fob_des = fd

    # coerce numerics
    for k in ["mtpa","mmbtu","cargoes","original_value"]:
        if k in data and data[k] not in (None,""):
            setattr(row, k, float(data[k]))

    # apply other safe fields
    for k,v in data.items():
        if k in SAFE_FIELDS:
            setattr(row, k, v)

    db.add(row); db.commit(); db.refresh(row)
    return {"status":"ok","id": row.id}

@app.delete("/api/opportunities/{row_id}")
def api_delete_opportunity(
    row_id: int,
    # NEW:
    delete_password: Optional[str] = Query(None),
    x_delete_password: Optional[str] = Header(None, alias="X-Delete-Password"),
    db: Session = Depends(get_db),
):
    row = db.get(Opportunity, row_id)
    if not row:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    # NEW: only protect firm rows
    if (row.status or "").lower() == "firm":
        provided = _provided_pw(delete_password, x_delete_password)
        if not _is_delete_pw_valid(provided):
            raise HTTPException(status_code=401, detail="Firm opportunity delete requires a valid password.")

    db.delete(row); db.commit()
    return {"status":"deleted","id": row_id}




#Add two “GET by id” API endpoints
# ---------- Fetch single Supply / Opportunity by ID (for form prefill) ----------
@app.get("/api/supply/{row_id}")
def api_get_supply(row_id: int, db: Session = Depends(get_db)):
    #row = db.query(Supply).get(row_id)
    row = db.get(Supply, row_id)  # instead of db.query(Supply).get(row_id)
    if not row:
        raise HTTPException(status_code=404, detail="Supply not found")
    return {
        "id": row.id,
        "source": row.source,
        "source_type": row.source_type,
        "region": row.region,
        "scenario": row.scenario,
        "year": row.year,
        "mtpa": row.mtpa,
        "mmbtu": row.mmbtu,
        "cargoes": row.cargoes,
        "equity_fraction": row.equity_fraction,
        "start_month": row.start_month,
        "months_active": row.months_active,
        "ghv_mmbtu_per_tonne": row.ghv_mmbtu_per_tonne,
        "cargo_mmbtu": row.cargo_mmbtu,
        "cargo_tonnes": row.cargo_tonnes,
        "notes": row.notes,
        "original_unit": getattr(row, "original_unit", None),
        "original_value": getattr(row, "original_value", None),
        "status": row.status,
    }

@app.get("/api/opportunities/{row_id}")
def api_get_opportunity(row_id: int, db: Session = Depends(get_db)):
    #row = db.query(Opportunity).get(row_id)
    row = db.get(Opportunity, row_id)
    if not row:
        raise HTTPException(status_code=404, detail="Opportunity not found")
    return {
        "id": row.id,
        "contract_name": row.contract_name,
        "counterparty": row.counterparty,
        "status": row.status,
        "region": row.region,
        "fob_des": row.fob_des,
        "pricing_index": row.pricing_index,
        "year": row.year,
        "mtpa": row.mtpa,
        "mmbtu": row.mmbtu,
        "cargoes": row.cargoes,
        "probability": row.probability,
        "notes": row.notes,
        "original_unit": getattr(row, "original_unit", None),
        "original_value": getattr(row, "original_value", None),
    }


@app.post("/api/ingest/supply_csv")
async def api_ingest_supply_csv(file: UploadFile = File(...), db: Session = Depends(get_db)):
    content = await file.read()
    text = content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    count = 0
    for row in reader:
        payload = {k: (None if (v is None or v == "" or v.lower() == "nan") else v) for k,v in row.items()}
        for k in ["year","original_value","ghv_mmbtu_per_tonne","cargo_mmbtu","cargo_tonnes","start_month","months_active","equity_fraction"]:
            if payload.get(k) is not None:
                try: payload[k] = float(payload[k]) if k!="year" else int(float(payload[k]))
                except: pass
        create_supply(db, payload); count += 1
    return {"status":"ok","rows":count}

@app.post("/api/ingest/opportunity_csv")
async def api_ingest_opp_csv(file: UploadFile = File(...), db: Session = Depends(get_db)):
    content = await file.read()
    text = content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    count = 0
    for row in reader:
        payload = {k: (None if (v is None or v == "" or v.lower() == "nan") else v) for k,v in row.items()}
        for k in ["year","original_value","ghv_mmbtu_per_tonne","cargo_mmbtu","cargo_tonnes","start_month","months_active","probability"]:
            if payload.get(k) is not None:
                try: payload[k] = float(payload[k]) if k!="year" else int(float(payload[k]))
                except: pass
        create_opportunity(db, payload); count += 1
    return {"status":"ok","rows":count}

@app.get("/api/supply")
def api_list_supply(source: Optional[str] = None, source_type: Optional[str] = None, region: Optional[str] = None,
                    scenario: Optional[str] = None, year_from: Optional[int] = None, year_to: Optional[int] = None,
                    db: Session = Depends(get_db)):
    rows = list_supply(db, source, source_type, _norm_region(region), scenario, year_from, year_to)
    return [{
        "id": r.id, "source": r.source, "source_type": r.source_type, "region": r.region, "scenario": r.scenario,
        "year": r.year, "mtpa": r.mtpa, "mmbtu": r.mmbtu, "cargoes": r.cargoes,
        "equity_fraction": r.equity_fraction,
        "equity_mtpa": r.equity_mtpa, "equity_mmbtu": r.equity_mmbtu, "equity_cargoes": r.equity_cargoes,
        "notes": r.notes, "status": r.status
    } for r in rows]

@app.get("/api/opportunities")
def api_list_opps(counterparty: Optional[str] = None, status: Optional[str] = None, region: Optional[str] = None,
                  fob_des: Optional[str] = None, pricing_index: Optional[str] = None,
                  year_from: Optional[int] = None, year_to: Optional[int] = None,
                  db: Session = Depends(get_db)):
    rows = list_opportunities(db, counterparty, status, _norm_region(region), fob_des, pricing_index, year_from, year_to)
    return [{
        "id": r.id, "contract_name": r.contract_name, "counterparty": r.counterparty, "status": r.status,
        "fob_des": r.fob_des, "pricing_index": r.pricing_index, "region": r.region, "year": r.year,
        "mtpa": r.mtpa, "mmbtu": r.mmbtu, "cargoes": r.cargoes, "probability": r.probability, "notes": r.notes
    } for r in rows]

from typing import Annotated
@app.get("/api/series/supply_monthly")
def api_supply_monthly(#unit: str = Query("mtpa", regex="^(mtpa|mmbtu|cargoes)$"),
                       unit: Annotated[str, Query(pattern=r"^(mtpa|mmbtu|cargoes)$")] = "mtpa",     
                       scenario: Optional[str] = None, source: Optional[str] = None, source_type: Optional[str] = None,
                       region: Optional[str] = None, year_from: Optional[int] = None, year_to: Optional[int] = None,
                       basis: str = Query("equity", regex="^(equity|gross)$"), db: Session = Depends(get_db)):
    return get_supply_monthly(db, unit, scenario, source, source_type, _norm_region(region), year_from, year_to, basis=basis)

@app.get("/api/series/opportunity_monthly")
def api_opportunity_monthly(#unit: str = Query("mtpa", regex="^(mtpa|mmbtu|cargoes)$"),
                            unit: Annotated[str, Query(pattern=r"^(mtpa|mmbtu|cargoes)$")] = "mtpa",   
                            status: Optional[str] = None, counterparty: Optional[str] = None, region: Optional[str] = None,
                            year_from: Optional[int] = None, year_to: Optional[int] = None,
                            probability_weighted: bool = False, db: Session = Depends(get_db)):
    return get_opportunity_monthly(db, unit, status, counterparty, _norm_region(region), year_from, year_to, probability_weighted)

@app.get("/api/series/gap_monthly")
def api_gap_monthly(#unit: str = Query("mtpa", regex="^(mtpa|mmbtu|cargoes)$"),
                    unit: Annotated[str, Query(pattern=r"^(mtpa|mmbtu|cargoes)$")] = "mtpa",
                    scenario: Optional[str] = None, region: Optional[str] = None,
                    year_from: Optional[int] = None, year_to: Optional[int] = None,
                    probability_weighted: bool = False, basis: str = Query("equity", regex="^(equity|gross)$"),
                    db: Session = Depends(get_db)):
    return get_gap_monthly(db, unit, scenario, _norm_region(region), year_from, year_to, probability_weighted, basis=basis)

# ---- Dash UI ----
import dash
import plotly.graph_objects as go
from fastapi.middleware.wsgi import WSGIMiddleware

'''def _fetch(path, params=None):
    try:
        r = requests.get("http://127.0.0.1:8000"+path, params=params, timeout=5)
        return r.json()
    except Exception:
        return []'''

# at top-level
import httpx
_asgi_client = httpx.Client(transport=httpx.ASGITransport(app=app), base_url="http://internal")


from starlette.testclient import TestClient
_asgi_client = TestClient(app)

def _fetch(path, params=None):
    try:
        r = _asgi_client.get(path, params=params, timeout=5.0)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []

@app.get("/__dash_debug")
def dash_debug():
    try:
        r_html = _asgi_client.get("/dash/")
        r_layout = _asgi_client.get("/dash/_dash-layout")
        r_deps = _asgi_client.get("/dash/_dash-dependencies")
        return {
            "index": r_html.status_code,
            "layout": r_layout.status_code,
            "dependencies": r_deps.status_code,
            "requests_prefix": dash_app.config.requests_pathname_prefix,
            "routes_prefix": dash_app.config.routes_pathname_prefix,
            "assets_path": dash_app.config.assets_url_path,
        }
    except Exception as e:
        return {"error": str(e)}    

from dash.exceptions import PreventUpdate
    
'''dash_app = dash.Dash(
    __name__,
    requests_pathname_prefix="/dash/",  # browser will call /dash/_dash-layout, /dash/_dash-update-component, etc.
    routes_pathname_prefix="/",         # inside the mounted app, the WSGI PATH_INFO starts at /
    suppress_callback_exceptions=True
)'''
dash_app = dash.Dash(
    __name__,
    requests_pathname_prefix="/dash/",
    routes_pathname_prefix="/",
    assets_url_path="/dash/assets",          # ← add this
    suppress_callback_exceptions=True,
    serve_locally=True,  # <- helps serve component bundles locally
)

'''dash_app = dash.Dash(
    __name__,
    requests_pathname_prefix="/dash/",
    routes_pathname_prefix="/dash/",   # <— change from "/"
    assets_url_path="/dash/assets",
    suppress_callback_exceptions=True,
    serve_locally=True,                # optional, but avoids CDN during dev
)'''

'''dash_app = dash.Dash(
    __name__,
    requests_pathname_prefix="/dash/",  # what the browser uses
    routes_pathname_prefix="/",         # what Dash sees inside the mount
    assets_url_path="/dash/assets",
    suppress_callback_exceptions=True,
)'''

# ---- Executive Plotly Templates (Light + Dark) ----
exec_light = go.layout.Template(
    layout=go.Layout(
        colorway=["#4E79A7","#F28E2B","#E15759","#76B7B2","#59A14F",
                  "#EDC949","#AF7AA1","#FF9DA7","#9C755F","#BAB0AC"],
        font=dict(family="Inter, Segoe UI, Roboto, sans-serif", color="#111827", size=13),
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        xaxis=dict(gridcolor="#E5E7EB", zeroline=False, linecolor="#9CA3AF", automargin=True),
        yaxis=dict(gridcolor="#E5E7EB", zeroline=False, linecolor="#9CA3AF", automargin=True),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
    )
)
pio.templates["exec_light"] = exec_light

exec_dark = go.layout.Template(
    layout=go.Layout(
        colorway=["#60A5FA","#F59E0B","#F87171","#67E8F9","#22C55E",
                  "#FDE68A","#C4B5FD","#FDA4AF","#D6D3D1","#94A3B8"],
        font=dict(family="Inter, Segoe UI, Roboto, sans-serif", color="#E5E7EB", size=13),
        paper_bgcolor="#0F172A",
        plot_bgcolor="#0F172A",
        xaxis=dict(gridcolor="#334155", zeroline=False, linecolor="#475569", automargin=True),
        yaxis=dict(gridcolor="#334155", zeroline=False, linecolor="#475569", automargin=True),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
    )
)
pio.templates["exec_dark"] = exec_dark

# Pick your default
pio.templates.default = "plotly_white+exec_light"
# For dark mode, switch to:
# pio.templates.default = "plotly_dark+exec_dark"

# ---- Woodside-inspired executive palette ----
# Woodside-ish palette
COL = {
    "p50": "#0F766E",
    "p90": "#C8102E",
    "after": "#111827",
    "firm": "#0F766E",
    "nego": "#C8102E",
    "option": "#1E3A8A",
    "total": "#374151",
    "uncommitted": "#94A3B8",
}

# Create the template in one shot (so the key definitely exists)
pio.templates["exec"] = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(family="Inter, Segoe UI, Arial", size=13, color="#111827"),
        legend=dict(title=None, bgcolor="rgba(0,0,0,0)"),
        colorway=[  # <- set colorway here to avoid KeyError
            COL["firm"], COL["nego"], COL["option"], COL["total"], COL["uncommitted"]
        ],
    )
)
pio.templates.default = "exec"

# ---- Contrast helpers (place right after COL palette) ----
def _hex_to_rgb(h: str):
    h = h.strip().lstrip("#")
    if len(h) == 3:
        h = "".join([c*2 for c in h])
    r = int(h[0:2], 16) / 255.0
    g = int(h[2:4], 16) / 255.0
    b = int(h[4:6], 16) / 255.0
    return r, g, b

def _rel_lum(r, g, b):
    def _c(c):
        return c/12.92 if c <= 0.03928 else ((c+0.055)/1.055)**2.4
    R, G, B = _c(r), _c(g), _c(b)
    return 0.2126*R + 0.7152*G + 0.0722*B

def contrast_text_color(bg_hex: str, dark="#111827", light="#FFFFFF"):
    """Return dark text for light backgrounds, white text for dark ones."""
    r, g, b = _hex_to_rgb(bg_hex)
    return dark if _rel_lum(r, g, b) > 0.5 else light

# Pick text & chip fills for the UNCOMMITTED hashed bars
UNCOMMITTED_TEXT = contrast_text_color(COL["uncommitted"])   # or COL["uncommitted_hat"] if that's your key
UNCOMMITTED_CHIP = (
    "rgba(255,255,255,0.92)" if UNCOMMITTED_TEXT != "#FFFFFF" else "rgba(17,24,39,0.85)"
)

def add_pct_chips(fig, xvals, labels, row=None, col=None):
    """
    Add small, high-contrast % chips just above the plotting area.
    Works for single-axes figures and subplots (pass row/col for the target cell).
    """
    for x, txt in zip(xvals, labels):
        fig.add_annotation(
            x=x, y=1.02, xref="x", yref="y domain",
            text=txt,
            showarrow=False,
            bgcolor=UNCOMMITTED_CHIP,
            font=dict(size=14, color=UNCOMMITTED_TEXT,
                      family="Inter, Segoe UI, Roboto, sans-serif"),
            bordercolor="rgba(0,0,0,0.25)", borderwidth=1, borderpad=4,
            opacity=0.98,
            row=row, col=col,         # <-- make it subplot-aware
        )
    # safe layout touch-ups (no layout-level cliponaxis!)
    fig.update_layout(uniformtext_minsize=12, uniformtext_mode="show")

### Put the percentage outside the hashed bars
def move_uncommitted_labels_top(fig, percent_mode: bool):
    """
    Puts the Uncommitted % labels ABOVE the bars.
    - percent_mode=True  -> chart is a 100% stacked view (set y up to 110)
    - percent_mode=False -> absolute stacked (adds headroom based on stack max)
    """
    # Make sure outside labels aren't clipped
    fig.update_traces(selector=dict(type="bar"), cliponaxis=False)

    # ✅ Use a callable selector (NOT a lambda inside a dict)
    fig.update_traces(
        selector=lambda tr: (
            getattr(tr, "type", None) == "bar"
            and "uncommitted" in (getattr(tr, "name", "") or "").lower()
        ),
        textposition="outside",
        texttemplate="%{text}",
        textfont=dict(size=14),
    )

    # Add headroom so the outside labels show
    if percent_mode:
        fig.update_yaxes(range=[0, 110])
        return

    # For absolute stacked bars, find the tallest stack and add 15% headroom
    from collections import defaultdict
    stacks = defaultdict(float)
    for tr in fig.data:
        if getattr(tr, "type", None) != "bar":
            continue
        xs = list(getattr(tr, "x", []) or [])
        ys = list(getattr(tr, "y", []) or [])
        for x, y in zip(xs, ys):
            try:
                stacks[x] += float(y or 0.0)
            except Exception:
                pass
    ymax = max(stacks.values()) if stacks else 0.0
    fig.update_yaxes(range=[0, ymax * 1.15])
    fig.update_layout(uniformtext_minsize=12, uniformtext_mode="show")

#####
def move_uncommitted_labels_top_subplot(fig, bar_row=2, bar_col=1, is_fraction=True):
    """
    For a make_subplots figure where the percent bars live at (row=bar_row, col=bar_col),
    put the Uncommitted labels outside the bars and add a bit of headroom so they show.

    Assumes y-values are 0..1 when is_fraction=True (your current code).
    """
    # Put labels outside only on the Uncommitted bars
    fig.update_traces(
        selector=lambda tr: (getattr(tr, "type", None) == "bar") and
                            ("uncommitted" in ((tr.name or "").lower())),
        textposition="outside",
        texttemplate="%{y:.0%}" if is_fraction else "%{text}",
        textfont=dict(size=14),
        cliponaxis=False,
        row=bar_row, col=bar_col
    )

    # Add headroom on the bars axis, in the correct units
    if is_fraction:
        fig.update_yaxes(range=[0, 1.12], row=bar_row, col=bar_col)  # a touch above 100%
    else:
        fig.update_yaxes(range=[0, 110], row=bar_row, col=bar_col)

    # Keep text rendering predictable
    fig.update_layout(uniformtext_minsize=12, uniformtext_mode="show")    
    
####    

# Add this:
dash_app.enable_dev_tools(
    dev_tools_ui=True,
    dev_tools_props_check=True,
    dev_tools_silence_routes_logging=False,
    dev_tools_serve_dev_bundles=True,
)

dash_app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    html.Div(id="page-container")
])


def layout_dashboard():
    return html.Div([
        html.H2("iTrackVol Dashboard"),

        #html.Div([
        #    dcc.Link(html.Button("Update Supply / Opportunity"), href="/dash/manage")
        #], style={"margin":"8px 0 16px"}),

        # Manage_handle
        html.Div(
            "Update Supply / Opportunity",
            id="manage_title",  # not used for dragging
            style={"fontWeight":"600","fontSize":"18px"},
        ),
        
        # --- open modal button ---
        html.Div([
            html.Button("Update Supply / Opportunity", id="open_manage", n_clicks=0)
        ], style={"margin":"8px 0 16px"}),

        # --- modal state + container (ADD THESE HERE) ---
        dcc.Store(id="manage_open", data=False),
        # --- Add two stores (drag state + position)
        dcc.Store(id="drag_state", data={"dragging": False}),
        dcc.Store(id="modal_pos", data={"x": 120, "y": 80}),  # initial left/top in px

        # NEW: selected supply "source" to focus
        dcc.Store(id="focus_source", data=None),
        # NEW: selected opportunity "counterparty" to focus
        dcc.Store(id="focus_counterparty", data=None),

        #from dash_extensions import EventListener  # add at top with other imports
        
        html.Div(
            id="manage_modal",
            style={"display": "none"},
            children=[
                EventListener(
                    id="drag_listener",
                    events=[
                        # Start the drag only when you press on the handle
                        {"event": "mousedown", "props": ["clientX", "clientY", "target.id"]},

                        # Keep receiving events anywhere on the page during the drag
                        {"event": "document:mousemove", "props": ["clientX", "clientY"]},
                        {"event": "document:mouseup",   "props": ["clientX", "clientY"]},
                    ],
                    children=html.Div(
                        id="manage_card",
                        style={
                            "position": "fixed",
                            "left": "120px",
                            "top":  "80px",
                            "background": "#fff",
                            "borderRadius": "10px",
                            "width": "92%",
                            "maxWidth": "1100px",
                            "maxHeight": "90vh",
                            "overflowY": "auto",
                            "boxShadow": "0 20px 50px rgba(0,0,0,.3)",
                            "padding": "16px",
                            "zIndex": 1000,   # keep on top
                        },
                        children=[
                            html.Div(
                                "Update Supply / Opportunity",
                                id="manage_drag_handle",
                                style={"fontWeight":"600","fontSize":"18px","cursor":"move","userSelect":"none"}
                            ),
                            html.Button("✕", id="close_manage", n_clicks=0,
                                        style={"fontSize":"20px","background":"transparent","border":"none","cursor":"pointer"}),
                            layout_manage_entries(show_back=False),
                        ],
                    ),
                ),
            ],
        ),  # ⬅️ add this comma to separate from the next sibling
        # --- controls row (everything you already had continues here version main27.py) ---
        
        # --- controls row ---
        html.Div([
            html.Div([html.Label("Scenario"), dcc.Dropdown(id="scenario", options=[{"label": s, "value": s} for s in ["P50", "P90", "Both"]], value="P50", clearable=True)], style={"display":"inline-block","width":"15%","paddingRight":"8px"}),
            html.Div([html.Label("Region"), dcc.Dropdown(id="region", options=[{"label": r, "value": r} for r in ["APAC","Atlantic", "Middle East", "Global"]], value=None, clearable=True)], style={"display":"inline-block","width":"15%","paddingRight":"8px"}),
            html.Div([html.Label("Unit"), dcc.Dropdown(id="unit", options=[{"label":u.upper(),"value":u} for u in ["mtpa","mmbtu","cargoes"]], value="mtpa")], style={"display":"inline-block","width":"12%","paddingRight":"8px"}),
            html.Div([html.Label("Basis"), dcc.Dropdown(id="basis", options=[{"label":"Equity","value":"equity"},{"label":"Gross","value":"gross"}], value="equity")], style={"display":"inline-block","width":"12%","paddingRight":"8px"}),
            html.Div([html.Label("Granularity"), dcc.Dropdown(id="granularity", options=[{"label": g, "value": g} for g in ["Monthly", "Yearly"]], value="Yearly")], style={"display":"inline-block","width":"12%","paddingRight":"8px"}),

            html.Div([html.Label("Sell-down Source"), dcc.Dropdown(id="sell_source", options=[], placeholder="Select a source", clearable=True)], style={"display":"inline-block","width":"18%","paddingRight":"8px"}),

            html.Div([html.Label("Sell-down %"), dcc.Input(id="sell_pct", type="number", min=-100, max=100, step=0.1, value=0)], style={"display":"inline-block","width":"14%","paddingRight":"8px"}),
            #html.Div([html.Label("Gap after sell-down"), dcc.Checklist(id="gap_apply_sell", options=[{"label": "Apply", "value": "yes"}], value=[], style={"marginTop": "8px"})], style={"display":"inline-block","width":"14%","paddingRight":"8px","verticalAlign":"top"}),
            html.Div([html.Label("Year From"), dcc.Input(id="year_from", type="number", value=2025)], style={"display":"inline-block","width":"12%","paddingRight":"8px"}),
            html.Div([html.Label("Year To"), dcc.Input(id="year_to", type="number", value=2035)], style={"display":"inline-block","width":"12%","paddingRight":"8px"}),
            html.Div([html.Label("Prob-weighted Opp"), dcc.Checklist(id="prob_w", options=[{"label":"Yes","value":"yes"}], value=[])], style={"display":"inline-block","width":"20%","verticalAlign":"top"}),
        ], style={"marginBottom":"10px"}),

        html.Div([
            html.Label("Export chart to CSV", style={"marginRight":"8px"}),
            dcc.Dropdown(
                id="export_chart_pick",
                options=[
                    {"label": "Supply (lines)", "value": "supply_chart"},
                    {"label": "Supply (stacked area)", "value": "supply_chart_area"},
                    {"label": "Sold vs Uncommitted (stacked %)", "value": "sold_uncommitted_chart"},
                    {"label": "Sold vs Uncommitted (absolute)", "value": "sold_uncommitted_abs_chart"},
                    {"label": "Sold vs Uncommitted (percent only)", "value": "sold_uncommitted_pct_chart"},
                    {"label": "Opportunities (lines)", "value": "opportunity_chart"},
                    {"label": "Contracted + Negotiations (lines)", "value": "contracted_neg_chart"},
                    {"label": "Focus: Source", "value": "source_focus_chart"},
                    {"label": "Focus: Counterparty", "value": "counterparty_focus_chart"},
                ],
                value="supply_chart",
                clearable=False,
                style={"width": "420px"}
            ),
            html.Button("Export CSV", id="btn_export", n_clicks=0, style={"marginLeft":"8px"}),
            dcc.Download(id="dl_chart_csv"),
        ], style={
            "margin": "8px 0 16px",
            "display": "flex",
            "justifyContent": "flex-end",
            "alignItems": "center",
            "gap": "8px",
        }),

        # Advanced plan details
        html.Details([
            html.Summary("Advanced sell-down / upside plan (by source & year)"),
            html.Div([
                html.Div([
                    html.Button("Add row", id="sd_add_row", n_clicks=0),
                    html.Button("Clear", id="sd_clear", n_clicks=0, style={"marginLeft":"8px"}),
                ], style={"margin":"6px 0"}),

                dash_table.DataTable(
                    id="sd_table",
                    columns=[
                        {"name":"Source","id":"source","presentation":"dropdown"},
                        {"name":"Year","id":"year","type":"numeric"},
                        {"name":"% (±)","id":"pct","type":"numeric"},
                    ],
                    data=[],
                    editable=True,
                    row_deletable=True,
                    dropdown={"source": {"options": []}},
                    style_table={"maxWidth":"900px","overflowX":"auto"}
                ),

                dcc.Store(id="sd_rules", data=[]),
            ], style={"marginTop":"8px"})
        ], open=False, style={"margin":"8px 0 16px"}),

        # NEW: Focus-by-Source panel (appears after clicking a Supply row)
        html.Div(
            id="source_focus_panel",
            style={"display": "none", "margin": "12px 0"},
            children=[
                html.Div(
                    style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"},
                    children=[
                        html.Div([
                            html.Span("Focus: ", style={"color":"#666"}),
                            html.Strong(id="source_focus_title"),
                            html.Span(" — Volume over time", style={"color":"#666"})
                        ]),
                        html.Button("Clear focus", id="clear_source_focus", n_clicks=0)
                    ]
                ),
                dcc.Graph(id="source_focus_chart")
            ]
        ),

        # NEW: Focus-by-Counterparty panel (Opportunities)
        html.Div(
            id="counterparty_focus_panel",
            style={"display": "none", "margin": "12px 0"},
            children=[
                html.Div(
                    style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"},
                    children=[
                        html.Div([
                            html.Span("Focus: ", style={"color":"#666"}),
                            html.Strong(id="counterparty_focus_title"),
                            html.Span(" — Opportunities over time", style={"color":"#666"})
                        ]),
                        html.Button("Clear focus", id="clear_counterparty_focus", n_clicks=0)
                    ]
                ),
                dcc.Graph(id="counterparty_focus_chart")
            ]
        ),

        dcc.Graph(id="contracted_neg_chart"), # fig_cn 
        dcc.Graph(id="sold_uncommitted_pct_chart"),  # fig_su_pct 
        dcc.Graph(id="supply_chart"),  # fig_s
        dcc.Graph(id="supply_chart_area"),  # fig_sa
        dcc.Graph(id="sold_uncommitted_chart"),  # fig_su
        dcc.Graph(id="sold_uncommitted_abs_chart"),  # fig_su_abs
        #dcc.Graph(id="sold_uncommitted_pct_chart"),  # 
        dcc.Graph(id="opportunity_chart"), # fig_o
        #dcc.Graph(id="contracted_neg_chart"),
        #dcc.Graph(id="gap_chart"),

        html.H3("Supply (annual)"),
        dash_table.DataTable(id="tbl_supply", page_size=10, style_table={"overflowX":"auto"}),

        html.H3("Opportunities (annual)"),
        dash_table.DataTable(id="tbl_opp", page_size=10, style_table={"overflowX":"auto"}),
    ])

def layout_manage_entries(show_back: bool = True):
    return html.Div([
        html.H2("Update Supply / Opportunity"),
        #dcc.Link("← Back to Dashboard", href="/dash/", style={"display":"inline-block","marginBottom":"16px"}),
        # top link (only when not in modal)
        (
            dcc.Link("← Back to Dashboard", href="/",   #href="/dash/",
                     style={"display":"inline-block","marginBottom":"16px"})
            if show_back else html.Div(style={"height":"8px"})
        ),

        html.Div([
            html.Div([
                html.Label("Entry Type"),
                dcc.RadioItems(
                    id="entry_type",
                    options=[{"label":"Supply","value":"supply"},{"label":"Opportunity","value":"opportunity"}],
                    value="supply", inline=True
                )
            ], style={"marginRight":"24px"}),

            html.Div([
                html.Label("Action"),
                dcc.RadioItems(
                    id="action_type",
                    options=[
                        {"label":"Create","value":"create"},
                        {"label":"Update","value":"update"},
                        {"label":"Delete","value":"delete"},
                    ],
                    value="update", inline=True
                )
            ]),
            html.Div([
                dcc.Checklist(
                    id="bulk_update",
                    options=[{"label": "Use bulk plan (by year)", "value": "yes"}],
                    value=[],
                    style={"marginTop":"6px"}
                )
            ], style={"marginBottom":"8px"})
        ], style={"display":"flex","gap":"24px","marginBottom":"12px"}),

        # Selector for update/delete
        html.Div(id="record_selector", children=[
            html.H4("Select existing record (for Update/Delete)"),
            html.Div([
                html.Div([html.Label("Scenario (Supply only)"),
                          dcc.Dropdown(id="flt_scn", options=[{"label":s,"value":s} for s in ["P50","P90"]], clearable=True)],
                         style={"width":"16%","paddingRight":"8px","display":"inline-block"}),
                html.Div([html.Label("Region"),
                          dcc.Dropdown(id="flt_region", options=[{"label":r,"value":r} for r in ["APAC","Atlantic", "Middle East"]], clearable=True)],
                         style={"width":"16%","paddingRight":"8px","display":"inline-block"}),
                html.Div([html.Label("Year From"), dcc.Input(id="flt_yf", type="number", value=2025)],
                         style={"width":"12%","paddingRight":"8px","display":"inline-block"}),
                html.Div([html.Label("Year To"), dcc.Input(id="flt_yt", type="number", value=2035)],
                         style={"width":"12%","paddingRight":"8px","display":"inline-block"}),

                # NEW: text filters for bulk delete convenience
                html.Div([html.Label("Source (Supply)"), dcc.Input(id="flt_source", type="text")],
                         style={"width":"18%","paddingRight":"8px","display":"inline-block"}),
                html.Div([html.Label("Counterparty (Opp)"), dcc.Input(id="flt_counterparty", type="text")],
                         style={"width":"18%","paddingRight":"8px","display":"inline-block"}),

                html.Button("Load records", id="load_records", style={"marginTop":"22px"}),
            ], style={"marginBottom":"8px"}),

            dcc.Dropdown(id="sel_record", options=[], placeholder="Select a record...", clearable=False, style={"width":"60%"}),
            #dcc.Checklist(id="bulk_update",options=[{"label":"Update ALL filtered records in year range (above)", "value":"yes"}],value=[],style={"marginTop":"8px"}), ## Bulk Update
            html.Div(id="sel_help", style={"color":"#666","marginTop":"6px"}),
            ####
            #Old password was here ...
            # === NEW: bulk delete selection panel ===
            html.Div(
                id="bulk_delete_panel",
                style={"display": "none", "marginTop": "12px"},
                children=[
                    html.Div(
                        "Select rows to delete from the results below. "
                        "If you leave the selection empty, the delete will apply to ALL search matches after confirmation.",
                        style={"color":"#666","margin":"6px 0 8px"}
                    ),
                    dash_table.DataTable(
                        id="bulk_delete_table",
                        columns=[],
                        data=[],
                        page_size=10,
                        row_selectable="multi",
                        style_table={"overflowX":"auto"},
                        style_cell={"fontSize":"12px"},
                    ),
                ],
            ),
        ], style={"border":"1px solid #ddd","padding":"12px","borderRadius":"8px","marginBottom":"16px"}),

        html.Hr(),

        # ---------- Supply form ----------
        html.Div(id="supply_form", children=[
            html.H4("Supply"),
            html.Div([
                html.Div([html.Label("Source"), dcc.Input(id="sup_source", type="text", style={"width":"100%"})], className="col"),
                html.Div([html.Label("Source Type"), dcc.Dropdown(id="sup_source_type", options=[{"label":x,"value":x} for x in ["asset","offtake","portfolio","third_party","tolling"]], clearable=True)], className="col"),
                html.Div([html.Label("Region"), dcc.Dropdown(id="sup_region", options=[{"label":r,"value":r} for r in ["APAC","Atlantic","Middle East"]], clearable=True)], className="col"),
                html.Div([html.Label("Scenario"), dcc.Dropdown(id="sup_scenario", options=[{"label":s,"value":s} for s in ["P50","P90"]], value="P50")], className="col"),
                html.Div([html.Label("Status"), dcc.Dropdown(id="sup_status", options=[{"label":s,"value":s} for s in ["firm","negotiation","option"]], value="negotiation")], className="col"),  # ← NEW
            ], style={"display":"grid","gridTemplateColumns":"repeat(5,1fr)","gap":"8px","marginBottom":"8px"}),

            html.Div([
                html.Div([html.Label("Unit"), dcc.Dropdown(id="sup_unit", options=[{"label":u.upper(),"value":u} for u in ["mtpa","mmbtu","cargoes"]], value="mtpa")], className="col"),
                html.Div([html.Label("Value"), dcc.Input(id="sup_value", type="number", style={"width":"100%"})], className="col"),
                html.Div([html.Label("Year"), dcc.Input(id="sup_year", type="number", value=2025, style={"width":"100%"})], className="col"),
                html.Div([html.Label("Equity Fraction (0-1)"), dcc.Input(id="sup_equity", type="number", min=0, max=1, step=0.05, value=1.0, style={"width":"100%"})], className="col"),
            ], style={"display":"grid","gridTemplateColumns":"repeat(4,1fr)","gap":"8px","marginBottom":"8px"}),

            html.Div([
                html.Div([html.Label("Start Month (1-12)"), dcc.Input(id="sup_start_month", type="number", min=1, max=12, step=1, style={"width":"100%"})]),
                html.Div([html.Label("Months Active"), dcc.Input(id="sup_months_active", type="number", min=1, max=12, step=1, style={"width":"100%"})]),
                html.Div([html.Label("GHV mmbtu/tonne"), dcc.Input(id="sup_ghv", type="number", style={"width":"100%"})]),
                html.Div([html.Label("Notes"), dcc.Input(id="sup_notes", type="text", style={"width":"100%"})]),
            ], style={"display":"grid","gridTemplateColumns":"repeat(4,1fr)","gap":"8px"}),
        ], style={"border":"1px solid #ddd","padding":"12px","borderRadius":"8px","marginBottom":"16px"}),

        # ---------- Opportunity form ----------
        html.Div(id="opp_form", children=[
            html.H4("Opportunity"),
            html.Div([
                html.Div([html.Label("Contract Name"), dcc.Input(id="opp_contract_name", type="text", style={"width":"100%"})]),
                html.Div([html.Label("Counterparty"), dcc.Input(id="opp_counterparty", type="text", style={"width":"100%"})]),
                html.Div([html.Label("Status"), dcc.Dropdown(id="opp_status", options=[{"label":s,"value":s} for s in ["firm","negotiation","option"]], value="negotiation")]),
                html.Div([html.Label("Region"), dcc.Dropdown(id="opp_region", options=[{"label":r,"value":r} for r in ["APAC","Atlantic", "Middle East"]], clearable=True)]),
            ], style={"display":"grid","gridTemplateColumns":"repeat(4,1fr)","gap":"8px","marginBottom":"8px"}),

            html.Div([
                html.Div([html.Label("FOB/DES"), dcc.Dropdown(id="opp_fob_des", options=[{"label":"FOB","value":"FOB"},{"label":"DES","value":"DES"}], clearable=True)]),
                html.Div([html.Label("Pricing Index"), dcc.Input(id="opp_pricing", type="text", style={"width":"100%"})]),
                html.Div([html.Label("Unit"), dcc.Dropdown(id="opp_unit", options=[{"label":u.upper(),"value":u} for u in ["mtpa","mmbtu","cargoes"]], value="mtpa")]),
                html.Div([html.Label("Value"), dcc.Input(id="opp_value", type="number", style={"width":"100%"})]),
            ], style={"display":"grid","gridTemplateColumns":"repeat(4,1fr)","gap":"8px","marginBottom":"8px"}),

            html.Div([
                html.Div([html.Label("Year"), dcc.Input(id="opp_year", type="number", value=2025, style={"width":"100%"})]),
                html.Div([html.Label("Probability (0-1)"), dcc.Input(id="opp_probability", type="number", min=0, max=1, step=0.05, value=0.5, style={"width":"100%"})]),
                html.Div([html.Label("Notes"), dcc.Input(id="opp_notes", type="text", style={"width":"100%"})]),
            ], style={"display":"grid","gridTemplateColumns":"repeat(3,1fr)","gap":"8px"}),
        ], style={"border":"1px solid #ddd","padding":"12px","borderRadius":"8px","marginBottom":"16px"}),

        #Frontend: add a small “Bulk values by year” grid
        html.Details([
            html.Summary("Bulk values by year (optional)"),
            html.Div([
                html.Div([
                    html.Button("Generate rows from filters", id="bp_generate", n_clicks=0),
                    html.Button("Add row", id="bp_add_row", n_clicks=0, style={"marginLeft":"8px"}),
                    html.Button("Clear", id="bp_clear", n_clicks=0, style={"marginLeft":"8px"}),
                    html.Div("Tip: uses the Unit you selected in the form (Supply: Unit; Opportunity: Unit).", style={"color":"#666","marginTop":"6px"})
                ], style={"margin":"6px 0"}),

                dash_table.DataTable(
                    id="bulk_plan",
                    columns=[
                        {"name":"Year","id":"year","type":"numeric"},
                        {"name":"Value","id":"value","type":"numeric"},
                    ],
                    data=[],
                    editable=True,
                    row_deletable=True,
                    style_table={"maxWidth":"500px","overflowX":"auto"}
                ),
            ], style={"marginTop":"8px"})
        ], open=False, id="bulk_plan_panel", style={"margin":"8px 0 16px","display":"none"}),

        #html.Div([
        #    html.Label("Delete password"),
        #    dcc.Input(id="delete_pw", type="password", placeholder="Required for deletes"),
        #], style={"margin":"8px 0"}),

        # ADD: password entry (shown only for Delete action)
        
        # Password for protected actions (delete, update firm rows, create firm supply, etc.)
        html.Div(
            id="protected_pw_wrap",
            style={"display": "none", "margin": "8px 0"},
            children=[
                html.Label("Password (required for protected actions)"),
                dcc.Input(id="delete_pw", type="password", style={"width": "280px"}),
            ],
        ),
        # Password for protected actions (delete, update firm rows, create firm supply, etc.)
        #### End of Password
        html.Button("Submit", id="submit_btn"),
        html.Span(id="submit_msg", style={"marginLeft":"10px","fontWeight":"bold"}),

        dcc.Store(id="pending_bulk_delete", data=None),
        dcc.ConfirmDialog(id="confirm_bulk_delete", displayed=False, message=""),

        html.Div(style={"height":"16px"}),
        #dcc.Link("← Back to Dashboard", href="/dash/"),
        # bottom link (only when not in modal)
        (
            dcc.Link("← Back to Dashboard", href="/dash/")
            if show_back else html.Div()
        ),
    ])


@dash_app.callback(
    Output("page-container", "children"),
    Input("url", "pathname")
)
def render_page(pathname):
    # normalize to the app’s internal paths
    p = (pathname or "")
    for pref in (
        (dash_app.config.requests_pathname_prefix or "").rstrip("/"),
        (dash_app.config.routes_pathname_prefix or "").rstrip("/"),
    ):
        if pref and p.startswith(pref):
            p = p[len(pref):]
    p = (p or "/").rstrip("/")

    if p in ("", "/"):
        return layout_dashboard()
    if p in ("/manage", "/new"):
        return layout_manage_entries()

    return html.Div([
        html.H3("404 — Page not found"),
        dcc.Link("Go to Dashboard", href=dash_app.get_relative_path("/")),
    ])

###Show this panel only when Bulk Update is enabled. Add this callback:
@dash_app.callback(
    Output("bulk_plan_panel","style"),
    Input("bulk_update","value"),
    Input("action_type","value")
)
def toggle_bulk_plan_panel(bulk, action_type):
    show = ("yes" in (bulk or [])) and (action_type in ("create","update"))
    return {"margin":"8px 0 16px","display":"block"} if show else {"display":"none"}

#####
@dash_app.callback(
    Output("bulk_delete_panel","style"),
    Input("action_type","value"),
    Input("bulk_update","value"),
)
def toggle_bulk_delete_panel(action_type, bulk):
    show = (action_type == "delete") and ("yes" in (bulk or []))
    return {"display":"block","marginTop":"12px"} if show else {"display":"none"}

#### password callback
@dash_app.callback(
    Output("protected_pw_wrap","style"),   # <- was delete_pw_wrap
    Input("action_type","value"),
    Input("entry_type","value"),
    Input("sup_status","value"),
    Input("opp_status","value"),
)
def _toggle_delete_pw(action_type, entry_type, sup_status, opp_status):
    # show for delete/update always
    show = action_type in ("delete", "update")

    # also show when creating a firm Supply
    creating_firm_supply = (
        action_type == "create"
        and entry_type == "supply"
        and str(sup_status or "").lower() == "firm"
    )

    show = show or creating_firm_supply
    return {"display": "block", "margin": "8px 0"} if show else {"display": "none"}

##### Generate rows for the selected range, manage add/clear:

@dash_app.callback(
    Output("bulk_plan","data"),
    Input("bp_generate","n_clicks"),
    Input("bp_add_row","n_clicks"),
    Input("bp_clear","n_clicks"),
    State("flt_yf","value"),
    State("flt_yt","value"),
    State("bulk_plan","data"),
    prevent_initial_call=True
)
def manage_bulk_plan(n_gen, n_add, n_clear, yf, yt, rows):
    import dash
    ctx = dash.callback_context
    rows = rows or []
    if not ctx.triggered:
        return rows
    trig = ctx.triggered[0]["prop_id"].split(".")[0]

    if trig == "bp_clear":
        return []
    if trig == "bp_add_row":
        return rows + [{"year": None, "value": None}]
    if trig == "bp_generate":
        try:
            yf = int(float(yf)); yt = int(float(yt))
        except Exception:
            return rows
        if yf > yt:
            yf, yt = yt, yf
        return [{"year": y, "value": None} for y in range(yf, yt+1)]
    return rows

##### Add two small callbacks for Modal Pop-up

'''@dash_app.callback(
    dash.Output("manage_open", "data"),
    dash.Input("open_manage", "n_clicks"),
    dash.Input("close_manage", "n_clicks"),
    dash.State("manage_open", "data"),
    prevent_initial_call=True
)'''
# AFTER
@dash_app.callback(
    Output("manage_open", "data"),
    Input("open_manage", "n_clicks"),
    Input("close_manage", "n_clicks"),
    State("manage_open", "data"),
    prevent_initial_call=True
)
def toggle_manage_modal(n_open, n_close, is_open):
    import dash
    if not dash.callback_context.triggered:
        raise PreventUpdate
    trig = dash.callback_context.triggered[0]["prop_id"].split(".")[0]
    if trig == "open_manage":
        return True
    if trig == "close_manage":
        return False
    raise PreventUpdate


@dash_app.callback(
    Output("manage_modal", "style"),
    Input("manage_open", "data")
)
def show_hide_manage_modal(is_open):
    base = {
        "display": "block" if is_open else "none"
    }
    return base

##### Process mouse events → update drag state + position #####

@dash_app.callback(
    Output("drag_state", "data"),
    Output("modal_pos", "data"),
    Input("drag_listener", "event"),
    State("drag_state", "data"),
    State("modal_pos", "data"),
    prevent_initial_call=True
)
def _drag_modal(ev, state, pos):
    from dash.exceptions import PreventUpdate
    state = dict(state or {"dragging": False})
    pos   = dict(pos or {"x": 120, "y": 80})

    if not ev:
        raise PreventUpdate

    etype = ev.get("type")  # e.g. "mousedown", "document:mousemove", "document:mouseup"

    # Start dragging only when mousedown on the handle
    if etype == "mousedown" and (ev.get("target.id") == "manage_drag_handle"):
        state["dragging"] = True
        state["dx"] = float(ev.get("clientX")) - float(pos.get("x", 0))
        state["dy"] = float(ev.get("clientY")) - float(pos.get("y", 0))
        return state, pos

    # Move while dragging (accept both local and document events)
    if etype in ("mousemove", "document:mousemove") and state.get("dragging"):
        mx = float(ev.get("clientX"))
        my = float(ev.get("clientY"))
        dx = float(state.get("dx", 0.0))
        dy = float(state.get("dy", 0.0))
        new_x = max(0.0, mx - dx)
        new_y = max(0.0, my - dy)
        return state, {"x": new_x, "y": new_y}

    # Stop dragging
    if etype in ("mouseup", "document:mouseup") and state.get("dragging"):
        state["dragging"] = False
        state.pop("dx", None); state.pop("dy", None)
        return state, pos

    raise PreventUpdate


@dash_app.callback(
    Output("manage_card", "style"),
    Input("modal_pos", "data")
)
def _place_modal(pos):
    # Base style mirrors what you set in layout_dashboard()
    base = {
        "position": "fixed",
        "background": "#fff",
        "borderRadius": "10px",
        "width": "92%",
        "maxWidth": "1100px",
        "maxHeight": "90vh",
        "overflowY": "auto",
        "boxShadow": "0 20px 50px rgba(0,0,0,.3)",
        "padding": "16px",
        "zIndex": 1000,
    }
    p = dict(pos or {"x": 120, "y": 80})
    base["left"] = f"{float(p.get('x', 120))}px"
    base["top"]  = f"{float(p.get('y', 80))}px"
    return base


#####


## Show/hide forms & selector depending on action/type

@dash_app.callback(
    Output("supply_form","style"),
    Output("opp_form","style"),
    Output("record_selector","style"),
    Input("entry_type","value"),
    Input("action_type","value"),
)
def toggle_forms(entry_type, action_type):
    # selector visible for update/delete
    sel_style = {"border":"1px solid #ddd","padding":"12px","borderRadius":"8px","marginBottom":"16px"}
    if action_type == "create":
        sel_style.update({"display":"none"})
    # show only the relevant form for create/update; none for delete
    if action_type == "delete":
        return {"display":"none"}, {"display":"none"}, sel_style
    if entry_type == "opportunity":
        return {"display":"none"}, {"display":"block"}, sel_style
    return {"display":"block"}, {"display":"none"}, sel_style

# Load records for Update/Delete dropdown
#@dash_app.callback(
#    Output("sel_record","options"),
#    Output("sel_help","children"),
#    Input("load_records","n_clicks"),
#    State("entry_type","value"),
#    State("flt_scn","value"),
#    State("flt_region","value"),
#    State("flt_yf","value"),
#    State("flt_yt","value"),
#    prevent_initial_call=True
#)
@dash_app.callback(
    Output("sel_record","options"),
    Output("sel_help","children"),
    Output("bulk_delete_table","data"),      # NEW
    Output("bulk_delete_table","columns"),   # NEW
    Input("load_records","n_clicks"),
    State("entry_type","value"),
    State("flt_scn","value"),
    State("flt_region","value"),
    State("flt_yf","value"),
    State("flt_yt","value"),
    State("flt_source","value"),          # Supply text filter (partial)
    State("flt_counterparty","value"),    # Opp text filter (partial)
    prevent_initial_call=True
)
def load_record_options(n_clicks, entry_type, scn, region, yf, yt, flt_source, flt_counterparty):
    params = {"year_from": yf, "year_to": yt}
    if region: params["region"] = region  # exact for categorical fields

    # SUPPLY
    if entry_type == "supply":
        if scn: params["scenario"] = scn
        rows = _fetch("/api/supply", params) or []

        # partial match on source (case-insensitive)
        if flt_source:
            needle = flt_source.strip().lower()
            rows = [r for r in rows if needle in (r.get("source") or "").lower()]

        # dropdown options
        opts = [{
            "label": f'{r["id"]} — {r.get("source","")} '
                     f'({r.get("scenario","")}/{r.get("region","")}/{r.get("year","")})',
            "value": r["id"]
        } for r in rows]

        # selection table
        cols = [
            {"name":"ID", "id":"id"},
            {"name":"Source", "id":"source"},
            {"name":"Scenario", "id":"scenario"},
            {"name":"Status", "id":"status"}, 
            {"name":"Region", "id":"region"},
            {"name":"Year", "id":"year"},
            {"name":"MTPA", "id":"mtpa"},
            {"name":"MMBTU", "id":"mmbtu"},
            {"name":"Cargoes", "id":"cargoes"},
        ]
        data = [{
            "id": r["id"],
            "source": r.get("source"),
            "scenario": r.get("scenario"),
            "status": r.get("status"),
            "region": r.get("region"),
            "year": r.get("year"),
            "mtpa": r.get("mtpa"),
            "mmbtu": r.get("mmbtu"),
            "cargoes": r.get("cargoes"),
        } for r in rows]

        return opts, f"Loaded {len(opts)} matching supply record(s).", data, cols

    # OPPORTUNITY
    rows = _fetch("/api/opportunities", params) or []
    if flt_counterparty:
        needle = flt_counterparty.strip().lower()
        rows = [r for r in rows if needle in (r.get("counterparty") or "").lower()]

    opts = [{
        "label": f'{r["id"]} — {r.get("contract_name","")} [{r.get("status","")}] '
                 f'({r.get("region","")}/{r.get("year","")})',
        "value": r["id"]
    } for r in rows]

    cols = [
        {"name":"ID", "id":"id"},
        {"name":"Contract", "id":"contract_name"},
        {"name":"Counterparty", "id":"counterparty"},
        {"name":"Status", "id":"status"},
        {"name":"Region", "id":"region"},
        {"name":"Year", "id":"year"},
        {"name":"MTPA", "id":"mtpa"},
        {"name":"MMBTU", "id":"mmbtu"},
        {"name":"Cargoes", "id":"cargoes"},
    ]
    data = [{
        "id": r["id"],
        "contract_name": r.get("contract_name"),
        "counterparty": r.get("counterparty"),
        "status": r.get("status"),
        "region": r.get("region"),
        "year": r.get("year"),
        "mtpa": r.get("mtpa"),
        "mmbtu": r.get("mmbtu"),
        "cargoes": r.get("cargoes"),
    } for r in rows]

    return opts, f"Loaded {len(opts)} matching opportunity record(s).", data, cols 

def _payload_from_unit(unit:str, value:float):
    if unit == "mmbtu":
        return {"mmbtu": value}
    elif unit == "cargoes":
        return {"cargoes": value}
    return {"mtpa": value}

# Submit handler now supports Create / Update / Delete

@dash_app.callback(
    Output("submit_msg","children"),
    Output("confirm_bulk_delete","displayed"),
    Output("confirm_bulk_delete","message"),
    Output("pending_bulk_delete","data"),
    Input("submit_btn","n_clicks"),
    State("entry_type","value"),
    State("action_type","value"),
    State("sel_record","value"),
    # Supply
    State("sup_source","value"),
    State("sup_source_type","value"),
    State("sup_region","value"),
    State("sup_scenario","value"),
    State("sup_unit","value"),
    State("sup_value","value"),
    State("sup_year","value"),
    State("sup_equity","value"),
    State("sup_start_month","value"),
    State("sup_months_active","value"),
    State("sup_ghv","value"),
    State("sup_notes","value"),
    State("sup_status","value"),
    # Opportunity
    State("opp_contract_name","value"),
    State("opp_counterparty","value"),
    State("opp_status","value"),
    State("opp_region","value"),
    State("opp_fob_des","value"),
    State("opp_pricing","value"),
    State("opp_unit","value"),
    State("opp_value","value"),
    State("opp_year","value"),
    State("opp_probability","value"),
    State("opp_notes","value"),
    # Filters for bulk + toggle
    State("flt_scn","value"),
    State("flt_region","value"),
    State("flt_yf","value"),
    State("flt_yt","value"),
    State("flt_source","value"),        # NEW
    State("flt_counterparty","value"),  # NEW
    State("bulk_update","value"),
    State("bulk_plan","data"),   # <-- IMPORTANT: pass the plan rows
    # === NEW: selection from the bulk delete table ===
    State("bulk_delete_table","selected_rows"),
    State("bulk_delete_table","derived_viewport_data"),
    State("delete_pw","value"),   # <-- add this State
    prevent_initial_call=True
)
def submit_manage(
    n_clicks, entry_type, action_type, sel_record,
    # Supply
    sup_source, sup_source_type, sup_region, sup_scenario, sup_unit, sup_value,
    sup_year, sup_equity, sup_start_month, sup_months_active, sup_ghv, sup_notes,
    sup_status,   # <-- add this
    # Opportunity
    opp_contract_name, opp_counterparty, opp_status, opp_region, opp_fob_des,
    opp_pricing, opp_unit, opp_value, opp_year, opp_probability, opp_notes,
    # Filters / bulk
    flt_scn, flt_region, flt_yf, flt_yt, flt_source, flt_counterparty,
    bulk_update, plan_rows,
    # Bulk delete table selection
    sel_rows, view_rows,
    # Password
    delete_pw,
):
    import requests
    base = "http://127.0.0.1:8000"
    is_bulk = "yes" in (bulk_update or [])
    plan_rows = plan_rows or []

    # --- helpers to ALWAYS return 4 outputs ---
    def _ok(msg):
        return msg, False, "", None
    def _ask(msg, data):
        return "", True, msg, data

    try:
        # ---------- DELETE single and bulk ----------
        if action_type == "delete":
            # BULK: prefer selected IDs; otherwise use filters + preview
            if is_bulk:
                # 1) Selected rows → staged per-ID delete
                selected_ids = []
                if (sel_rows and view_rows):
                    for i in sel_rows:
                        if 0 <= i < len(view_rows):
                            rid = (view_rows[i] or {}).get("id")
                            if rid is not None:
                                try:
                                    selected_ids.append(int(rid))
                                except Exception:
                                    pass

                if selected_ids:
                    msg = f"This will delete {len(selected_ids)} selected row(s). Proceed?"
                    pending = {"mode": "ids", "entry_type": entry_type, "ids": selected_ids}
                    if delete_pw:
                        pending["password"] = delete_pw   # <<< carry pw for IDs path
                    return _ask(msg, pending)

                # 2) No selection -> filter-based preview + staged bulk
                if not any([flt_source, flt_counterparty, flt_region, flt_scn, flt_yf, flt_yt]):
                    return _ok("For bulk delete, set at least one filter (Source/Counterparty, Region/Scenario, or Year range).")

                if entry_type == "supply":
                    params = {"year_from": flt_yf, "year_to": flt_yt}
                    if flt_region: params["region"] = flt_region
                    if flt_scn:    params["scenario"] = flt_scn
                    if flt_source: params["source"] = flt_source

                    r = requests.delete(f"{base}/api/supply/bulk", params={**params, "dry_run": True}, timeout=30)
                    if r.status_code >= 400:
                        return _ok(f"Error previewing delete: {r.text}")
                    cnt = (r.json() or {}).get("count", 0)
                    if cnt == 0:
                        return _ok("No matching supply rows to delete.")

                    msg = f"This will delete {cnt} supply row(s). Do you want to proceed?"
                    pending = {"mode":"params", "entry_type":"supply", "params": params, "count": cnt}
                    if delete_pw:
                        pending["params"]["delete_password"] = delete_pw  # also accepted by header later
                    return _ask(msg, pending)

                else:
                    params = {"year_from": flt_yf, "year_to": flt_yt}
                    if flt_region:       params["region"] = flt_region
                    if flt_counterparty: params["counterparty"] = flt_counterparty

                    r = requests.delete(f"{base}/api/opportunities/bulk", params={**params, "dry_run": True}, timeout=30)
                    if r.status_code >= 400:
                        return _ok(f"Error previewing delete: {r.text}")
                    cnt = (r.json() or {}).get("count", 0)
                    if cnt == 0:
                        return _ok("No matching opportunity rows to delete.")

                    msg = f"This will delete {cnt} opportunity row(s). Do you want to proceed?"
                    pending = {"mode":"params", "entry_type":"opportunity", "params": params, "count": cnt}
                    if delete_pw:
                        pending["params"]["delete_password"] = delete_pw
                    return _ask(msg, pending)

            # Single record delete
            if not sel_record:
                return _ok("Please click 'Load records' and select a record to delete.")
            url = f"{base}/api/supply/{sel_record}" if entry_type == "supply" else f"{base}/api/opportunities/{sel_record}"
            headers = {"X-Delete-Password": delete_pw} if delete_pw else None
            qparams = {"delete_password": delete_pw} if delete_pw else None
            r = requests.delete(url, timeout=10, params=qparams, headers=headers)
            return _ok("Deleted.") if r.status_code < 400 else _ok(f"Error: {r.text}")

        # ---------- SUPPLY ----------
        if entry_type == "supply":
            payload = {}
            if sup_source is not None:       payload["source"] = sup_source
            if sup_source_type is not None:  payload["source_type"] = sup_source_type
            if sup_region is not None:       payload["region"] = sup_region
            if sup_scenario is not None:     payload["scenario"] = sup_scenario
            if sup_year is not None:         payload["year"] = int(float(sup_year))
            if sup_value is not None and sup_unit:
                payload["unit"] = (sup_unit or "").lower()
                payload["value"] = float(sup_value)
                payload["original_unit"]  = payload["unit"]
                payload["original_value"] = float(sup_value)
            if sup_equity is not None:       payload["equity_fraction"] = float(sup_equity)
            if sup_start_month is not None:  payload["start_month"] = int(float(sup_start_month))
            if sup_months_active is not None:payload["months_active"] = int(float(sup_months_active))
            if sup_ghv is not None:          payload["ghv_mmbtu_per_tonne"] = float(sup_ghv)
            if sup_notes is not None:        payload["notes"] = sup_notes
            if sup_status is not None:       payload["status"] = sup_status

            if action_type == "create":
                if is_bulk:
                    clean_plan = []
                    for row in plan_rows:
                        try:
                            y = int(float(row.get("year"))); v = float(row.get("value"))
                            clean_plan.append({"year": y, "value": v})
                        except Exception:
                            pass
                    if not clean_plan:
                        return _ok("For bulk Supply create: fill at least one (Year, Value) row in the plan.")

                    bulk_payload = {
                        "source": sup_source,
                        "source_type": sup_source_type,
                        "region": sup_region,
                        "scenario": sup_scenario,
                        "unit": (sup_unit or "").lower(),
                        "equity_fraction": (float(sup_equity) if sup_equity is not None else None),
                        "start_month": (int(float(sup_start_month)) if sup_start_month not in (None,"") else None),
                        "months_active": (int(float(sup_months_active)) if sup_months_active not in (None,"") else None),
                        "ghv_mmbtu_per_tonne": (float(sup_ghv) if sup_ghv not in (None,"") else None),
                        "notes": sup_notes or "",
                        "status": (sup_status or "negotiation"),
                        "plan": clean_plan,
                    }
                    if not (sup_source and sup_region is not None and sup_scenario and sup_unit):
                        return _ok("For bulk Supply create: fill Source, Region, Scenario, Unit, then the plan grid.")
                    #r = requests.post(f"{base}/api/supply/bulk", json=bulk_payload, timeout=30)
                    headers = {"X-Delete-Password": delete_pw} if delete_pw else None
                    qparams = {"delete_password": delete_pw} if delete_pw else None
                    r = requests.post(
                        f"{base}/api/supply/bulk",
                        json=bulk_payload,
                        params=qparams,
                        headers=headers,
                        timeout=30
                    )
                    if r.status_code < 400:
                        j = r.json() or {}
                        return _ok(f"Bulk-created {j.get('created',0)} supply row(s); skipped existing: {j.get('skipped_existing',0)}.")
                    return _ok(f"Error: {r.text}")

                # single create
                if not (sup_source and sup_region and sup_scenario and sup_unit and sup_value and sup_year):
                    return _ok("For Supply create: fill Source, Region, Scenario, Unit, Value, Year.")
                #r = requests.post(f"{base}/api/supply", json=payload, timeout=12)
                headers = {"X-Delete-Password": delete_pw} if delete_pw else None
                qparams = {"delete_password": delete_pw} if delete_pw else None
                r = requests.post(
                    f"{base}/api/supply",
                    json=payload,
                    params=qparams,
                    headers=headers,
                    timeout=12
                )
                return _ok("Supply created.") if r.status_code < 400 else _ok(f"Error: {r.text}")

            # ---- UPDATE ----
            if is_bulk:
                params = {"year_from": flt_yf, "year_to": flt_yt}
                if flt_region: params["region"]   = flt_region
                if flt_scn:    params["scenario"] = flt_scn
                #if flt_source: params["source"]   = (flt_source or "").strip()   # ← ADD THIS

                clean_plan = []
                for row in plan_rows:
                    try:
                        y = int(float(row.get("year"))); v = float(row.get("value"))
                        clean_plan.append({"year": y, "value": v})
                    except Exception:
                        pass
                if clean_plan:
                    payload["plan"] = clean_plan
                    if sup_unit:
                        payload["unit"] = (sup_unit or "").lower()

                # don't change identity keys in bulk
                for k in ["source","source_type","region","scenario","year"]:
                    payload.pop(k, None)

                #r = requests.patch(f"{base}/api/supply/bulk", params=params, json=payload, timeout=30)
                # include password if provided
                if delete_pw:
                    params = {**params, "delete_password": delete_pw}
                headers = {"X-Delete-Password": delete_pw} if delete_pw else None
                r = requests.patch(
                    f"{base}/api/supply/bulk",
                    params=params,
                    json=payload,
                    headers=headers,
                    timeout=30
                )    
                if r.status_code < 400:
                    n = (r.json() or {}).get("updated", 0)
                    return _ok(f"Bulk-updated {n} supply row(s).")
                return _ok(f"Error: {r.text}")
            else:
                if not sel_record:
                    return _ok("Please select a Supply record to update (or enable bulk update).")
                #r = requests.patch(f"{base}/api/supply/{sel_record}", json=payload, timeout=12)
                headers = {"X-Delete-Password": delete_pw} if delete_pw else None
                qparams = {"delete_password": delete_pw} if delete_pw else None
                r = requests.patch(
                    f"{base}/api/supply/{sel_record}",
                    json=payload,
                    params=qparams,
                    headers=headers,
                    timeout=12
                )
                return _ok("Supply updated.") if r.status_code < 400 else _ok(f"Error: {r.text}")

        # ---------- OPPORTUNITY ----------
        else:
            payload = {}
            if opp_contract_name is not None: payload["contract_name"] = opp_contract_name
            if opp_counterparty is not None:  payload["counterparty"] = opp_counterparty
            if opp_status is not None:        payload["status"] = opp_status
            if opp_region is not None:        payload["region"] = opp_region
            if opp_fob_des is not None:       payload["fob_des"] = (opp_fob_des or "FOB")
            if opp_pricing is not None:       payload["pricing_index"] = opp_pricing
            if opp_year is not None:          payload["year"] = int(float(opp_year))
            if opp_value is not None and opp_unit:
                payload["unit"] = (opp_unit or "").lower()
                payload["value"] = float(opp_value)
                payload["original_unit"]  = payload["unit"]
                payload["original_value"] = float(opp_value)
            if opp_probability is not None:   payload["probability"] = float(opp_probability)
            if opp_notes is not None:         payload["notes"] = opp_notes

            if action_type == "create":
                if is_bulk:
                    clean_plan = []
                    for row in plan_rows:
                        try:
                            y = int(float(row.get("year"))); v = float(row.get("value"))
                            clean_plan.append({"year": y, "value": v})
                        except Exception:
                            pass
                    if not clean_plan:
                        return _ok("For bulk Opportunity create: fill at least one (Year, Value) row in the plan.")

                    bulk_payload = {
                        "contract_name": opp_contract_name,
                        "counterparty": opp_counterparty,
                        "status": opp_status,
                        "region": opp_region,
                        "fob_des": (opp_fob_des or "FOB"),
                        "pricing_index": opp_pricing or "",
                        "probability": (float(opp_probability) if opp_probability not in (None,"") else None),
                        "unit": (opp_unit or "").lower(),
                        "notes": opp_notes or "",
                        "plan": clean_plan,
                    }
                    if not (opp_contract_name and opp_counterparty and opp_status and opp_region and opp_unit):
                        return _ok("For bulk Opportunity create: fill Contract Name, Counterparty, Status, Region, Unit, then the plan grid.")
                    r = requests.post(f"{base}/api/opportunities/bulk", json=bulk_payload, timeout=30)
                    if r.status_code < 400:
                        j = r.json() or {}
                        return _ok(f"Bulk-created {j.get('created',0)} opportunity row(s); skipped existing: {j.get('skipped_existing',0)}.")
                    return _ok(f"Error: {r.text}")

                # single create
                if not (opp_contract_name and opp_counterparty and opp_status and opp_region and opp_unit and opp_value and opp_year):
                    return _ok("For Opportunity create: fill Contract Name, Counterparty, Status, Region, Unit, Value, Year.")
                r = requests.post(f"{base}/api/opportunities", json=payload, timeout=12)
                return _ok("Opportunity created.") if r.status_code < 400 else _ok(f"Error: {r.text}")

            # ---- UPDATE ----
            if is_bulk:
                params = {"year_from": flt_yf, "year_to": flt_yt}
                if flt_region: params["region"] = flt_region

                clean_plan = []
                for row in plan_rows:
                    try:
                        y = int(float(row.get("year"))); v = float(row.get("value"))
                        clean_plan.append({"year": y, "value": v})
                    except Exception:
                        pass
                if clean_plan:
                    payload["plan"] = clean_plan
                    if opp_unit:
                        payload["unit"] = (opp_unit or "").lower()

                # don't change identity keys in bulk
                for k in ["contract_name", "counterparty", "region", "year"]:
                    payload.pop(k, None)

                #r = requests.patch(f"{base}/api/opportunities/bulk", params=params, json=payload, timeout=30)
                if delete_pw:
                    params = {**params, "delete_password": delete_pw}
                headers = {"X-Delete-Password": delete_pw} if delete_pw else None
                r = requests.patch(
                    f"{base}/api/opportunities/bulk",
                    params=params,
                    json=payload,
                    headers=headers,
                    timeout=30
                )    
                if r.status_code < 400:
                    n = (r.json() or {}).get("updated", 0)
                    return _ok(f"Bulk-updated {n} opportunity row(s).")
                return _ok(f"Error: {r.text}")
            else:
                if not sel_record:
                    return _ok("Please select an Opportunity record to update (or enable bulk update).")
                #r = requests.patch(f"{base}/api/opportunities/{sel_record}", json=payload, timeout=12)
                headers = {"X-Delete-Password": delete_pw} if delete_pw else None
                qparams = {"delete_password": delete_pw} if delete_pw else None
                r = requests.patch(
                    f"{base}/api/opportunities/{sel_record}",
                    json=payload,
                    params=qparams,
                    headers=headers,
                    timeout=12
                )
                return _ok("Opportunity updated.") if r.status_code < 400 else _ok(f"Error: {r.text}")

    except Exception as e:
        return _ok(f"Error: {e}")



# OK (Confirm)
@dash_app.callback(
    Output("submit_msg","children", allow_duplicate=True),
    Output("confirm_bulk_delete","displayed", allow_duplicate=True),
    Output("pending_bulk_delete","data", allow_duplicate=True),
    Input("confirm_bulk_delete","submit_n_clicks"),
    State("pending_bulk_delete","data"),
    prevent_initial_call=True
)
def execute_confirmed_bulk_delete(n_ok, pending):
    from dash.exceptions import PreventUpdate
    if not n_ok or not pending:
        raise PreventUpdate

    import requests
    base = "http://127.0.0.1:8000"

    # Selected-IDs mode
    if pending.get("mode") == "ids":
        ids   = pending.get("ids") or []
        etype = pending.get("entry_type")
        pw    = pending.get("password")
        headers = {"X-Delete-Password": pw} if pw else None
        params  = {"delete_password": pw}  if pw else None

        ok = 0; bad = 0; errs = []
        for rid in ids:
            url = f"{base}/api/supply/{rid}" if etype == "supply" \
                  else f"{base}/api/opportunities/{rid}"
            r = requests.delete(url, params=params, headers=headers, timeout=15)
            if r.status_code < 400:
                ok += 1
            else:
                bad += 1
                try: errs.append(r.json().get("detail", r.text))
                except Exception: errs.append(r.text)
        msg = f"Deleted {ok}/{len(ids)} row(s)." + (f" Errors: {bad} — {'; '.join(errs[:3])}" if bad else "")
        return msg, False, None

    # ALWAYS build params first
    params = dict(pending.get("params") or {})
    params["confirm_count"] = pending.get("count", 0)

    # Safe default headers; include password if you staged it in params
    headers = {}
    pw = params.get("delete_password")
    if pw:
        headers["X-Delete-Password"] = pw  # backend also accepts header

    if pending.get("entry_type") == "supply":
        r = requests.delete(f"{base}/api/supply/bulk", params=params, headers=(headers or None), timeout=30)
    else:
        r = requests.delete(f"{base}/api/opportunities/bulk", params=params, headers=(headers or None), timeout=30)

    if r.status_code < 400:
        deleted = (r.json() or {}).get("deleted", params["confirm_count"])
        return (f"Deleted {deleted} row(s).", False, None)

    # surface server message if it still failed
    try:
        msg = r.json().get("detail", r.text)
    except Exception:
        msg = r.text
    return (f"Error: {msg}", False, None)


# Cancel — close dialog and clear the staged delete
@dash_app.callback(
    Output("confirm_bulk_delete","displayed", allow_duplicate=True),
    Output("pending_bulk_delete","data", allow_duplicate=True),
    Input("confirm_bulk_delete","cancel_n_clicks"),
    prevent_initial_call=True
)
def cancel_bulk_delete(_):
    return False, None

# Callbacks to drive the Advanced sell-down plan table    

@dash_app.callback(
    Output("sd_table", "dropdown"),
    Output("sd_table", "data"),
    Input("sell_source", "options"),
    Input("sd_add_row", "n_clicks"),
    Input("sd_clear", "n_clicks"),
    State("sd_table", "data"),
    prevent_initial_call=True
)
def manage_sd_table(source_opts, add_clicks, clear_clicks, rows):
    import dash
    rows = rows or []
    dropdown = {"source": {"options": source_opts or []}}

    ctx = dash.callback_context
    if not ctx.triggered:
        return dropdown, rows

    trig = ctx.triggered[0]["prop_id"].split(".")[0]
    if trig == "sd_add_row":
        rows = rows + [{"source": None, "year": 2026, "pct": 0.0}]
    elif trig == "sd_clear":
        rows = []
    return dropdown, rows


# Normalize/validate plan rows (allow negative %)
@dash_app.callback(
    Output("sd_rules","data"),
    Input("sd_table","data")
)
def normalize_rules(rows):
    out = []
    for r in rows or []:
        src = (r.get("source") or "").strip()
        try:
            year = int(float(r.get("year")))
            pct  = float(r.get("pct"))  # can be negative
        except Exception:
            continue
        if not src or pct == 0:
            continue
        out.append({"source": src, "year": year, "pct": pct})
    return out


@dash_app.callback(
    Output("sell_source", "options"),
    Input("scenario", "value"),
    Input("region", "value"),
    Input("year_from", "value"),
    Input("year_to", "value"),
)
def populate_sell_sources(scenario, region, yf, yt):
    region_param = None if (region is None or str(region).strip().lower()=="global") else region
    rows = _fetch("/api/supply", {
        "region": region_param,
        "scenario": (None if scenario=="Both" else scenario),
        "year_from": yf, "year_to": yt
    }) or []
    names = sorted({r.get("source") for r in rows if r.get("source")})
    return [{"label": n, "value": n} for n in names]


# Prefill forms
@dash_app.callback(
    [
        Output("sup_source", "value"),
        Output("sup_source_type", "value"),
        Output("sup_region", "value"),
        Output("sup_scenario", "value"),
        Output("sup_status","value"),
        Output("sup_unit", "value"),
        Output("sup_value", "value"),
        Output("sup_year", "value"),
        Output("sup_equity", "value"),
        Output("sup_start_month", "value"),
        Output("sup_months_active", "value"),
        Output("sup_ghv", "value"),
        Output("sup_notes", "value"),
        Output("opp_contract_name", "value"),
        Output("opp_counterparty", "value"),
        Output("opp_status", "value"),
        Output("opp_region", "value"),
        Output("opp_fob_des", "value"),
        Output("opp_pricing", "value"),
        Output("opp_unit", "value"),
        Output("opp_value", "value"),
        Output("opp_year", "value"),
        Output("opp_probability", "value"),
        Output("opp_notes", "value"),
    ],
    [
        Input("entry_type", "value"),
        Input("action_type", "value"),
        Input("sel_record", "value"),
    ],
    prevent_initial_call=True,
)
def prefill_forms(entry_type, action_type, sel_record):
    import requests
    base = "http://127.0.0.1:8000"
    CLEAR = [None] * 24
    CLEAR[4] = "negotiation"  # index 4 corresponds to sup_status in the new order

    if action_type == "create" or not sel_record:
        return CLEAR

    try:
        if entry_type == "supply":
            r = requests.get(f"{base}/api/supply/{sel_record}", timeout=5)
            if r.status_code >= 400:
                return CLEAR
            row = r.json() or {}

            unit = (row.get("original_unit") or "").lower()
            val = row.get("original_value")
            if not unit or val in (None, ""):
                if (row.get("mtpa") or 0) > 0:
                    unit, val = "mtpa", float(row.get("mtpa") or 0)
                elif (row.get("mmbtu") or 0) > 0:
                    unit, val = "mmbtu", float(row.get("mmbtu") or 0)
                elif (row.get("cargoes") or 0) > 0:
                    unit, val = "cargoes", float(row.get("cargoes") or 0)
                else:
                    unit, val = "mtpa", 0.0

            return [
                row.get("source"),
                row.get("source_type"),
                row.get("region"),
                row.get("scenario"),
                (row.get("status") or "negotiation"),
                unit,
                val,
                row.get("year"),
                row.get("equity_fraction"),
                row.get("start_month"),
                row.get("months_active"),
                row.get("ghv_mmbtu_per_tonne"),
                row.get("notes"),
                None, None, None, None, None, None, None, None, None, None, None,
            ]

        else:
            r = requests.get(f"{base}/api/opportunities/{sel_record}", timeout=5)
            if r.status_code >= 400:
                return CLEAR
            row = r.json() or {}

            unit = (row.get("original_unit") or "").lower()
            val = row.get("original_value")
            if not unit or val in (None, ""):
                if (row.get("mtpa") or 0) > 0:
                    unit, val = "mtpa", float(row.get("mtpa") or 0)
                elif (row.get("mmbtu") or 0) > 0:
                    unit, val = "mmbtu", float(row.get("mmbtu") or 0)
                elif (row.get("cargoes") or 0) > 0:
                    unit, val = "cargoes", float(row.get("cargoes") or 0)
                else:
                    unit, val = "mtpa", 0.0

            return [
                None, None, None, None, None, None, None, None, None, None, None, None,
                row.get("contract_name"),
                row.get("counterparty"),
                row.get("status"),
                row.get("region"),
                row.get("fob_des"),
                row.get("pricing_index"),
                unit,
                val,
                row.get("year"),
                row.get("probability"),
                row.get("notes"),
            ]
    except Exception:
        return CLEAR

@dash_app.callback(
    Output("focus_source", "data"),
    Input("tbl_supply", "active_cell"),
    Input("tbl_supply", "selected_rows"),
    Input("clear_source_focus", "n_clicks"),
    State("tbl_supply", "derived_viewport_data"),   # ← use viewport data
    prevent_initial_call=True
)
def handle_focus_source(active_cell, selected_rows, n_clear, view_rows):
    import dash
    if not dash.callback_context.triggered:
        raise PreventUpdate
    trig = dash.callback_context.triggered[0]["prop_id"].split(".")[0]

    if trig == "clear_source_focus":
        return None

    rows = view_rows or []
    # Prefer explicit row selection if present
    if selected_rows:
        rix = selected_rows[0]
    elif active_cell and active_cell.get("row") is not None:
        rix = active_cell["row"]
    else:
        raise PreventUpdate

    if rix < 0 or rix >= len(rows):
        raise PreventUpdate

    src = (rows[rix] or {}).get("source")
    if not src:
        raise PreventUpdate
    return {"source": src}


@dash_app.callback(
    Output("source_focus_panel", "style"),
    Output("source_focus_chart", "figure"),
    Output("source_focus_title", "children"),
    Input("focus_source", "data"),
    Input("scenario", "value"),
    Input("region", "value"),
    Input("unit", "value"),
    Input("basis", "value"),
    Input("granularity", "value"),
    Input("year_from", "value"),    # ← was State
    Input("year_to", "value"),      # ← was State
)
def render_source_focus(focus, scenario, region, unit, basis, granularity, yf, yt):
    import plotly.graph_objects as go
    from collections import defaultdict

    if not focus or not focus.get("source"):
        return {"display": "none"}, go.Figure(), ""

    source = focus["source"]
    is_yearly = str(granularity).lower() == "yearly"
    region_param = None if (region is None or str(region).strip().lower() == "global") else region

    def sum_by(rows, yearly: bool):
        acc = defaultdict(float)
        for r in rows or []:
            key = r["date"][:4] if yearly else r["date"]
            acc[key] += float(r.get("value") or 0.0)
        xs = sorted(acc.keys())
        ys = [acc[x] for x in xs]
        return xs, ys

    def fetch_series(scn: str):
        return _fetch("/api/series/supply_monthly", {
            "unit": unit,
            "scenario": scn,
            "source": source,
            "region": region_param,
            "year_from": yf,
            "year_to": yt,
            "basis": basis
        }) or []

    fig = go.Figure()
    mode = "lines+markers" if is_yearly else "lines"

    if scenario == "Both":
        s_p50 = fetch_series("P50")
        s_p90 = fetch_series("P90")
        x50, y50 = sum_by(s_p50, is_yearly)
        x90, y90 = sum_by(s_p90, is_yearly)
        #fig.add_trace(go.Scatter(x=x50, y=y50, mode=mode, name="P50"))
        #fig.add_trace(go.Scatter(x=x90, y=y90, mode=mode, name="P90"))
        fig.add_trace(go.Scatter(x=x50, y=y50, mode=mode, name="P50", line=dict(color=COL["p50"], width=2)))
        fig.add_trace(go.Scatter(x=x90, y=y90, mode=mode, name="P90", line=dict(color=COL["p90"], width=2)))
        title = f"{source} — {'annual' if is_yearly else 'monthly'} [{unit.upper()}] • {basis} • P50 vs P90"
    else:
        scn = scenario or "P50"
        series = fetch_series(scn)
        xs, ys = sum_by(series, is_yearly)
        #fig.add_trace(go.Scatter(x=xs, y=ys, mode=mode, name=f"{scn}"))
        fig.add_trace(go.Scatter(x=xs, y=ys, mode=mode, name=f"{scn}", line=dict(color=(COL["p50"] if scn=="P50" else COL["p90"]), width=2)))
        title = f"{source} — {'annual' if is_yearly else 'monthly'} [{unit.upper()}] • {basis} • {scn}"

    fig.update_layout(
        title=title,
        xaxis_title=("Year" if is_yearly else "Month"),
        yaxis_title=unit.upper(),
        showlegend=True,
        legend=dict(
            orientation="v",
            x=1.02, xanchor="left",
            y=1.0, yanchor="top"
        ),
        margin=dict(r=220)
    )

    return {"display": "block"}, fig, source


@dash_app.callback(
    Output("focus_counterparty", "data"),
    Input("tbl_opp", "active_cell"),
    Input("tbl_opp", "selected_rows"),
    Input("clear_counterparty_focus", "n_clicks"),
    State("tbl_opp", "derived_viewport_data"),
    prevent_initial_call=True
)
def handle_focus_counterparty(active_cell, selected_rows, n_clear, view_rows):
    import dash
    from dash.exceptions import PreventUpdate
    if not dash.callback_context.triggered:
        raise PreventUpdate
    trig = dash.callback_context.triggered[0]["prop_id"].split(".")[0]

    if trig == "clear_counterparty_focus":
        return None

    rows = view_rows or []
    if selected_rows:
        rix = selected_rows[0]
    elif active_cell and active_cell.get("row") is not None:
        rix = active_cell["row"]
    else:
        raise PreventUpdate

    if rix < 0 or rix >= len(rows):
        raise PreventUpdate

    cp = (rows[rix] or {}).get("counterparty")
    if not cp:
        raise PreventUpdate

    # include contract_name for future display if you want
    contract = (rows[rix] or {}).get("contract_name")
    return {"counterparty": cp, "contract_name": contract}

@dash_app.callback(
    Output("counterparty_focus_panel", "style"),
    Output("counterparty_focus_chart", "figure"),
    Output("counterparty_focus_title", "children"),
    Input("focus_counterparty", "data"),
    Input("region", "value"),
    Input("unit", "value"),
    Input("granularity", "value"),
    Input("year_from", "value"),
    Input("year_to", "value"),
    Input("prob_w", "value"),
)
def render_counterparty_focus(focus, region, unit, granularity, yf, yt, probw):
    import plotly.graph_objects as go
    from collections import defaultdict

    if not focus or not focus.get("counterparty"):
        return {"display": "none"}, go.Figure(), ""

    counterparty = focus["counterparty"]
    is_yearly = str(granularity).lower() == "yearly"
    region_param = None if (region is None or str(region).strip().lower() == "global") else region
    probability_weighted = "yes" in (probw or [])

    def sum_by(rows, yearly):
        acc = defaultdict(float)
        for r in rows or []:
            key = r["date"][:4] if yearly else r["date"]
            acc[key] += float(r.get("value") or 0.0)
        xs = sorted(acc.keys())
        ys = [acc[x] for x in xs]
        return xs, ys

    def fetch_series(status):
        return _fetch("/api/series/opportunity_monthly", {
            "unit": unit,
            "status": status,
            "counterparty": counterparty,
            "region": region_param,
            "year_from": yf,
            "year_to": yt,
            "probability_weighted": probability_weighted
        }) or []

    firm = fetch_series("firm")
    nego = fetch_series("negotiation")
    optn = fetch_series("option")

    xs_f, ys_f = sum_by(firm, is_yearly)
    xs_n, ys_n = sum_by(nego, is_yearly)
    xs_o, ys_o = sum_by(optn, is_yearly)

    # total line
    total_map = defaultdict(float)
    for xs, ys in ((xs_f, ys_f), (xs_n, ys_n), (xs_o, ys_o)):
        for x, y in zip(xs, ys):
            total_map[x] += y
    xs_t = sorted(total_map.keys())
    ys_t = [total_map[x] for x in xs_t]

    mode = "lines+markers" if is_yearly else "lines"
    fig = go.Figure()
    #fig.add_trace(go.Scatter(x=xs_f, y=ys_f, mode=mode, name="Firm"))
    #fig.add_trace(go.Scatter(x=xs_n, y=ys_n, mode=mode, name=("Negotiation (weighted)" if probability_weighted else "Negotiation")))
    #fig.add_trace(go.Scatter(x=xs_o, y=ys_o, mode=mode, name=("Option (weighted)" if probability_weighted else "Option")))
    #fig.add_trace(go.Scatter(x=xs_t, y=ys_t, mode=mode, name="Total", line=dict(width=3)))

    fig.add_trace(go.Scatter(x=xs_f, y=ys_f, mode=mode, name="Firm", line=dict(color=COL["firm"], width=2)))
    fig.add_trace(go.Scatter(x=xs_n, y=ys_n, mode=mode, name=("Negotiation (weighted)" if probability_weighted else "Negotiation"), line=dict(color=COL["nego"], width=2, dash="dash")))
    fig.add_trace(go.Scatter(x=xs_o, y=ys_o, mode=mode, name=("Option (weighted)" if probability_weighted else "Option"), line=dict(color=COL["option"], width=2, dash="dot")))
    fig.add_trace(go.Scatter(x=xs_t, y=ys_t, mode=mode, name="Total", line=dict(color=COL["total"], width=3)))

    fig.update_layout(
        title=f"{counterparty} — {'annual' if is_yearly else 'monthly'} opportunities [{unit.upper()}]",
        xaxis_title=("Year" if is_yearly else "Month"),
        yaxis_title=unit.upper(),
        showlegend=True,
        legend=dict(orientation="v", x=1.02, xanchor="left", y=1.0, yanchor="top"),
        margin=dict(r=220)
    )

    return {"display": "block"}, fig, counterparty

# Muted, earthy palette (Woodside-ish) + your #5459AC.
# ~28 distinct colors to avoid repeats without going rainbow.
UNIQUE_PALETTE = [
    "#5459AC", "#8B0000", "#D32F2F", "#A33D2E", "#C56A31", "#6B4F2A", "#9C8061",
    "#2E7D32", "#4A7B5A", "#00695C", "#327A7A", "#455A64", "#37474F", "#5B6B73",
    "#6E7C7C", "#7F8A7A", "#8C6D5A", "#A0867A", "#7C5C55", "#6B6E8F", "#545F74",
    "#6A7286", "#7A86A5", "#8D9BB6", "#9EAFBF", "#7B5F3F", "#5E6A5A", "#6A5E5A",
]

def build_unique_color_map(names, fixed=None):
    """
    Return {name -> unique color}. 'fixed' (dict) is applied first.
    Remaining names get colors from UNIQUE_PALETTE in order, no reuse until exhausted.
    If there are more names than palette colors, we create subtle tints to stay non-rainbow.
    """
    fixed = fixed or {}
    cmap = dict(fixed)
    pool = [c for c in UNIQUE_PALETTE if c.lower() not in {v.lower() for v in fixed.values()}]

    # simple hex tint (toward white) to extend palette if needed
    def _tint(hexcol, t=0.18):
        hexcol = hexcol.lstrip("#")
        r = int(hexcol[0:2], 16); g = int(hexcol[2:4], 16); b = int(hexcol[4:6], 16)
        r = int(min(255, r + (255 - r) * t))
        g = int(min(255, g + (255 - g) * t))
        b = int(min(255, b + (255 - b) * t))
        return f"#{r:02X}{g:02X}{b:02X}"

    # assign base colors first
    i = 0
    for n in names:
        if n in cmap:
            continue
        if i < len(pool):
            cmap[n] = pool[i]; i += 1
        else:
            # extend: tint the palette cyclically
            base = UNIQUE_PALETTE[(i - len(pool)) % len(UNIQUE_PALETTE)]
            # slight increasing tint so they remain distinct but not rainbow
            tint_amount = 0.18 + 0.08 * ((i - len(pool)) // len(UNIQUE_PALETTE))
            cmap[n] = _tint(base, tint_amount)
            i += 1
    return cmap


# --- Light grey theme helper ---
def apply_grey_theme(fig):
    # Light greys (tweak to taste)
    paper = "#F3F4F6"   # whole card/page behind the plotting area
    plot  = "#F9FAFB"   # plotting area
    grid  = "#E5E7EB"   # gridlines (very light grey)

    fig.update_layout(
        paper_bgcolor=paper,
        plot_bgcolor=plot,
        legend=dict(bgcolor="rgba(255,255,255,0.65)"),
        margin=dict(l=60, r=220, t=70, b=60)  # keep your roomy right margin
    )
    # Turn on light gridlines across all axes (works for subplots too)
    fig.update_xaxes(showgrid=True, gridcolor=grid, zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor=grid, zeroline=False)
    return fig


@dash_app.callback(
    [
        Output("contracted_neg_chart", "figure"),        # <-- now first
        Output("sold_uncommitted_pct_chart", "figure"),  # <-- now second
        Output("supply_chart","figure"),
        Output("supply_chart_area","figure"),  
        Output("sold_uncommitted_chart","figure"),    
        Output("sold_uncommitted_abs_chart","figure"),     
        #Output("sold_uncommitted_pct_chart","figure"),  
        Output("opportunity_chart","figure"),
        #Output("contracted_neg_chart","figure"),
        Output("tbl_supply","data"),
        Output("tbl_supply","columns"),
        Output("tbl_opp","data"),
        Output("tbl_opp","columns"),
    ],
    [
        Input("scenario","value"),
        Input("region","value"),
        Input("unit","value"),
        Input("basis","value"),
        Input("year_from","value"),
        Input("year_to","value"),
        Input("prob_w","value"),
        Input("granularity","value"),
        Input("sell_source","value"),
        Input("sell_pct","value"),
        Input("sd_rules","data"),
    ],
)
def refresh_all(scenario, region, unit, basis, yf, yt, probw, granularity,
                sell_source, sell_pct, sd_rules):
    from collections import defaultdict
    import math, logging

    # put this near the top of refresh_all, before you start building charts
    sold: list[float] = []
    uncomm: list[float] = []
    xs_su: list[str] = []  # your x-axis for the SU charts

    probability_weighted = "yes" in (probw or [])
    region_param = None if (region is None or str(region).strip().lower()=="global") else region
    is_yearly = (str(granularity).lower() == "yearly")

    # Which scenario the overlay should follow for NON-supply charts (P90 when Both)
    scenario_value = (scenario or "P50")
    sell_scn = "P90" if scenario_value in ("P90", "Both") else "P50"

    # ---- helpers ----
    def sum_by_month(rows):
        acc = defaultdict(float)
        for r in rows or []:
            acc[r["date"]] += float(r.get("value") or 0.0)  # YYYY-MM-01
        xs = sorted(acc.keys()); ys = [acc[x] for x in xs]
        return xs, ys

    def sum_by_year(rows):
        acc = defaultdict(float)
        for r in rows or []:
            y = r["date"][:4]
            acc[y] += float(r.get("value") or 0.0)
        xs = sorted(acc.keys()); ys = [acc[x] for x in xs]
        return xs, ys

    def sum_series(rows, yearly=False):
        return sum_by_year(rows) if yearly else sum_by_month(rows)

    def series_to_dict(rows, yearly=False):
        acc = defaultdict(float)
        for r in rows or []:
            key = r["date"][:4] if yearly else r["date"]
            acc[key] += float(r.get("value") or 0.0)
        return acc

    def fetch_supply_series(scn, src=None):
        return _fetch("/api/series/supply_monthly", {
            "unit": unit, "scenario": scn, "source": src, "region": region_param,
            "year_from": yf, "year_to": yt, "basis": basis
        })

    # ============================
    # Build unified overlay deltas (simple + advanced) for NON-supply charts
    # ============================
    # ============================
    # Advanced sell-down helpers (used by Supply + last chart)
    # ============================
    def _pct_fraction(val) -> float:
        """Convert UI value like -10 or '-10 %' into a fraction -0.10 (clamped [-1,1])."""
        if val is None:
            return 0.0
        try:
            s = str(val).strip()
            if s.endswith("%"): s = s[:-1]
            f = float(s)
            f = max(-100.0, min(100.0, f))
            return f / 100.0
        except Exception:
            return 0.0

    simple_pct = _pct_fraction(sell_pct)  # e.g. -0.10 for -10%

    # Parse advanced rules coming from the grid
    rules = sd_rules or []
    from collections import defaultdict as _dd
    rules_by_source = _dd(list)
    for rr in rules:
        try:
            src = (rr.get("source") or "").strip()
            yr  = int(rr.get("year"))
            pct = float(rr.get("pct")) / 100.0  # store as fraction once
        except Exception:
            continue
        if not src or pct == 0:
            continue
        rules_by_source[src].append({"year": yr, "pct": pct})

    has_simple       = bool(sell_source and simple_pct != 0)
    has_advanced     = bool(rules_by_source)
    has_overlay_plan = has_simple or has_advanced

    # ============================
    # Build overlay deltas *per scenario* (for the Contracted+Negotiations chart)
    # Values are what to SUBTRACT from supply for that scenario.
    # ============================
    from collections import defaultdict as _dd

    def build_overlay_delta_for_scn(scn: str):
        dm, dy = _dd(float), _dd(float)

        # SIMPLE: uniform % on a single source
        if sell_source and simple_pct != 0:
            ser = _fetch("/api/series/supply_monthly", {
                "unit": unit, "scenario": scn, "source": sell_source,
                "region": region_param, "year_from": yf, "year_to": yt, "basis": basis
            }) or []
            for r in ser:
                v = float(r.get("value") or 0.0)
                ym = r["date"]; yy = ym[:4]
                dm[ym] += simple_pct * v
                dy[yy] += simple_pct * v

        # ADVANCED: per-source per-year %
        for src, rr_list in (rules_by_source or {}).items():
            ser = _fetch("/api/series/supply_monthly", {
                "unit": unit, "scenario": scn, "source": src,
                "region": region_param, "year_from": yf, "year_to": yt, "basis": basis
            }) or []
            pct_by_year = _dd(float)
            for rr in rr_list:
                try:
                    pct_by_year[int(rr["year"])] += float(rr["pct"])  # already a fraction
                except Exception:
                    pass
            for r in ser:
                ym = r["date"]; yy = int(ym[:4])
                p = pct_by_year.get(yy, 0.0)
                if not p:
                    continue
                v = float(r.get("value") or 0.0)
                #dm[ym]      += p * v
                #dy[str(yy)] += p * v
                dm[ym]      += (p) * v
                dy[str(yy)] += (p) * v

        return dm, dy

    overlay_m_p50, overlay_y_p50 = build_overlay_delta_for_scn("P50")
    overlay_m_p90, overlay_y_p90 = build_overlay_delta_for_scn("P90")

    # ============================
    #  Supply chart(s) — apply SIMPLE and/or ADVANCED per scenario (supports negatives)
    #  NOTE: compute per-scenario plan deltas directly (do NOT reuse sell_scn),
    #  then do: after = base - delta
    # ============================
    def _plan_delta_for_scenario(scn: str, yearly: bool) -> defaultdict:
        """Return map key->delta_to_subtract for this scenario (simple + advanced)."""
        delta = defaultdict(float)

        # simple: only on the chosen sell_source
        if has_simple and sell_source:
            for r in (fetch_supply_series(scn, src=sell_source) or []):
                k = r["date"][:4] if yearly else r["date"]
                delta[k] += simple_pct * float(r.get("value") or 0.0)

        # advanced: per source & year
        for src, rr_list in rules_by_source.items():
            pct_by_year = defaultdict(float)
            for rr in rr_list:
                pct_by_year[int(rr["year"])] += float(rr["pct"])  # already fraction
            for r in (fetch_supply_series(scn, src=src) or []):
                yy = int(r["date"][:4])
                pct = pct_by_year.get(yy, 0.0)
                if pct == 0: continue
                k = r["date"][:4] if yearly else r["date"]
                #delta[k] += pct * float(r.get("value") or 0.0)
                delta[k] += (pct) * float(r.get("value") or 0.0)

        return delta

    def _compute_after_plan(scn: str, yearly: bool):
        base_map = series_to_dict(fetch_supply_series(scn) or [], yearly=yearly)
        delta    = _plan_delta_for_scenario(scn, yearly)
        after    = defaultdict(float)
        keys = set(base_map.keys()) | set(delta.keys())
        for k in keys:
            after[k] = base_map.get(k, 0.0) - delta.get(k, 0.0)
        return after

    # Build supply figures
    if scenario == "Both":
        # Base P50 & P90
        s_p50_all = fetch_supply_series("P50")
        s_p90_all = fetch_supply_series("P90")
        xs_p50, ys_p50 = sum_series(s_p50_all, yearly=is_yearly)
        xs_p90, ys_p90 = sum_series(s_p90_all, yearly=is_yearly)

        fig_s = go.Figure()
        fig_s.add_trace(go.Scatter(x=xs_p50, y=ys_p50, mode=("lines+markers" if is_yearly else "lines"), name="P50"))
        fig_s.add_trace(go.Scatter(x=xs_p90, y=ys_p90, mode=("lines+markers" if is_yearly else "lines"), name="P90"))

        if has_overlay_plan:
            base_p50 = series_to_dict(s_p50_all, yearly=is_yearly)
            base_p90 = series_to_dict(s_p90_all, yearly=is_yearly)

            after_p50 = _compute_after_plan("P50", is_yearly)
            after_p90 = _compute_after_plan("P90", is_yearly)

            ys_p50_after = [after_p50.get(x, base_p50.get(x, 0.0)) for x in xs_p50]
            ys_p90_after = [after_p90.get(x, base_p90.get(x, 0.0)) for x in xs_p90]

            if has_advanced:
                name_p50 = "Supply P50 after advanced plan"
                name_p90 = "Supply P90 after advanced plan"
            else:
                pct_label = f"{(simple_pct*100):+g}%"
                name_p50 = f"Supply P50 after simple ({pct_label} on {sell_source})"
                name_p90 = f"Supply P90 after simple ({pct_label} on {sell_source})"

            fig_s.add_trace(go.Scatter(
                x=xs_p50, y=ys_p50_after,
                mode=("lines+markers" if is_yearly else "lines"),
                name=name_p50,
                line=dict(width=3, dash="dash")
            ))
            fig_s.add_trace(go.Scatter(
                x=xs_p90, y=ys_p90_after,
                mode=("lines+markers" if is_yearly else "lines"),
                name=name_p90,
                line=dict(width=3, dash="dash")
            ))

        '''fig_s.update_layout(
            title=f"Supply ({'annual' if is_yearly else 'monthly'}) [{unit.upper()}] — {basis} — P50 vs P90",
            xaxis_title=("Year" if is_yearly else "Month"),
            yaxis_title=unit.upper(),
            showlegend=True,
            legend=dict(orientation="h", y=1.02, x=0),
        )'''
        fig_s.update_layout(
            title=f"Supply ({'annual' if is_yearly else 'monthly'}) [{unit.upper()}] — {basis}" if scenario != "Both"
                  else f"Supply ({'annual' if is_yearly else 'monthly'}) [{unit.upper()}] — {basis} — P50 vs P90",
            xaxis_title=("Year" if is_yearly else "Month"),
            yaxis_title=unit.upper(),
            showlegend=True,
            legend=dict(
                orientation="v",   # vertical list
                x=1.02,            # push legend outside the plotting area
                xanchor="left",
                y=1.0,
                yanchor="top"
            ),
            margin=dict(r=220)     # make room on the right for the legend
        )
        scenario_for_gap = "P50"  # unchanged
    else:
        scn = scenario or "P50"
        s_all = fetch_supply_series(scn)
        xs_s, ys_s = sum_series(s_all, yearly=is_yearly)

        fig_s = go.Figure()
        fig_s.add_trace(go.Scatter(
            x=xs_s, y=ys_s,
            mode=("lines+markers" if is_yearly else "lines"),
            name=f"Supply {scn}"
        ))

        if has_overlay_plan:
            base_map  = series_to_dict(s_all, yearly=is_yearly)
            after_map = _compute_after_plan(scn, is_yearly)
            ys_after  = [after_map.get(x, base_map.get(x, 0.0)) for x in xs_s]

            if has_advanced:
                name_after = "Supply after advanced plan"
            else:
                pct_label = f"{(simple_pct*100):+g}%"
                name_after = f"Supply after simple ({pct_label} on {sell_source})"

            fig_s.add_trace(go.Scatter(
                x=xs_s, y=ys_after,
                mode=("lines+markers" if is_yearly else "lines"),
                name=name_after,
                line=dict(width=3, dash="dash")
            ))

        '''fig_s.update_layout(
            title=f"Supply ({'annual' if is_yearly else 'monthly'}) [{unit.upper()}] — {basis}",
            xaxis_title=("Year" if is_yearly else "Month"),
            yaxis_title=unit.upper(),
            showlegend=True,
            legend=dict(orientation="h", y=1.02, x=0),
        )'''
        # legend=dict(orientation="h", x=0, xanchor="left", y=1.02, yanchor="bottom"),margin=dict(t=90)
        fig_s.update_layout(
            title=f"Supply ({'annual' if is_yearly else 'monthly'}) [{unit.upper()}] — {basis}" if scenario != "Both"
                  else f"Supply ({'annual' if is_yearly else 'monthly'}) [{unit.upper()}] — {basis} — P50 vs P90",
            xaxis_title=("Year" if is_yearly else "Month"),
            yaxis_title=unit.upper(),
            showlegend=True,
            legend=dict(
                orientation="v",   # vertical list
                x=1.02,            # push legend outside the plotting area
                xanchor="left",
                y=1.0,
                yanchor="top"
            ),
            margin=dict(r=220)     # make room on the right for the legend
        )
        scenario_for_gap = scn

    # ===== Stacked filled-area Supply chart (by individual SOURCE) =====
    # If "Both" is selected, we default to P50 for this stacked view.
    scn_for_stacked = "P50" if scenario == "Both" else (scenario or "P50")

    # 1) Find candidate source names under current filters (one call)
    rows_for_names = _fetch("/api/supply", {
        "region": region_param,
        "scenario": scn_for_stacked,
        "year_from": yf,
        "year_to": yt,
    }) or []

    # 2) Rank sources by total volume (consistent with BASIS + UNIT as best we can)
    from collections import defaultdict as _dd
    totals_by_source = _dd(float)
    for r in rows_for_names:
        src = (r.get("source") or "").strip()
        if not src:
            continue
        if basis == "equity":
            if unit == "mtpa":
                v = r.get("equity_mtpa")
            elif unit == "mmbtu":
                v = r.get("equity_mmbtu")
            else:
                v = r.get("equity_cargoes")
        else:
            v = r.get(unit)  # mtpa/mmbtu/cargoes (gross)
        try:
            totals_by_source[src] += float(v or 0.0)
        except Exception:
            pass

    # Keep top-N sources to avoid too many traces
    TOP_SOURCES = 10
    top_sources = [s for s, _ in sorted(totals_by_source.items(), key=lambda kv: kv[1], reverse=True)[:TOP_SOURCES]]

    def fetch_supply_for_source(scn, src):
        return _fetch("/api/series/supply_monthly", {
            "unit": unit,
            "scenario": scn,
            "source": src,
            "region": region_param,
            "year_from": yf,
            "year_to": yt,
            "basis": basis
        }) or []

    fig_sa = go.Figure()
    for src in top_sources:
        series = fetch_supply_for_source(scn_for_stacked, src)
        xs, ys = sum_series(series, yearly=is_yearly)  # reuse your helper
        if xs:
            fig_sa.add_trace(go.Scatter(
                x=xs, y=ys,
                mode="lines",
                name=src,
                stackgroup="supply",      # <-- stacked filled-area
                line=dict(width=1),
            ))

    '''fig_sa.update_layout(
        title=f"Stacked Supply by Source ({'annual' if is_yearly else 'monthly'}) [{unit.upper()}] — {basis} — {scn_for_stacked}",
        xaxis_title=("Year" if is_yearly else "Month"),
        yaxis_title=unit.upper(),
        legend=dict(orientation="h", y=1.02, x=0)
    )'''
    #fig_sa.update_layout(legend_font=dict(size=10))
    fig_sa.update_layout(
        title=f"Stacked Supply by Source ({'annual' if is_yearly else 'monthly'}) [{unit.upper()}] — {basis} — {scn_for_stacked}",
        xaxis_title=("Year" if is_yearly else "Month"),
        yaxis_title=unit.upper(),
        legend=dict(
            orientation="v",
            x=1.02, xanchor="left",  # push legend outside the plotting area
            y=1.0,  yanchor="top"
        ),
        margin=dict(r=220)  # space for the side legend
    )

    # ===== Sold vs Uncommitted (stacked bars with percentages; uncommitted hatched) =====
    # Scenario basis: use the same as the stacked area (P50 if "Both" is selected)
    scn_for_bars = "P50" if scenario == "Both" else (scenario or "P50")

    # Base supply for the chosen unit/basis
    supply_series_all = fetch_supply_series(scn_for_bars)
    supply_map_base = series_to_dict(supply_series_all or [], yearly=is_yearly)  # key -> base supply

    # Build plan delta (SIMPLE + ADVANCED) for this scenario, reusing your logic
    from collections import defaultdict as _dd

    def _plan_delta_for_sold_bars(scn: str, yearly: bool):
        delta = _dd(float)

        # SIMPLE: one source across all months/years
        if sell_source and simple_pct != 0:
            for r in (fetch_supply_series(scn, src=sell_source) or []):
                k = r["date"][:4] if yearly else r["date"]
                delta[k] += simple_pct * float(r.get("value") or 0.0)

        # ADVANCED: per-source, per-year pct (already converted to fraction above)
        for src, rr_list in (rules_by_source or {}).items():
            pct_by_year = _dd(float)
            for rr in rr_list:
                pct_by_year[int(rr["year"])] += float(rr["pct"])  # rr["pct"] is a fraction (e.g., -0.10)
            for r in (fetch_supply_series(scn, src=src) or []):
                yy = int(r["date"][:4])
                pct = pct_by_year.get(yy, 0.0)
                if pct == 0:
                    continue
                k = r["date"][:4] if yearly else r["date"]
                #delta[k] += pct * float(r.get("value") or 0.0)
                delta[k] += (pct) * float(r.get("value") or 0.0)
        return delta

    delta_for_bars = _plan_delta_for_sold_bars(scn_for_bars, yearly=is_yearly) if (has_simple or has_advanced) else _dd(float)

    # Supply AFTER plan (clip at zero so denominators can't go negative)
    supply_map = _dd(float)
    for k in set(supply_map_base.keys()) | set(delta_for_bars.keys()):
        supply_map[k] = max(supply_map_base.get(k, 0.0) - delta_for_bars.get(k, 0.0), 0.0)

    # Build sold (FIRM opportunities) per period, broken down by contract
    from collections import defaultdict as _dd
    firm_rows = _fetch("/api/opportunities", {
        "status": "firm",
        "region": region_param,
        "year_from": yf,
        "year_to": yt
    }) or []

    sold_by_contract = _dd(lambda: _dd(float))   # contract -> {period_key -> vol}
    total_by_contract = _dd(float)               # contract -> total across range

    def _period_keys_for_year(y: int):
        if is_yearly:
            return [f"{y:04d}"]
        # monthly: spread evenly across 12 months
        return [f"{y:04d}-{m:02d}-01" for m in range(1, 13)]

    for r in firm_rows:
        name = (r.get("contract_name") or "Unknown").strip() or "Unknown"
        try:
            y = int(r.get("year") or 0)
            v = float(r.get(unit) or 0.0)  # mtpa/mmbtu/cargoes
        except Exception:
            continue
        if y <= 0 or v <= 0:
            continue

        if is_yearly:
            sold_by_contract[name][f"{y:04d}"] += v
            total_by_contract[name] += v
        else:
            per_month = v / 12.0
            for key in _period_keys_for_year(y):
                sold_by_contract[name][key] += per_month
                total_by_contract[name] += per_month

    # X axis (union of supply keys and sold keys)
    all_keys = set(supply_map.keys())
    for kmap in sold_by_contract.values():
        all_keys.update(kmap.keys())
    xs_su = sorted(all_keys)

    # Keep top-N contracts to avoid clutter; aggregate the rest into "Other firm"
    TOP_CONTRACTS = 6
    top_names = [n for n, _ in sorted(total_by_contract.items(), key=lambda kv: kv[1], reverse=True)[:TOP_CONTRACTS]]
    other_name = "Other firm"

    # Precompute totals per period and the per-trace arrays
    sold_totals_by_key = _dd(float)
    ys_by_contract = {}

    for name in top_names:
        ys = [sold_by_contract[name].get(x, 0.0) for x in xs_su]
        ys_by_contract[name] = ys
        for x, yv in zip(xs_su, ys):
            sold_totals_by_key[x] += yv

    # "Other firm" bucket
    ys_other = [0.0] * len(xs_su)
    for name, kmap in sold_by_contract.items():
        if name in top_names:
            continue
        for i, x in enumerate(xs_su):
            yv = kmap.get(x, 0.0)
            if yv:
                ys_other[i] += yv
                sold_totals_by_key[x] += yv
    if any(ys_other):
        ys_by_contract[other_name] = ys_other

    # Uncommitted = Supply - Sold (clip at zero)
    ys_supply = [supply_map.get(x, 0.0) for x in xs_su]
    ys_sold_total = [sold_totals_by_key[x] for x in xs_su]
    ys_uncommitted = [max(s - so, 0.0) for s, so in zip(ys_supply, ys_sold_total)]

    # Percent labels need period totals (sold + uncommitted)
    period_totals = [ (so + uc) for so, uc in zip(ys_sold_total, ys_uncommitted) ]

    def _pct_array(values, totals):
        out = []
        for v, t in zip(values, totals):
            if t > 0:
                out.append(v / t)
            else:
                out.append(None)
        return out

    fig_su = go.Figure()

    # Add each top contract (stacked bars) with % labels
    for name, ys in ys_by_contract.items():
        custom = _pct_array(ys, period_totals)
        fig_su.add_trace(go.Bar(
            x=xs_su, y=ys, name=name,
            customdata=custom,
            texttemplate="%{customdata:.0%}",
            textposition="inside",
            hovertemplate="%{x}<br>" + name + ": %{y:.3f} " + unit.upper() + "<extra></extra>",
        ))

    # Add uncommitted as hatched bar on top
    custom_unc = _pct_array(ys_uncommitted, period_totals)
    fig_su.add_trace(go.Bar(
        x=xs_su, y=ys_uncommitted, name="Uncommitted",
        customdata=custom_unc,
        #texttemplate="%{customdata:.0%}",
        #textposition="inside",
        marker=dict(
            pattern=dict(shape="/", solidity=0.3),
            line=dict(width=0.5)
        ),
        hovertemplate="%{x}<br>Uncommitted: %{y:.3f} " + unit.upper() + "<extra></extra>",
    ))

    fig_su.update_layout(
        barmode="stack",
        title=f"Sold vs Uncommitted ({'annual' if is_yearly else 'monthly'}) [{unit.upper()}] — {basis} — {scn_for_bars}",
        xaxis_title=("Year" if is_yearly else "Month"),
        yaxis_title=unit.upper(),
        legend=dict(
            orientation="v",
            x=1.02, xanchor="left",
            y=1,    yanchor="top"
        ),
        margin=dict(r=200, t=70)  # room for the side legend
    )

    fig_su.update_traces(
        selector=dict(name="Uncommitted"),
        textfont=dict(color="#111827"),   # dark gray/near black
        insidetextanchor="middle"
    )

    # --- Uncommitted % square chips inside fig_su (absolute bars) ---
    # --- Uncommitted % square chips inside fig_su (match fig_su_pct style) ---
    # --- Uncommitted % SQUARE chips inside fig_su (pixel-true, like fig_su_pct) ---

    # === Uncommitted % SQUARE chips for fig_su (absolute bars), mirroring fig_su_pct style ===

    # 1) Give the figure a known size so pixel math is predictable
    '''if not getattr(fig_su.layout, "width", None):
        fig_su.update_layout(width=1100)
    if not getattr(fig_su.layout, "height", None):
        fig_su.update_layout(height=650)'''

    # 2) Make sure we have headroom and a defined y-range
    bar_tops = [(so + uc) for so, uc in zip(ys_sold_total, ys_uncommitted)]
    if bar_tops:
        ymax = max(bar_tops)
        if ymax > 0:
            fig_su.update_yaxes(range=[0, ymax * 1.08])

    # Safe reads for domains/ranges (Plotly may leave them None until render)
    xdom = tuple(getattr(getattr(fig_su.layout, "xaxis", None), "domain", None) or (0.0, 1.0))
    ydom = tuple(getattr(getattr(fig_su.layout, "yaxis", None), "domain", None) or (0.0, 1.0))

    fig_w = float(fig_su.layout.width or 1100)
    fig_h = float(fig_su.layout.height or 650)

    yrng = list(getattr(getattr(fig_su.layout, "yaxis", None), "range", None) or [0.0, (max(bar_tops) if bar_tops else 1.0)])
    yrng_span = float(yrng[1]) - float(yrng[0]) or 1.0

    # pixels per x-domain unit and per y data unit
    xdom_span = (xdom[1] - xdom[0]) or 1.0
    ydom_span = (ydom[1] - ydom[0]) or 1.0

    px_per_xdomain = xdom_span * fig_w
    px_per_yunit   = (ydom_span * fig_h) / yrng_span

    # 3) Parameters controlling chip size — keep same feel as fig_su_pct
    V_FRACTION_OF_U = 0.38         # square side as fraction of UC height
    SIDE_CAP_Y      = 0.10*yrng_span   # cap in y-units (10% of axis span)
    MIN_SIDE_Y      = 0.035*yrng_span  # min in y-units (3.5% of axis span)

    # Helper: center of bar i in x-domain
    def _x_center_domain(i, n):
        return (i + 0.5) / n

    n = len(xs_su)
    if n and px_per_xdomain > 0 and px_per_yunit > 0:
        # % labels for UC relative to each period total
        period_totals = [(so + uc) for so, uc in zip(ys_sold_total, ys_uncommitted)]
        uc_pct = [(uc / t * 100.0) if t > 0 else 0.0 for uc, t in zip(ys_uncommitted, period_totals)]

        for i, x in enumerate(xs_su):
            uc = float(ys_uncommitted[i] or 0.0)
            if uc <= 0:
                continue

            # If UC slice is tiny, place label above the bar instead of a chip
            if uc_pct[i] < 6.0:
                fig_su.add_annotation(
                    x=x, y=bar_tops[i],
                    xref="x", yref="y",
                    text=f"{uc_pct[i]:.0f}%",
                    showarrow=False,
                    yshift=10,
                    font=dict(color="#111827", size=11),
                    xanchor="center", yanchor="bottom"
                )
                continue

            # --- Compute square side in y-units (absolute scale) ---
            side_y = max(MIN_SIDE_Y, min(SIDE_CAP_Y, V_FRACTION_OF_U * uc))

            # Keep the square fully inside the Uncommitted slice [sold, sold+uc]
            sold = float(ys_sold_total[i] or 0.0)
            y_bottom_uc = sold
            y_top_uc    = sold + uc
            y_center    = (y_bottom_uc + y_top_uc) / 2.0

            y0 = max(y_bottom_uc, y_center - side_y / 2.0)
            y1 = min(y_top_uc,    y_center + side_y / 2.0)
            side_y = max(0.0, y1 - y0)  # re-adjust if clipped

            # --- Convert that y-size to an x-domain size for a SQUARE in pixels ---
            side_px      = side_y * px_per_yunit
            side_xdomain = side_px / px_per_xdomain

            # Center horizontally on the bar
            x_center_dom = _x_center_domain(i, n)
            x0 = x_center_dom - side_xdomain / 2.0
            x1 = x_center_dom + side_xdomain / 2.0

            # Draw the square
            fig_su.add_shape(
                type="rect",
                xref="x domain", yref="y",
                x0=x0, x1=x1, y0=y0, y1=y1,
                fillcolor="rgba(255,255,255,0.95)",
                line=dict(width=0),
                layer="above"
            )

            # % label centered in the square
            fig_su.add_annotation(
                x=x_center_dom,               # same horizontal center as the square
                y=(y0 + y1) / 2.0,            # square's vertical center
                xref="x domain", yref="y",    # same refs as the square shape
                text=f"{uc_pct[i]:.0f}%",
                showarrow=False,
                font=dict(color="#111827", size=10),
                xanchor="center", yanchor="middle"
            )
    
    # ===== NEW: Sold vs Uncommitted (absolute) — fig_su_abs =====
    # Same bars as fig_su (absolute volumes) but WITHOUT bar % labels,
    # and with an overlay line for Committed (with % labels vs supply).

    fig_su_abs = go.Figure()

    # Add each top contract (stacked bars) without % text in bars
    for name, ys in ys_by_contract.items():
        fig_su_abs.add_trace(go.Bar(
            x=xs_su, y=ys, name=name,
            hovertemplate="%{x}<br>" + name + ": %{y:.3f} " + unit.upper() + "<extra></extra>",
        ))

    # Add uncommitted on top (keep the same hatched style as fig_su)
    fig_su_abs.add_trace(go.Bar(
        x=xs_su, y=ys_uncommitted, name="Uncommitted",
        marker=dict(
            pattern=dict(shape="/", solidity=0.3),
            line=dict(width=0.5)
        ),
        hovertemplate="%{x}<br>Uncommitted: %{y:.3f} " + unit.upper() + "<extra></extra>",
    ))

    # --- Overlay line: connect TOP of each stacked bar, label shows committed %
    committed_abs = [sold_totals_by_key[x] for x in xs_su]   # total sold per period
    supply_abs    = [supply_map.get(x, 0.0) for x in xs_su]  # total supply per period

    # Percent labels (Committed as share of Supply)
    committed_share_labels = [
        (f"{(c/s):.0%}" if s and s > 0 else "")
        for c, s in zip(committed_abs, supply_abs)
    ]

    # Top of each stacked bar = committed + uncommitted
    bar_tops = [c + u for c, u in zip(committed_abs, ys_uncommitted)]

    fig_su_abs.add_trace(go.Scatter(
        x=xs_su,
        y=bar_tops,  # <<— anchor line to bar tops
        name="Committed (share) — overlay",
        mode="lines+markers+text",
        text=committed_share_labels,
        textposition="top center",
        textfont=dict(size=11),
        line=dict(width=3),
        cliponaxis=False,
        hovertemplate=(
            "%{x}"
            "<br>Total (bar top): %{y:.3f} " + unit.upper() +
            "<br>Committed share: %{text}"
            "<extra></extra>"
        ),
    ))

    # Layout: legend outside (right), room for it, and optional headroom for labels
    fig_su_abs.update_layout(
        barmode="stack",
        title=f"Sold vs Uncommitted (absolute) ({'annual' if is_yearly else 'monthly'}) [{unit.upper()}] — {basis} — {scn_for_bars}",
        xaxis_title=("Year" if is_yearly else "Month"),
        yaxis_title=unit.upper(),
        legend=dict(
            orientation="v",
            x=1.02, xanchor="left",
            y=1.0,  yanchor="top"
        ),
        margin=dict(r=200, t=70)
    )

    # Headroom so % labels on the line don't clip
    if bar_tops:
        ymax = max(bar_tops)
        if ymax > 0:
            fig_su_abs.update_yaxes(range=[0, ymax * 1.08])

    # ===== Sold vs Uncommitted as % of Supply (100% stacked) =====
    # Reuse earlier variables from the Sold vs Uncommitted block:
    # xs_su, ys_by_contract (dict name->list), sold_totals_by_key, ys_uncommitted, supply_map, unit, basis, is_yearly

    # --- Overlay line: (JKM% + TTF% + Uncommitted%) labels on each year/month ---
    # ===== Sold vs Uncommitted — 100% of Supply (stacked) + overlay line =====
    
    # Sold vs Uncommitted — 100% of Supply (stacked bars + top strip lines)
    # =========================
    from plotly.subplots import make_subplots
    from collections import defaultdict as _dd

    # denominator per period = supply
    denom = [supply_map.get(x, 0.0) for x in xs_su]

    def _pct(vals, den):
        out = []
        for v, d in zip(vals, den):
            out.append((v / d) if d and d > 0 else 0.0)
        return out

    # convert bars to 0..1 shares
    ys_by_contract_pct = {name: _pct(ys, denom) for name, ys in ys_by_contract.items()}
    ys_uncommitted_pct = _pct(ys_uncommitted, denom)

    # --- overlay numerator: JKM + TTF + Uncommitted (as a % of supply) ---
    sold_by_index = _dd(lambda: _dd(float))
    for r in (firm_rows or []):
        idx = (r.get("pricing_index") or "").strip().upper()
        if idx not in ("JKM", "TTF"):
            continue
        try:
            y = int(r.get("year") or 0)
            v = float(r.get(unit) or 0.0)
        except Exception:
            continue
        if y <= 0 or v <= 0:
            continue
        if is_yearly:
            sold_by_index[idx][f"{y:04d}"] += v
        else:
            per_month = v / 12.0
            for key in _period_keys_for_year(y):
                sold_by_index[idx][key] += per_month

    jkm_abs = [sold_by_index["JKM"].get(x, 0.0) for x in xs_su]
    ttf_abs = [sold_by_index["TTF"].get(x, 0.0) for x in xs_su]
    jkm_sh  = _pct(jkm_abs, denom)
    ttf_sh  = _pct(ttf_abs, denom)

    overlay_pct  = [min((jk or 0.0) + (tf or 0.0) + (uc or 0.0), 1.0)
                    for jk, tf, uc in zip(jkm_sh, ttf_sh, ys_uncommitted_pct)]
    overlay_text = [f"{p:.0%}" for p in overlay_pct]

    # ---- 2-row subplot: thin strip for overlay line (row 1), 100% stacked bars (row 2) ----
    fig_su_pct = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.22, 0.78], vertical_spacing=0.02
    )

    # row 1: overlay line (ABOVE the bars)
    fig_su_pct.add_trace(
        go.Scatter(
            x=xs_su, y=overlay_pct,
            name="Gas Hub(JKM/TTF/Uncommitted)",
            mode="lines+markers+text",
            text=[f"{p:.0%}" for p in overlay_pct],
            textposition="top center",
            cliponaxis=False,
            hovertemplate="%{x}<br>%{text}<extra></extra>",
        ),
        row=1, col=1
    )

    # row 2: 100% stacked bars (with % labels INSIDE)
    for name, ys in ys_by_contract_pct.items():
        fig_su_pct.add_trace(
            go.Bar(
                x=xs_su, y=ys, name=name,
                hovertemplate="%{x}<br>" + name + ": %{y:.0%}<extra></extra>",
                texttemplate="%{y:.0%}", textposition="inside",
            ),
            row=2, col=1
        )

    fig_su_pct.add_trace(
        go.Bar(
            x=xs_su,
            y=ys_uncommitted_pct,
            name="Uncommitted",
            marker=dict(pattern=dict(shape="/", solidity=0.3), line=dict(width=0.5)),
            hovertemplate="%{x}<br>Uncommitted: %{y:.0%}<extra></extra>",
            # give it explicit text (used by our label-mover)
            #text=[f"{v:.0%}" for v in ys_uncommitted_pct],
            #texttemplate="%{text}",
            #textposition="inside",
        ),
        row=2, col=1
    )

    # 1) Uncommitted: outside the bar
    

    # move only Uncommitted % labels outside (subplot-aware)
    #move_uncommitted_labels_top_subplot(fig_su_pct, bar_row=2, bar_col=1, is_fraction=True)

    # Remove built-in % labels from the Uncommitted bar (row 2)
    fig_su_pct.update_traces(
        selector=dict(type="bar", name="Uncommitted"),
        text=None,
        texttemplate=None
    )

# (If you still have it) disable the mover for fig_su_pct — we don't want extra labels
# move_uncommitted_labels_top_subplot(fig_su_pct, bar_row=2, bar_col=1, is_fraction=True)  # <-- delete/comment out

    # === paste the scaling code here ===
    def _size_by_height(ys, min_size=8, max_size=14, low=0.06, high=0.60):
        out = []
        span = max(1e-9, high - low)
        for y in ys or []:
            y = float(y or 0)
            t = (y - low) / span
            t = 0.0 if t < 0 else (1.0 if t > 1 else t)
            out.append(min_size + t * (max_size - min_size))
        return out

    def _text_color_for(ys, cutoff=0.25, dark="#111827", light="#FFFFFF"):
        return [light if (float(y or 0) >= cutoff) else dark for y in ys or []]

    for i, tr in enumerate(fig_su_pct.data):
        if getattr(tr, "type", None) == "bar" and "committed" in ((tr.name or "").lower()) and (tr.name or "").lower() != "uncommitted":
            ys = list(tr.y or [])
            fig_su_pct.data[i].textposition = "inside"
            fig_su_pct.data[i].insidetextanchor = "middle"
            fig_su_pct.data[i].texttemplate = "%{y:.0%}"
            fig_su_pct.data[i].textfont = dict(
                size=_size_by_height(ys, min_size=8, max_size=13, low=0.05, high=0.55),
                color=_text_color_for(ys, cutoff=0.25)
            )

    fig_su_pct.update_layout(
        uniformtext_minsize=8,
        uniformtext_mode="hide"
    )
    # apply to all bar traces (where the text actually lives)
    fig_su_pct.update_traces(constraintext="both", selector=dict(type="bar"))
    # === end of paste ===

    # ---- Add "Uncommitted %" chips above each category (row 2) ----
    uc_labels = [f"{p:.0%}" for p in ys_uncommitted_pct]  # ys_uncommitted_pct is 0..1
    #add_pct_chips(fig_su_pct, xs_su, uc_labels, row=2, col=1)

    # axes & layout
    fig_su_pct.update_yaxes(visible=False, range=[0, 1.1], showgrid=False, zeroline=False, row=1, col=1)
    fig_su_pct.update_yaxes(title_text="Share of Supply", tickformat=".0%", range=[0, 1], row=2, col=1)
    fig_su_pct.update_xaxes(title_text=("Year" if is_yearly else "Month"), row=2, col=1)

    fig_su_pct.update_layout(
        barmode="stack",
        title=f"Sold vs Uncommitted — 100% of Supply ({'annual' if is_yearly else 'monthly'}) [{unit.upper()}] — {basis}",
        legend=dict(orientation="v", x=1.02, xanchor="left", y=1.0, yanchor="top"),
        margin=dict(r=220, t=70, b=60)
    )

    # keep Uncommitted text dark-on-light if needed
    fig_su_pct.update_traces(
        selector=dict(name="Uncommitted"),
        textfont=dict(color="#111827"),
        insidetextanchor="middle"
    )

    # === Uncommitted % SQUARE chips for fig_su_pct (row 2, 100% stacked) ===
    # Place *inside* the Uncommitted slice, centered, sized as a square in pixels.

    # === Uncommitted % SQUARE chips for fig_su (absolute stacked bars) ===

    # 4) Draw SQUARE chips inside the Uncommitted segment (row 2) with centered %
    #    We do this with xref="x domain" + yref="y2" and figure/subplot domains.

    # --- SQUARE chips inside the Uncommitted slice (row 2) ---

    # 1) Lock a known figure size so pixel math stays consistent
    '''if not getattr(fig_su_pct.layout, "width", None):
        fig_su_pct.update_layout(width=1100)
    if not getattr(fig_su_pct.layout, "height", None):
        fig_su_pct.update_layout(height=650)'''

    # 2) Read subplot domains to convert y-units <-> pixels, and pixels <-> x-domain units
    xdom  = tuple(getattr(fig_su_pct.layout.xaxis,  "domain", None) or (0.0, 1.0))   # row-2 shares x with row-1
    y2dom = tuple(getattr(fig_su_pct.layout.yaxis2, "domain", None) or (0.0, 1.0))   # row-2 vertical domain

    fig_w = float(fig_su_pct.layout.width or 1100)
    fig_h = float(fig_su_pct.layout.height or 650)

    # pixels per unit in x-domain and in y2 data units (y2 is [0..1] for the 100% bars)
    xdom_span   = max(1e-9, (xdom[1] - xdom[0]))
    y2dom_span  = max(1e-9, (y2dom[1] - y2dom[0]))
    px_per_xdom = xdom_span * fig_w               # pixels for 1.0 of x-domain
    px_per_y2   = y2dom_span * fig_h              # pixels for 1.0 of y2 (100% stack)

    # 3) Square sizing parameters (tune to taste)
    V_FRACTION_OF_U = 0.38   # square side in y2-units as a fraction of the UC slice height u
    SIDE_CAP_Y      = 0.10   # absolute cap on side in y2-units
    MIN_SIDE_Y      = 0.035  # minimum side in y2-units

    # Helper: horizontal center of bar i in x-domain (categories are evenly spaced)
    def _x_center_domain(i, n):
        return (i + 0.5) / max(1, n)

    n = len(xs_su)
    if n:
        # For the 100% stacked bar, UC slice height is u; committed share is 1-u
        committed_share = [1.0 - float(u or 0.0) for u in ys_uncommitted_pct]

        for i, x in enumerate(xs_su):
            u = float(ys_uncommitted_pct[i] or 0.0)
            if u <= 0:
                continue

            # If UC slice is tiny, show a label above instead of a chip
            if u < 0.06:
                fig_su_pct.add_annotation(
                    x=x, y=1.0, xref="x", yref="y2",
                    text=f"{u:.0%}", showarrow=False, yshift=8,
                    font=dict(color="#111827", size=11)
                )
                continue

            # --- Compute a vertical side in y2 units (0..1), then convert to an x-domain width
            side_y = max(MIN_SIDE_Y, min(SIDE_CAP_Y, V_FRACTION_OF_U * u))

            # Keep the square fully inside the UC slice [bottom, 1.0]
            bottom    = committed_share[i]            # start of UC slice
            y_center  = bottom + u * 0.5
            y0        = max(bottom, y_center - side_y / 2.0)
            y1        = min(1.0,    y_center + side_y / 2.0)
            side_y    = y1 - y0                         # (re-)exact side after boundary clamp

            # Convert vertical side (y2 units) -> pixels, then pixels -> x-domain units to keep it square
            side_px      = side_y * px_per_y2
            side_xdomain = side_px / px_per_xdom

            # Center the square horizontally over bar i in x-domain
            x_center_dom = _x_center_domain(i, n)
            x0_dom = x_center_dom - side_xdomain / 2.0
            x1_dom = x_center_dom + side_xdomain / 2.0

            # Draw the SQUARE chip
            fig_su_pct.add_shape(
                type="rect",
                xref="x domain", yref="y2",
                x0=x0_dom, x1=x1_dom, y0=y0, y1=y1,
                fillcolor="rgba(255,255,255,0.95)",
                line=dict(width=0),
                layer="above"
            )

            # % label exactly centered inside the square
            fig_su_pct.add_annotation(
                x=x_center_dom, y=(y0 + y1) / 2.0,
                xref="x domain", yref="y2",
                text=f"{u:.0%}", showarrow=False,
                font=dict(color="#111827", size=10),
                xanchor="center", yanchor="middle"
            )

    # A bit of headroom so above-bar labels (for UC < 6%) don't clip
    fig_su_pct.update_yaxes(row=2, col=1, range=[0, 1.08])

    #Keep legend on the right but reduce padding
    '''fig_su.update_layout(
        legend=dict(orientation="v", x=1.01, xanchor="left", y=1.0, yanchor="top"),
        margin=dict(r=120, l=60, t=70, b=60)
    )
    fig_su_pct.update_layout(
        legend=dict(orientation="v", x=1.01, xanchor="left", y=1.0, yanchor="top"),
        margin=dict(r=120, l=60, t=70, b=60)
    )'''

    '''fig_su.update_layout(
        legend=dict(orientation="h", y=-0.2, yanchor="top", x=0, xanchor="left"),
        margin=dict(l=60, r=60, t=70, b=120)
    )
    fig_su_pct.update_layout(
        legend=dict(orientation="h", y=-0.2, yanchor="top", x=0, xanchor="left"),
        margin=dict(l=60, r=60, t=70, b=120)
    )'''


    # DO NOT keep the global "all bars outside" block

    # use the subplot-aware mover for outside labels + headroom
    #move_uncommitted_labels_top_subplot(fig_su_pct, bar_row=2, bar_col=1, is_fraction=True)
        
    # ============================
    #  Opportunities chart
    # ============================
    o_firm = _fetch("/api/series/opportunity_monthly", {
        "unit": unit, "status": "firm", "region": region_param,
        "year_from": yf, "year_to": yt
    })
    o_neg = _fetch("/api/series/opportunity_monthly", {
        "unit": unit, "status": "negotiation", "region": region_param,
        "year_from": yf, "year_to": yt, "probability_weighted": probability_weighted
    })
    o_opt = _fetch("/api/series/opportunity_monthly", {
        "unit": unit, "status": "option", "region": region_param,
        "year_from": yf, "year_to": yt, "probability_weighted": probability_weighted
    })
    
    def _sum(rows):
        return sum(float(r.get("value") or 0.0) for r in (rows or []))

    print("o_firm rows:", len(o_firm or []), "sum:", _sum(o_firm))
    print("o_neg  rows:", len(o_neg  or []), "sum:", _sum(o_neg))
    print("o_opt  rows:", len(o_opt  or []), "sum:", _sum(o_opt))

    # optional: show first few negotiation points
    print("o_neg sample:", (o_neg or [])[:5])

    if is_yearly:
        xs_f, ys_f = sum_by_year(o_firm)
        xs_n, ys_n = sum_by_year(o_neg)
        xs_o, ys_o = sum_by_year(o_opt)
        acc_total = defaultdict(float)
        for rows in (o_firm, o_neg, o_opt):
            for r in rows or []:
                acc_total[r["date"][:4]] += float(r.get("value") or 0.0)
        xs_t = sorted(acc_total.keys())
        ys_t = [acc_total[x] for x in xs_t]
    else:
        xs_f, ys_f = sum_by_month(o_firm)
        xs_n, ys_n = sum_by_month(o_neg)
        xs_o, ys_o = sum_by_month(o_opt)
        acc_total = defaultdict(float)
        for rows in (o_firm, o_neg, o_opt):
            for r in rows or []:
                acc_total[r["date"]] += float(r.get("value") or 0.0)
        xs_t = sorted(acc_total.keys())
        ys_t = [acc_total[x] for x in xs_t]

    neg_label = "Negotiation (weighted)" if probability_weighted else "Negotiation"
    opt_label = "Option (weighted)" if probability_weighted else "Option"

    fig_o = go.Figure()
    mk = "lines+markers" if is_yearly else "lines"
    fig_o.add_trace(go.Scatter(x=xs_f, y=ys_f, mode=mk, name="Firm"))
    fig_o.add_trace(go.Scatter(x=xs_n, y=ys_n, mode=mk, name=neg_label))
    fig_o.add_trace(go.Scatter(x=xs_o, y=ys_o, mode=mk, name=opt_label))
    fig_o.add_trace(go.Scatter(x=xs_t, y=ys_t, mode=mk, name="Total", line=dict(width=3)))
    fig_o.update_layout(
        title=f"Opportunities ({'annual' if is_yearly else 'monthly'}) [{unit.upper()}]",
        xaxis_title=("Year" if is_yearly else "Month"),
        yaxis_title=unit.upper()
    )

    # ============================
    #  Contracted + Negotiations + Supply overlay (uses prebuilt overlay_delta_* maps)
    # ============================
    def _unit_value(row): return float(row.get(unit) or 0.0)
    fig_cn = go.Figure()

    if is_yearly:
        firm_rows = _fetch("/api/opportunities", {
            "status": "firm", "region": region_param, "year_from": yf, "year_to": yt
        }) or []
        from collections import defaultdict as _dd
        firm_by_year = _dd(float); years = set()
        for r in firm_rows:
            y = str(r.get("year")); years.add(y)
            firm_by_year[y] += _unit_value(r)

        neg_rows = _fetch("/api/opportunities", {
            "status": "negotiation", "region": region_param, "year_from": yf, "year_to": yt
        }) or []
        for r in neg_rows:
            base_v = _unit_value(r)
            r[unit] = (base_v * float(r.get("probability") or 0.0)) if probability_weighted else base_v

        neg_by_contract_year = _dd(lambda: _dd(float))
        contract_totals = _dd(float)
        for r in neg_rows:
            name = r.get("contract_name") or "Unknown"
            y = str(r.get("year")); v = float(r[unit])
            neg_by_contract_year[name][y] += v
            contract_totals[name] += v
            years.add(y)

        years = sorted(years)
        fig_cn.add_trace(go.Bar(x=years, y=[firm_by_year.get(y,0.0) for y in years], name="Contracted (Firm)"))

        TOP_N = 8
        top_names = [n for n,_ in sorted(contract_totals.items(), key=lambda kv: kv[1], reverse=True)[:TOP_N]]
        other_by_year = _dd(float)
        for name in neg_by_contract_year.keys():
            if name in top_names:
                fig_cn.add_trace(go.Bar(x=years, y=[neg_by_contract_year[name].get(y,0.0) for y in years], name=name))
            else:
                for y in years: other_by_year[y] += neg_by_contract_year[name].get(y,0.0)
        if other_by_year:
            fig_cn.add_trace(go.Bar(x=years, y=[other_by_year.get(y,0.0) for y in years], name="Other negotiations"))

        # Supply P50/P90 lines
        s_p50_all = fetch_supply_series("P50")
        s_p90_all = fetch_supply_series("P90")
        xs_p50, ys_p50 = sum_by_year(s_p50_all)
        xs_p90, ys_p90 = sum_by_year(s_p90_all)
        p50_map = {x: y for x, y in zip(xs_p50, ys_p50)}
        p90_map = {x: y for x, y in zip(xs_p90, ys_p90)}

        fig_cn.add_trace(go.Scatter(x=years, y=[p50_map.get(y, 0.0) for y in years],
                                    mode="lines+markers", name="Supply P50"))
        fig_cn.add_trace(go.Scatter(x=years, y=[p90_map.get(y, 0.0) for y in years],
                                    mode="lines+markers", name="Supply P90"))

        # After-plan overlays (per scenario)
        if has_overlay_plan:
            if scenario == "Both":
                y_after_p50 = [p50_map.get(y, 0.0) - overlay_y_p50.get(y, 0.0) for y in years]
                y_after_p90 = [p90_map.get(y, 0.0) - overlay_y_p90.get(y, 0.0) for y in years]
                fig_cn.add_trace(go.Scatter(
                    x=years, y=y_after_p50, mode="lines+markers",
                    name="Supply P50 after plan", line=dict(width=3, dash="dash")
                ))
                fig_cn.add_trace(go.Scatter(
                    x=years, y=y_after_p90, mode="lines+markers",
                    name="Supply P90 after plan", line=dict(width=3, dash="dot")
                ))
            else:
                if (scenario or "P50") == "P90":
                    base_map, overlay_y = p90_map, overlay_y_p90
                else:
                    base_map, overlay_y = p50_map, overlay_y_p50
                y_after = [base_map.get(y, 0.0) - overlay_y.get(y, 0.0) for y in years]
                fig_cn.add_trace(go.Scatter(
                    x=years, y=y_after, mode="lines+markers",
                    name="Supply after plan", line=dict(width=3, dash="dash")
                ))
        fig_cn.update_layout(
            barmode="stack",
            title=f"Contracted + Negotiations (+ Supply P50/P90) — annual [{unit.upper()}]",
            xaxis_title="Year", yaxis_title=unit.upper()
        )
    else:
        # Monthly bars: contracted + negotiations
        firm_m = _fetch("/api/series/opportunity_monthly", {
            "unit": unit, "status": "firm", "region": region_param,
            "year_from": yf, "year_to": yt, "probability_weighted": False
        }) or []
        neg_m = _fetch("/api/series/opportunity_monthly", {
            "unit": unit, "status": "negotiation", "region": region_param,
            "year_from": yf, "year_to": yt, "probability_weighted": probability_weighted
        }) or []

        xs_f, ys_f = sum_by_month(firm_m)
        xs_n, ys_n = sum_by_month(neg_m)
        xs_union = sorted(set(xs_f) | set(xs_n))
        firm_map = {x: y for x, y in zip(xs_f, ys_f)}
        neg_map  = {x: y for x, y in zip(xs_n, ys_n)}

        fig_cn.add_trace(go.Bar(x=xs_union, y=[firm_map.get(x, 0.0) for x in xs_union], name="Contracted (Firm)"))
        fig_cn.add_trace(go.Bar(x=xs_union, y=[neg_map.get(x, 0.0)  for x in xs_union], name=("Negotiation (weighted)" if probability_weighted else "Negotiation")))

        # Supply P50/P90 monthly lines
        s_p50_m = fetch_supply_series("P50") or []
        s_p90_m = fetch_supply_series("P90") or []
        xs50, ys50 = sum_by_month(s_p50_m)
        xs90, ys90 = sum_by_month(s_p90_m)
        p50m = {x: y for x, y in zip(xs50, ys50)}
        p90m = {x: y for x, y in zip(xs90, ys90)}

        fig_cn.add_trace(go.Scatter(x=xs_union, y=[p50m.get(x, 0.0) for x in xs_union],
                                    mode="lines", name="Supply P50"))
        fig_cn.add_trace(go.Scatter(x=xs_union, y=[p90m.get(x, 0.0) for x in xs_union],
                                    mode="lines", name="Supply P90"))

        # After-plan monthly overlays (per scenario)
        if has_overlay_plan:
            if scenario == "Both":
                y_after_p50 = [p50m.get(x, 0.0) - overlay_m_p50.get(x, 0.0) for x in xs_union]
                y_after_p90 = [p90m.get(x, 0.0) - overlay_m_p90.get(x, 0.0) for x in xs_union]
                fig_cn.add_trace(go.Scatter(
                    x=xs_union, y=y_after_p50, mode="lines",
                    name="Supply P50 after plan", line=dict(width=3, dash="dash")
                ))
                fig_cn.add_trace(go.Scatter(
                    x=xs_union, y=y_after_p90, mode="lines",
                    name="Supply P90 after plan", line=dict(width=3, dash="dot")
                ))
            else:
                if (scenario or "P50") == "P90":
                    base_m, overlay_m = p90m, overlay_m_p90
                else:
                    base_m, overlay_m = p50m, overlay_m_p50
                y_after = [base_m.get(x, 0.0) - overlay_m.get(x, 0.0) for x in xs_union]
                fig_cn.add_trace(go.Scatter(
                    x=xs_union, y=y_after, mode="lines",
                    name="Supply after plan", line=dict(width=3, dash="dash")
                ))

        fig_cn.update_layout(
            barmode="stack",
            title=f"Contracted + Negotiations (+ Supply P50/P90) — monthly [{unit.upper()}]",
            xaxis_title="Month", yaxis_title=unit.upper()
        )

    # --- Color mapping (unique per opportunity) ---
    if is_yearly:
        has_other = any((other_by_year or {}).values())

        special_fixed = {
            "Contracted (Firm)": "#5459AC",    # lock Firm to your indigo
        }
        if has_other:
            special_fixed["Other negotiations"] = "#9CA3AF"  # neutral gray bucket

        legend_order = ["Contracted (Firm)"] + top_names + (["Other negotiations"] if has_other else [])
        cmap = build_unique_color_map(legend_order, fixed=special_fixed)

        # Bars (unique colors, no repeats)
        fig_cn.add_trace(go.Bar(
            x=years,
            y=[firm_by_year.get(y, 0.0) for y in years],
            name="Contracted (Firm)",
            marker_color=cmap["Contracted (Firm)"],
        ))
        for name in top_names:
            fig_cn.add_trace(go.Bar(
                x=years,
                y=[neg_by_contract_year[name].get(y, 0.0) for y in years],
                name=name,
                marker_color=cmap[name],
            ))
        if has_other:
            fig_cn.add_trace(go.Bar(
                x=years,
                y=[other_by_year.get(y, 0.0) for y in years],
                name="Other negotiations",
                marker_color=cmap["Other negotiations"],
            ))
    else:
         # (keep your existing monthly data prep here)
        xs_f, ys_f = sum_by_month(firm_m)
        xs_n, ys_n = sum_by_month(neg_m)
        xs_union = sorted(set(xs_f) | set(xs_n))
        firm_map = {x: y for x, y in zip(xs_f, ys_f)}
        neg_map  = {x: y for x, y in zip(xs_n, ys_n)}

        # --- UNIQUE COLORS (monthly) ---
        neg_name = "Negotiation (weighted)" if probability_weighted else "Negotiation"
        names = ["Contracted (Firm)", neg_name]
        cmap = build_unique_color_map(names, fixed={"Contracted (Firm)": "#5459AC"})

        # Replace your two bar traces with these:
        fig_cn.add_trace(go.Bar(
            x=xs_union,
            y=[firm_map.get(x, 0.0) for x in xs_union],
            name="Contracted (Firm)",
            marker_color=cmap["Contracted (Firm)"],
        ))
        fig_cn.add_trace(go.Bar(
            x=xs_union,
            y=[neg_map.get(x, 0.0) for x in xs_union],
            name=neg_name,
            marker_color=cmap[neg_name],
        ))

        # (keep your existing monthly supply lines and layout below this)
        fig_cn.add_trace(go.Scatter(x=xs_union, y=[p50m.get(x, 0.0) for x in xs_union],
                                    mode="lines", name="Supply P50"))
        fig_cn.add_trace(go.Scatter(x=xs_union, y=[p90m.get(x, 0.0) for x in xs_union],
                                    mode="lines", name="Supply P90"))
        # ... after-plan overlays (if any) ...
        fig_cn.update_layout(
            barmode="stack",
            title=f"Contracted + Negotiations (+ Supply P50/P90) — monthly [{unit.upper()}]",
            xaxis_title="Month", yaxis_title=unit.upper()
        )


    # ============================
    #  Tables
    # ============================
    tbl_s = _fetch("/api/supply", {
        "region": region_param,
        "scenario": (scenario if scenario != "Both" else "P50"),
        "year_from": yf, "year_to": yt
    })
    s_cols = [{"name": k, "id": k} for k in (tbl_s[0].keys() if tbl_s else
               ["source", "source_type", "region", "scenario", "year", "mtpa"])]

    tbl_o = _fetch("/api/opportunities", {"region": region_param, "year_from": yf, "year_to": yt})
    o_cols = [{"name": k, "id": k} for k in (tbl_o[0].keys() if tbl_o else
               ["contract_name", "counterparty", "status", "year", "mtpa"])]

    for _f in (fig_s, fig_sa, fig_su, fig_su_abs, fig_su_pct, fig_o, fig_cn):
        apply_grey_theme(_f)    

    #return fig_s, fig_s_stacked, fig_o, fig_cn, fig_g, tbl_s, s_cols, tbl_o, o_cols
    return (
        fig_cn,           # contracted_neg_chart
        fig_su_pct,       # sold_uncommitted_pct_chart (100% stacked + strip)
        fig_s,            # supply_chart
        fig_sa,           # supply_chart_area
        fig_su,           # sold_uncommitted_chart (with % labels)
        fig_su_abs,       # NEW: absolute (no % labels)
        #fig_su_pct,       # sold_uncommitted_pct_chart (100% stacked + strip)
        fig_o,            # opportunity_chart
        #fig_cn,           # contracted_neg_chart
        #fig_g,            # gap_chart
        tbl_s, s_cols,
        tbl_o, o_cols,
    )

#Callback (conversion to CSV)
@dash_app.callback(
    Output("dl_chart_csv", "data"),
    Input("btn_export", "n_clicks"),
    State("export_chart_pick", "value"),
    # figures to export
    State("supply_chart", "figure"),
    State("supply_chart_area", "figure"),
    State("sold_uncommitted_chart", "figure"),
    State("sold_uncommitted_abs_chart", "figure"),
    State("sold_uncommitted_pct_chart", "figure"),
    State("opportunity_chart", "figure"),
    State("contracted_neg_chart", "figure"),
    State("source_focus_chart", "figure"),
    State("counterparty_focus_chart", "figure"),
    prevent_initial_call=True
)
def export_chart_to_csv(n_clicks, which,
                        fig_supply, fig_supply_area,
                        fig_sold_pct, fig_sold_abs, fig_sold_pct_only,
                        fig_opp, fig_contract,
                        fig_source_focus, fig_counterparty_focus):
    from dash import dcc
    from dash.exceptions import PreventUpdate
    from datetime import datetime
    import csv, io, re

    fig_map = {
        "supply_chart":               fig_supply,
        "supply_chart_area":          fig_supply_area,
        "sold_uncommitted_chart":     fig_sold_pct,
        "sold_uncommitted_abs_chart": fig_sold_abs,
        "sold_uncommitted_pct_chart": fig_sold_pct_only,
        "opportunity_chart":          fig_opp,
        "contracted_neg_chart":       fig_contract,
        "source_focus_chart":         fig_source_focus,
        "counterparty_focus_chart":   fig_counterparty_focus,
    }
    fig = fig_map.get(which) or {}
    traces = fig.get("data") or []
    if not traces:
        raise PreventUpdate

    # collect all x values in display order
    keys, seen = [], set()
    for tr in traces:
        for x in tr.get("x") or []:
            if x not in seen:
                seen.add(x); keys.append(x)

    # sort if they look like years or YYYY-MM-DD
    def yearlike(v): s=str(v); return s.isdigit() and len(s)==4
    def ymdlike(v): return isinstance(v, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", v)
    if keys and all(yearlike(k) for k in keys):
        keys = sorted(keys, key=lambda v: int(float(v)))
    elif keys and all(ymdlike(k) for k in keys):
        keys = sorted(keys)

    headers = ["period"] + [(tr.get("name") or f"trace_{i+1}") for i, tr in enumerate(traces)]
    series_maps = [{x: y for x, y in zip(tr.get("x") or [], tr.get("y") or [])} for tr in traces]

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    for k in keys:
        w.writerow([k] + [m.get(k, "") for m in series_maps])

    fname = f"{which}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    #return dcc.send_string(lambda: buf.getvalue(), fname)
    return dcc.send_string(buf.getvalue(), filename=fname, type="text/csv")


# ---- DRAG: client-side callback (place at module level) ---
'''dash_app.clientside_callback(
    """
    function(evt, pos, drag, is_open, baseStyle) {
        if (!is_open) {
            return [
                window.dash_clientside.no_update,
                window.dash_clientside.no_update,
                window.dash_clientside.no_update
            ];
        }

        var x = (pos && pos.x !== undefined) ? pos.x : 120;
        var y = (pos && pos.y !== undefined) ? pos.y : 80;
        var dragging = (drag && drag.dragging) ? true : false;

        if (typeof window._modal_last === "undefined") {
            window._modal_last = null;
        }

        if (evt && evt.type) {
            var targetId = evt["target.id"];
            if (evt.type === "mousedown") {
                if (targetId === "manage_drag_handle" || targetId === "manage_card") {
                    dragging = true;
                    window._modal_last = {x: evt.clientX, y: evt.clientY};
                    document.body.style.userSelect = "none";
                }
            } else if (evt.type === "mousemove" && dragging && window._modal_last) {
                var dx = evt.clientX - window._modal_last.x;
                var dy = evt.clientY - window._modal_last.y;
                x += dx; y += dy;
                window._modal_last = {x: evt.clientX, y: evt.clientY};
            } else if (evt.type === "mouseup") {
                dragging = false;
                window._modal_last = null;
                document.body.style.userSelect = "";
            }
        } the clientside_callback

        var w = (typeof window !== "undefined") ? window.innerWidth : 1200;
        var h = (typeof window !== "undefined") ? window.innerHeight : 800;
        x = Math.max(10, Math.min(x, w - 40));
        y = Math.max(10, Math.min(y, h - 40));

        var style = Object.assign({}, baseStyle || {}, {
            position: "fixed",
            left: x + "px",
            top: y + "px"
        });

        return [style, {x:x, y:y}, {dragging: dragging}];
    }
    """,
    [  # Outputs (positional)
        Output("manage_card", "style"),
        Output("modal_pos", "data"),
        Output("drag_state", "data"),
    ],
    [  # Inputs (positional)
        Input("drag_listener", "event"),
    ],
    [  # States (positional)
        State("modal_pos", "data"),
        State("drag_state", "data"),
        State("manage_open", "data"),
        State("manage_card", "style"),
    ],
)'''

app.mount("/dash", WSGIMiddleware(dash_app.server))

from app.db import engine
print("API DB:", engine.url)