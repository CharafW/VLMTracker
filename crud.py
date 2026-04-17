
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, delete
from .models import Supply, Opportunity, SupplyMonthly, OpportunityMonthly
from .utils import normalize_units, month_weights

def create_supply(db: Session, payload: dict) -> Supply:
    mtpa, mmbtu, cargoes = normalize_units(
        payload.get("original_unit"), payload.get("original_value"),
        payload.get("ghv_mmbtu_per_tonne"), payload.get("cargo_mmbtu"), payload.get("cargo_tonnes")
    )
    eq = payload.get("equity_fraction")
    try:
        eq = 1.0 if eq is None else float(eq)
    except Exception:
        eq = 1.0
    eq = 0.0 if eq < 0 else (1.0 if eq > 1.0 else eq)

    row = Supply(
        source=payload.get("source"),
        source_type=payload.get("source_type","production_asset"),
        region=payload.get("region","Global"),
        scenario=payload.get("scenario","P50"),
        status=payload.get("status","negotiation"),
        year=int(payload.get("year")),
        start_month=payload.get("start_month",1),
        months_active=payload.get("months_active",12),
        profile=payload.get("profile","flat"),
        profile_weights_json=payload.get("profile_weights_json"),
        original_unit=payload.get("original_unit","mtpa"),
        original_value=float(payload.get("original_value")),
        ghv_mmbtu_per_tonne=payload.get("ghv_mmbtu_per_tonne"),
        cargo_mmbtu=payload.get("cargo_mmbtu"),
        cargo_tonnes=payload.get("cargo_tonnes"),
        mtpa=mtpa, mmbtu=mmbtu, cargoes=cargoes,
        equity_fraction=eq,
        equity_mtpa=(mtpa or 0.0) * eq,
        equity_mmbtu=(mmbtu or 0.0) * eq,
        equity_cargoes=(cargoes or 0.0) * eq,
        notes=payload.get("notes"),
    )
    db.add(row)
    db.flush()
    _refresh_supply_monthly_for(db, row)
    db.commit(); db.refresh(row)
    return row

def _refresh_supply_monthly_for(db: Session, row: Supply):
    db.execute(delete(SupplyMonthly).where(
        SupplyMonthly.source==row.source,
        SupplyMonthly.source_type==row.source_type,
        SupplyMonthly.region==row.region,
        SupplyMonthly.scenario==row.scenario,
        SupplyMonthly.year==row.year,
    ))
    weights = month_weights(row.start_month, row.months_active, row.profile, row.profile_weights_json)
    for m in range(1,13):
        w = weights[m-1]
        db.add(SupplyMonthly(
            source=row.source, source_type=row.source_type, region=row.region, scenario=row.scenario,
            year=row.year, month=m,
            value_mtpa=(row.mtpa or 0.0)*w,
            value_mmbtu=(row.mmbtu or 0.0)*w,
            value_cargoes=(row.cargoes or 0.0)*w,
            equity_value_mtpa=(row.equity_mtpa or 0.0)*w,
            equity_value_mmbtu=(row.equity_mmbtu or 0.0)*w,
            equity_value_cargoes=(row.equity_cargoes or 0.0)*w,
        ))

def list_supply(db: Session, source: Optional[str], source_type: Optional[str], region: Optional[str], scenario: Optional[str],
                year_from: Optional[int], year_to: Optional[int]):
    stmt = select(Supply)
    if source: stmt = stmt.where(Supply.source==source)
    if source_type: stmt = stmt.where(Supply.source_type==source_type)
    if region: stmt = stmt.where(Supply.region==region)
    if scenario: stmt = stmt.where(Supply.scenario==scenario)
    if year_from is not None: stmt = stmt.where(Supply.year >= year_from)
    if year_to is not None: stmt = stmt.where(Supply.year <= year_to)
    stmt = stmt.order_by(Supply.source.asc(), Supply.year.asc())
    return db.execute(stmt).scalars().all()

def create_opportunity(db: Session, payload: dict) -> Opportunity:
    mtpa, mmbtu, cargoes = normalize_units(
        payload.get("original_unit"), payload.get("original_value"),
        payload.get("ghv_mmbtu_per_tonne"), payload.get("cargo_mmbtu"), payload.get("cargo_tonnes")
    )
    row = Opportunity(
        contract_name=payload.get("contract_name"),
        counterparty=payload.get("counterparty"),
        status=payload.get("status","negotiation"),
        fob_des=payload.get("fob_des"),
        pricing_index=payload.get("pricing_index"),
        region=payload.get("region","Global"),
        year=int(payload.get("year")),
        start_month=payload.get("start_month",1),
        months_active=payload.get("months_active",12),
        profile=payload.get("profile","flat"),
        profile_weights_json=payload.get("profile_weights_json"),
        original_unit=payload.get("original_unit","mtpa"),
        original_value=float(payload.get("original_value")),
        ghv_mmbtu_per_tonne=payload.get("ghv_mmbtu_per_tonne"),
        cargo_mmbtu=payload.get("cargo_mmbtu"),
        cargo_tonnes=payload.get("cargo_tonnes"),
        mtpa=mtpa, mmbtu=mmbtu, cargoes=cargoes,
        probability=payload.get("probability"),
        notes=payload.get("notes"),
    )
    db.add(row)
    db.flush()
    _refresh_opportunity_monthly_for(db, row)
    db.commit(); db.refresh(row)
    return row

def _refresh_opportunity_monthly_for(db: Session, row: Opportunity):
    db.execute(delete(OpportunityMonthly).where(
        OpportunityMonthly.contract_name==row.contract_name,
        OpportunityMonthly.counterparty==row.counterparty,
        OpportunityMonthly.year==row.year,
    ))
    weights = month_weights(row.start_month, row.months_active, row.profile, row.profile_weights_json)
    for m in range(1,13):
        w = weights[m-1]
        db.add(OpportunityMonthly(
            contract_name=row.contract_name, counterparty=row.counterparty, status=row.status, region=row.region,
            year=row.year, month=m,
            value_mtpa=(row.mtpa or 0.0)*w,
            value_mmbtu=(row.mmbtu or 0.0)*w,
            value_cargoes=(row.cargoes or 0.0)*w,
            probability=row.probability
        ))

def list_opportunities(db: Session, counterparty: Optional[str], status: Optional[str], region: Optional[str],
                       fob_des: Optional[str], pricing_index: Optional[str],
                       year_from: Optional[int], year_to: Optional[int]):
    stmt = select(Opportunity)
    if counterparty: stmt = stmt.where(Opportunity.counterparty==counterparty)
    if status: stmt = stmt.where(Opportunity.status==status)
    if region: stmt = stmt.where(Opportunity.region==region)
    if fob_des: stmt = stmt.where(Opportunity.fob_des==fob_des)
    if pricing_index: stmt = stmt.where(Opportunity.pricing_index==pricing_index)
    if year_from is not None: stmt = stmt.where(Opportunity.year >= year_from)
    if year_to is not None: stmt = stmt.where(Opportunity.year <= year_to)
    stmt = stmt.order_by(Opportunity.counterparty.asc(), Opportunity.year.asc())
    return db.execute(stmt).scalars().all()

def get_supply_monthly(db: Session, unit: str, scenario: Optional[str], source: Optional[str],
                       source_type: Optional[str], region: Optional[str], year_from: Optional[int], year_to: Optional[int],
                       basis: str = "equity"):
    from sqlalchemy import select
    S = SupplyMonthly
    stmt = select(S)
    if scenario: stmt = stmt.where(S.scenario==scenario)
    if source: stmt = stmt.where(S.source==source)
    if source_type: stmt = stmt.where(S.source_type==source_type)
    if region: stmt = stmt.where(S.region==region)
    if year_from is not None: stmt = stmt.where(S.year >= year_from)
    if year_to is not None: stmt = stmt.where(S.year <= year_to)
    rows = db.execute(stmt.order_by(S.year.asc(), S.month.asc())).scalars().all()
    out = []
    for r in rows:
        if basis == "equity":
            val = r.equity_value_mtpa if unit=="mtpa" else (r.equity_value_mmbtu if unit=="mmbtu" else r.equity_value_cargoes)
        else:
            val = r.value_mtpa if unit=="mtpa" else (r.value_mmbtu if unit=="mmbtu" else r.value_cargoes)
        out.append({"date": f"{r.year:04d}-{r.month:02d}-01", "value": float(val or 0.0),
                    "scenario": r.scenario, "source": r.source, "region": r.region, "source_type": r.source_type})
    return out

def get_opportunity_monthly(db: Session, unit: str, status: Optional[str], counterparty: Optional[str],
                            region: Optional[str], year_from: Optional[int], year_to: Optional[int],
                            probability_weighted: bool):
    from sqlalchemy import select
    O = OpportunityMonthly
    stmt = select(O)
    if status: stmt = stmt.where(O.status==status)
    if counterparty: stmt = stmt.where(O.counterparty==counterparty)
    if region: stmt = stmt.where(O.region==region)
    if year_from is not None: stmt = stmt.where(O.year >= year_from)
    if year_to is not None: stmt = stmt.where(O.year <= year_to)
    rows = db.execute(stmt.order_by(O.year.asc(), O.month.asc())).scalars().all()
    out = []
    for r in rows:
        raw = r.value_mtpa if unit=="mtpa" else (r.value_mmbtu if unit=="mmbtu" else r.value_cargoes)
        if probability_weighted and r.status != "firm":
            p = r.probability if r.probability is not None else 0.0
            raw = (raw or 0.0) * p
        out.append({"date": f"{r.year:04d}-{r.month:02d}-01", "value": float(raw or 0.0),
                    "status": r.status, "counterparty": r.counterparty, "region": r.region})
    return out

def get_gap_monthly(db: Session, unit: str, scenario: Optional[str], region: Optional[str],
                    year_from: Optional[int], year_to: Optional[int], probability_weighted: bool, basis: str = "equity"):
    s = get_supply_monthly(db, unit, scenario, None, None, region, year_from, year_to, basis=basis)
    firm = get_opportunity_monthly(db, unit, "firm", None, region, year_from, year_to, False)
    potl = get_opportunity_monthly(db, unit, None, None, region, year_from, year_to, probability_weighted)

    from collections import defaultdict
    acc_s, acc_f, acc_p = defaultdict(float), defaultdict(float), defaultdict(float)
    for r in s: acc_s[r["date"]] += r["value"]
    for r in firm: acc_f[r["date"]] += r["value"]
    for r in potl: acc_p[r["date"]] += r["value"]

    out = []
    for d in sorted(set(list(acc_s.keys()) + list(acc_f.keys()) + list(acc_p.keys()))):
        supply_v = acc_s.get(d, 0.0)
        firm_v = acc_f.get(d, 0.0)
        potl_v = acc_p.get(d, 0.0)
        gap = supply_v - firm_v
        potl_gap = supply_v - potl_v if probability_weighted else None
        out.append({"date": d, "supply": supply_v, "firm": firm_v, "gap": gap, "potential_gap": potl_gap})
    return out
