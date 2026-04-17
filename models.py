
from sqlalchemy import Column, Integer, String, Float, UniqueConstraint
from .db import Base

class Supply(Base):
    __tablename__ = "supply"
    id = Column(Integer, primary_key=True)
    source = Column(String(128), index=True)
    source_type = Column(String(32), index=True)
    region = Column(String(32), index=True)
    scenario = Column(String(32), index=True)
    year = Column(Integer, index=True)
    start_month = Column(Integer, nullable=True)
    months_active = Column(Integer, nullable=True)
    profile = Column(String(32), nullable=True)
    profile_weights_json = Column(String(1024), nullable=True)

    original_unit = Column(String(16))
    original_value = Column(Float)

    ghv_mmbtu_per_tonne = Column(Float, nullable=True)
    cargo_mmbtu = Column(Float, nullable=True)
    cargo_tonnes = Column(Float, nullable=True)

    mtpa = Column(Float)
    mmbtu = Column(Float)
    cargoes = Column(Float)

    equity_fraction = Column(Float, nullable=True, default=1.0)
    equity_mtpa = Column(Float)
    equity_mmbtu = Column(Float)
    equity_cargoes = Column(Float)

    notes = Column(String(512), nullable=True)

    status = Column(String(16), nullable=False, server_default="negotiation", default="negotiation", index=True)

    __table_args__ = (UniqueConstraint("source","source_type","region","scenario","year", name="uq_supply_key"),)

class Opportunity(Base):
    __tablename__ = "opportunity"
    id = Column(Integer, primary_key=True)
    contract_name = Column(String(128), index=True)
    counterparty = Column(String(128), index=True)
    status = Column(String(16), index=True)        # firm | negotiation | option
    fob_des = Column(String(8), nullable=True)     # FOB | DES
    pricing_index = Column(String(32), nullable=True)
    region = Column(String(32), index=True)
    year = Column(Integer, index=True)
    start_month = Column(Integer, nullable=True)
    months_active = Column(Integer, nullable=True)
    profile = Column(String(32), nullable=True)
    profile_weights_json = Column(String(1024), nullable=True)

    original_unit = Column(String(16))
    original_value = Column(Float)

    ghv_mmbtu_per_tonne = Column(Float, nullable=True)
    cargo_mmbtu = Column(Float, nullable=True)
    cargo_tonnes = Column(Float, nullable=True)

    mtpa = Column(Float)
    mmbtu = Column(Float)
    cargoes = Column(Float)

    probability = Column(Float, nullable=True)
    notes = Column(String(512), nullable=True)

    __table_args__ = (UniqueConstraint("contract_name","counterparty","year", name="uq_opportunity_key"),)

class SupplyMonthly(Base):
    __tablename__ = "supply_monthly"
    id = Column(Integer, primary_key=True)
    source = Column(String(128), index=True)
    source_type = Column(String(32), index=True)
    region = Column(String(32), index=True)
    scenario = Column(String(32), index=True)
    year = Column(Integer, index=True)
    month = Column(Integer, index=True)
    value_mtpa = Column(Float)
    value_mmbtu = Column(Float)
    value_cargoes = Column(Float)
    equity_value_mtpa = Column(Float)
    equity_value_mmbtu = Column(Float)
    equity_value_cargoes = Column(Float)

class OpportunityMonthly(Base):
    __tablename__ = "opportunity_monthly"
    id = Column(Integer, primary_key=True)
    contract_name = Column(String(128), index=True)
    counterparty = Column(String(128), index=True)
    status = Column(String(16), index=True)
    region = Column(String(32), index=True)
    year = Column(Integer, index=True)
    month = Column(Integer, index=True)
    value_mtpa = Column(Float)
    value_mmbtu = Column(Float)
    value_cargoes = Column(Float)
    probability = Column(Float, nullable=True)
