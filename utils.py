
import json
from typing import Optional, List, Tuple
from .config import DEFAULT_GHV_MMBTU_PER_TONNE, DEFAULT_CARGO_MMBTU

def to_float(x, default=None):
    try:
        if x is None: return default
        return float(x)
    except Exception:
        return default

def normalize_units(original_unit: str, original_value: float,
                    ghv: Optional[float], cargo_mmbtu: Optional[float], cargo_tonnes: Optional[float]) -> Tuple[float,float,float]:
    ghv = to_float(ghv, DEFAULT_GHV_MMBTU_PER_TONNE)
    cargo_mmbtu = to_float(cargo_mmbtu, DEFAULT_CARGO_MMBTU)
    cargo_tonnes = to_float(cargo_tonnes, None)
    mtpa = mmbtu = cargoes = None

    if original_unit == "mtpa":
        mtpa = float(original_value)
        mmbtu = mtpa * 1_000_000.0 * ghv
        cargoes = mmbtu / cargo_mmbtu if cargo_mmbtu else None
    elif original_unit == "mmbtu":
        mmbtu = float(original_value)
        mtpa = (mmbtu / ghv) / 1_000_000.0 if ghv else None
        cargoes = mmbtu / cargo_mmbtu if cargo_mmbtu else None
    elif original_unit == "cargoes":
        cargoes = float(original_value)
        if cargo_mmbtu:
            mmbtu = cargoes * cargo_mmbtu
            mtpa = (mmbtu / ghv) / 1_000_000.0 if ghv else None
        elif cargo_tonnes:
            mtpa = (cargoes * cargo_tonnes) / 1_000_000.0
            mmbtu = mtpa * 1_000_000.0 * ghv if ghv else None
        else:
            mmbtu = cargoes * DEFAULT_CARGO_MMBTU
            mtpa = (mmbtu / ghv) / 1_000_000.0 if ghv else None
    else:
        raise ValueError("original_unit must be one of: mtpa | mmbtu | cargoes")

    if mtpa is None and mmbtu is not None and ghv:
        mtpa = (mmbtu / ghv) / 1_000_000.0
    if mmbtu is None and mtpa is not None and ghv:
        mmbtu = mtpa * 1_000_000.0 * ghv
    if cargoes is None and mmbtu is not None and cargo_mmbtu:
        cargoes = mmbtu / cargo_mmbtu

    return float(mtpa or 0.0), float(mmbtu or 0.0), float(cargoes or 0.0)

def month_weights(start_month: Optional[int], months_active: Optional[int], profile: Optional[str], profile_json: Optional[str]) -> List[float]:
    sm = int(start_month or 1)
    ma = int(months_active or 12)
    if sm < 1: sm = 1
    if sm > 12: sm = 12
    if ma < 1: ma = 1
    if ma > 12: ma = 12
    last = min(12, sm + ma - 1)
    weights = [0.0]*12
    n = last - sm + 1

    if (profile or "flat") == "flat":
        w = 1.0 / n
        for i in range(sm-1, last): weights[i] = w
    else:
        # simplify for seeded build, other profiles default to flat
        w = 1.0 / n
        for i in range(sm-1, last): weights[i] = w

    ssum = sum(weights)
    if ssum > 0 and abs(ssum-1.0) > 1e-9:
        weights = [w/ssum for w in weights]
    return weights
