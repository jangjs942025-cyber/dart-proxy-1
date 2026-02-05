# main.py
# FastAPI single-endpoint proxy for OpenDART:
# - op=resolve   : stock_code(6) -> corp_code(8) + company(acc_mt)
# - op=list      : regular filings list (A) for target selection
# - op=fnltt_all : fnlttSinglAcntAll paged (cursor/limit) with server-side cache
#
# ⚠️ 절대 주의:
# - OPENDART_API_KEY는 GitHub에 올리지 말고 Render "Environment"에만 넣어라.
# - (선택) PROXY_BEARER_TOKEN을 설정하면 외부 호출을 토큰으로 막는다.

import os
import io
import zipfile
import datetime as dt
import xml.etree.ElementTree as ET
from typing import Dict, Any, Optional, List

import httpx
from fastapi import FastAPI, HTTPException, Header


# ====== ENV ======
OPENDART_API_KEY = (os.getenv("OPENDART_API_KEY") or "").strip()
if not OPENDART_API_KEY:
    raise RuntimeError("Missing OPENDART_API_KEY env var (set it in Render Environment).")

# optional: protect endpoint (recommended)
PROXY_BEARER_TOKEN = (os.getenv("PROXY_BEARER_TOKEN") or "").strip()


# ====== APP ======
app = FastAPI(title="DART Proxy (Single Endpoint)", version="1.0.0")


# ====== CONSTANTS ======
OPENDART_BASE = "https://opendart.fss.or.kr"
CORP_MAP_TTL_SEC = 24 * 60 * 60          # refresh corpCode.zip daily
FS_CACHE_TTL_SEC = 6 * 60 * 60           # cache fnlttSinglAcntAll 6 hours
HTTP_TIMEOUT = httpx.Timeout(30.0)

# prevent paging abuse (Render free tier / Actions constraints)
MAX_LIMIT = 500
MIN_LIMIT = 50
MAX_CURSOR_PAGES_HINT = 12  # client should stop around this many pages per report


# ====== IN-MEMORY CACHES ======
# stock_code(6) -> {corp_code, corp_name, stock_code, modify_date}
CORP_MAP: Dict[str, Dict[str, str]] = {}
CORP_MAP_TS: Optional[dt.datetime] = None

# key = "corp_code:bsns_year:reprt_code:fs_div" -> full JSON from fnlttSinglAcntAll
FS_CACHE: Dict[str, Dict[str, Any]] = {}
FS_CACHE_TS: Dict[str, dt.datetime] = {}


# ====== HELPERS ======
def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _today_yyyymmdd() -> str:
    return dt.date.today().strftime("%Y%m%d")


def _years_ago_yyyymmdd(years: int) -> str:
    d = dt.date.today() - dt.timedelta(days=365 * years)
    return d.strftime("%Y%m%d")


def _fs_key(corp_code: str, bsns_year: str, reprt_code: str, fs_div: str) -> str:
    return f"{corp_code}:{bsns_year}:{reprt_code}:{fs_div}"


def _require_token(authorization: Optional[str]) -> None:
    """Optional Bearer auth. If PROXY_BEARER_TOKEN is set, enforce it."""
    if not PROXY_BEARER_TOKEN:
        return
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization: Bearer <token>")
    token = authorization.split(" ", 1)[1].strip()
    if token != PROXY_BEARER_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid bearer token")


async def _get_bytes(path: str, params: Dict[str, str]) -> bytes:
    url = f"{OPENDART_BASE}{path}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.content


async def _get_json(path: str, params: Dict[str, str]) -> Dict[str, Any]:
    url = f"{OPENDART_BASE}{path}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json()


async def _refresh_corp_map_if_needed() -> None:
    """Download corpCode.xml(zip) and build stock_code -> corp_code map."""
    global CORP_MAP, CORP_MAP_TS

    now = _utcnow()
    if CORP_MAP_TS and (now - CORP_MAP_TS).total_seconds() < CORP_MAP_TTL_SEC and CORP_MAP:
        return

    zip_bytes = await _get_bytes("/api/corpCode.xml", {"crtfc_key": OPENDART_API_KEY})
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))

    xml_name = next((n for n in zf.namelist() if n.lower().endswith(".xml")), None)
    if not xml_name:
        raise RuntimeError("corpCode zip has no XML file")

    xml_data = zf.read(xml_name)
    root = ET.fromstring(xml_data)

    m: Dict[str, Dict[str, str]] = {}
    # CORPCODE.xml contains many <list> nodes
    for node in root.findall(".//list"):
        corp_code = (node.findtext("corp_code") or "").strip()
        corp_name = (node.findtext("corp_name") or "").strip()
        stock_code = (node.findtext("stock_code") or "").strip()
        modify_date = (node.findtext("modify_date") or "").strip()

        if len(stock_code) == 6 and len(corp_code) == 8:
            m[stock_code] = {
                "corp_code": corp_code,
                "corp_name": corp_name,
                "stock_code": stock_code,
                "modify_date": modify_date,
            }

    if not m:
        raise RuntimeError("corpCode parse produced empty mapping")

    CORP_MAP = m
    CORP_MAP_TS = now


async def _resolve_stock_code(stock_code: str) -> Dict[str, Any]:
    await _refresh_corp_map_if_needed()

    info = CORP_MAP.get(stock_code)
    if not info:
        raise HTTPException(status_code=404, detail=f"Unknown stock_code: {stock_code}")

    # company.json to get acc_mt etc.
    comp = await _get_json(
        "/api/company.json",
        {"crtfc_key": OPENDART_API_KEY, "corp_code": info["corp_code"]},
    )

    return {
        "op": "resolve",
        "status": "OK",
        "message": "",
        "stock_code": stock_code,
        "corp_code": info["corp_code"],
        "corp_name": info["corp_name"],
        "modify_date": info["modify_date"],
        "acc_mt": comp.get("acc_mt"),
        "company_status": comp.get("status"),
        "company_message": comp.get("message"),
    }


async def _list_regular_filings(stock_code: str, lookback_years: int) -> Dict[str, Any]:
    resolved = await _resolve_stock_code(stock_code)
    corp_code = resolved["corp_code"]

    lookback_years = max(3, min(10, int(lookback_years)))

    params = {
        "crtfc_key": OPENDART_API_KEY,
        "corp_code": corp_code,
        "bgn_de": _years_ago_yyyymmdd(lookback_years),
        "end_de": _today_yyyymmdd(),
        "last_reprt_at": "Y",   # prefer latest corrected if exists
        "pblntf_ty": "A",       # regular disclosures
        "sort": "date",
        "sort_mth": "desc",
        "page_no": "1",
        "page_count": "100",
    }
    data = await _get_json("/api/list.json", params)

    # shrink payload: keep only common fields
    items: List[Dict[str, Any]] = []
    for it in (data.get("list") or []):
        items.append(
            {
                "rcept_no": it.get("rcept_no"),
                "rcept_dt": it.get("rcept_dt"),
                "report_nm": it.get("report_nm"),
                "corp_name": it.get("corp_name"),
                "stock_code": it.get("stock_code"),
                "bsns_year": it.get("bsns_year"),
                "pblntf_ty": it.get("pblntf_ty"),
                "pblntf_detail_ty": it.get("pblntf_detail_ty"),
            }
        )

    return {
        "op": "list",
        "status": str(data.get("status")),
        "message": str(data.get("message")),
        "stock_code": stock_code,
        "corp_code": corp_code,
        "lookback_years": lookback_years,
        "items": items,
    }


async def _fnltt_all_page(
    stock_code: str,
    bsns_year: str,
    reprt_code: str,
    fs_div: str,
    cursor: int,
    limit: int,
) -> Dict[str, Any]:
    resolved = await _resolve_stock_code(stock_code)
    corp_code = resolved["corp_code"]

    key = _fs_key(corp_code, bsns_year, reprt_code, fs_div)
    now = _utcnow()

    cached = FS_CACHE.get(key)
    ts = FS_CACHE_TS.get(key)
    if not cached or not ts or (now - ts).total_seconds() > FS_CACHE_TTL_SEC:
        cached = await _get_json(
            "/api/fnlttSinglAcntAll.json",
            {
                "crtfc_key": OPENDART_API_KEY,
                "corp_code": corp_code,
                "bsns_year": bsns_year,
                "reprt_code": reprt_code,
                "fs_div": fs_div,
            },
        )
        FS_CACHE[key] = cached
        FS_CACHE_TS[key] = now

    rows = cached.get("list") or []
    total_rows = len(rows)

    end = min(cursor + limit, total_rows)
    page = rows[cursor:end]
    next_cursor = end if end < total_rows else None

    return {
        "op": "fnltt_all",
        "status": str(cached.get("status")),
        "message": str(cached.get("message")),
        "hint": f"Client should avoid >{MAX_CURSOR_PAGES_HINT} pages per report to prevent abuse/timeouts.",
        "stock_code": stock_code,
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "reprt_code": reprt_code,
        "fs_div": fs_div,
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total_rows": total_rows,
        "list": page,
    }


# ====== ROUTES ======
@app.get("/healthz")
async def healthz():
    return {"ok": True, "ts": _utcnow().isoformat()}


@app.get("/v1/dart")
async def dart(
    op: str,
    stock_code: str,
    lookback_years: int = 6,
    bsns_year: Optional[str] = None,
    reprt_code: Optional[str] = None,
    fs_div: str = "CFS",
    cursor: int = 0,
    limit: int = 200,
    authorization: Optional[str] = Header(default=None),
):
    # optional bearer auth
    _require_token(authorization)

    # basic validations
    if op not in ("resolve", "list", "fnltt_all"):
        raise HTTPException(status_code=400, detail="op must be resolve | list | fnltt_all")

    if not (stock_code.isdigit() and len(stock_code) == 6):
        raise HTTPException(status_code=400, detail="stock_code must be 6 digits")

    fs_div = fs_div.strip().upper()
    if fs_div not in ("CFS", "OFS"):
        raise HTTPException(status_code=400, detail="fs_div must be CFS or OFS")

    if cursor < 0:
        raise HTTPException(status_code=400, detail="cursor must be >= 0")

    if limit < MIN_LIMIT or limit > MAX_LIMIT:
        raise HTTPException(status_code=400, detail=f"limit must be {MIN_LIMIT}..{MAX_LIMIT}")

    if op == "resolve":
        return await _resolve_stock_code(stock_code)

    if op == "list":
        return await _list_regular_filings(stock_code, lookback_years)

    # op == fnltt_all
    if not bsns_year or not (bsns_year.isdigit() and len(bsns_year) == 4):
        raise HTTPException(status_code=400, detail="bsns_year(YYYY) required for op=fnltt_all")

    if not reprt_code or reprt_code not in ("11011", "11012", "11013", "11014"):
        raise HTTPException(
            status_code=400,
            detail="reprt_code required for op=fnltt_all (11011/11012/11013/11014)",
        )

    return await _fnltt_all_page(stock_code, bsns_year, reprt_code, fs_div, cursor, limit)
