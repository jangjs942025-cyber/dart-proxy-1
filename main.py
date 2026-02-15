# main.py
# FastAPI single-endpoint proxy for OpenDART (ANNUAL-ONLY):
# - op=resolve   : stock_code(6) -> corp_code(8) + company(acc_mt)
# - op=list      : regular filings list (A) BUT locked to annual business report (A001)
# - op=fnltt_all : fnlttSinglAcntAll paged (cursor/limit) with server-side cache
#
# Scope Lock (강제):
# - op=fnltt_all 은 "III. 재무에 관한 사항" 범위만 데이터 확보 대상으로 인정한다.
#   - 2. 연결재무제표 5종: BS/IS/CIS/SCE/CF 만 반환(서버 단 필터)
#   - fs_div는 CFS(연결)로 고정(오염 방지)
# - 연간(사업보고서)만 허용:
#   - list.json: pblntf_detail_ty = A001 (사업보고서)
#   - fnltt_all: reprt_code = 11011 (사업보고서/연간)만 허용
# - 1. 요약재무정보는 별도 표 크롤링이 아니라, 위 5종에서 파생/집계하는 것으로 정의한다(수집기/분석기 레벨에서 처리).
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
from fastapi import FastAPI, HTTPException, Header, Query


# ====== ENV ======
OPENDART_API_KEY = (os.getenv("OPENDART_API_KEY") or "").strip()
if not OPENDART_API_KEY:
    raise RuntimeError("Missing OPENDART_API_KEY env var (set it in Render Environment).")

# optional: protect endpoint (recommended)
PROXY_BEARER_TOKEN = (os.getenv("PROXY_BEARER_TOKEN") or "").strip()


# ====== APP ======
app = FastAPI(title="DART Proxy (Single Endpoint, Annual-Only)", version="1.0.4")


# ====== CONSTANTS ======
OPENDART_BASE = "https://opendart.fss.or.kr"
CORP_MAP_TTL_SEC = 24 * 60 * 60          # refresh corpCode.zip daily
FS_CACHE_TTL_SEC = 6 * 60 * 60           # cache fnlttSinglAcntAll 6 hours
HTTP_TIMEOUT = httpx.Timeout(30.0)

# prevent paging abuse (Render free tier / Actions constraints)
MAX_LIMIT = 500
MIN_LIMIT = 50
MAX_CURSOR_PAGES_HINT = 12  # client should stop around this many pages per report

# III. 재무 스코프 락(연결재무제표 5종)
FS_DIV_LOCK = "CFS"
SJ_DIV_ALLOWLIST_DEFAULT = {"BS", "IS", "CIS", "SCE", "CF"}
REQUIRED_SJ_DIV = ["BS", "IS", "CIS", "SCE", "CF"]

# Annual-only lock
ANNUAL_REPRT_CODE_LOCK = "11011"   # 사업보고서(연간)
ANNUAL_PBLNTF_DETAIL_TY = "A001"   # 사업보고서


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


def _required_sj_set() -> List[str]:
    return REQUIRED_SJ_DIV


def _parse_sj_div_in(sj_div_in: Optional[str]) -> List[str]:
    """
    sj_div_in: "BS,IS,CIS,SCE,CF" 같은 콤마 구분 문자열.
    - 허용값만 필터.
    - 유효값이 하나도 없으면 기본 allowlist 적용(5종).
    """
    if not sj_div_in:
        return sorted(list(SJ_DIV_ALLOWLIST_DEFAULT))

    parsed = {x.strip().upper() for x in sj_div_in.split(",") if x.strip()}
    allow = parsed.intersection(SJ_DIV_ALLOWLIST_DEFAULT)
    if not allow:
        allow = SJ_DIV_ALLOWLIST_DEFAULT
    return sorted(list(allow))


def _parse_sj_div_requested(sj_div: Optional[str], sj_div_in: Optional[str]) -> List[str]:
    """
    requested: 클라이언트가 요청한 sj_div 목록(원형 보존).
    - sj_div(단건)가 있으면 그것이 requested.
    - sj_div_in(콤마)가 있으면 파싱한 목록이 requested.
    - 둘 다 없으면 REQUIRED(5종)가 requested.
    """
    if sj_div:
        return [sj_div.strip().upper()]
    if sj_div_in:
        parsed = [x.strip().upper() for x in sj_div_in.split(",") if x.strip()]
        return parsed if parsed else _required_sj_set()
    return _required_sj_set()


def _effective_sj_div(requested: List[str]) -> List[str]:
    """
    effective: 서버가 실제 적용한 sj_div(요청 ∩ 정책/락).
    - 요청값이 전부 비허용이면 기본 5종으로 회귀(안전).
    """
    allow = set(SJ_DIV_ALLOWLIST_DEFAULT)
    eff = sorted(list(set([x.upper() for x in requested]).intersection(allow)))
    return eff if eff else _required_sj_set()


def _missing_required_from_rows(rows: List[Dict[str, Any]]) -> List[str]:
    """
    missing: REQUIRED(5종) 대비 '실제 데이터'에 존재하지 않는 sj_div.
    - 요청값/필터링과 무관하게 OpenDART 원문 list에 무엇이 있었는지로 결측 판단.
    """
    present = {str(r.get("sj_div") or "").upper() for r in rows}
    present_required = present.intersection(set(REQUIRED_SJ_DIV))
    return sorted(list(set(REQUIRED_SJ_DIV) - present_required))


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


def _guard_dart_status(data: Dict[str, Any], *, allow_013_empty: bool = False) -> None:
    """
    OpenDART는 HTTP 200이어도 JSON status로 에러를 준다.
    - allow_013_empty=True이면 013(조회된 데이터 없음)은 빈 list로 허용한다.
    """
    st = str(data.get("status"))
    if st == "000":
        return
    if allow_013_empty and st == "013":
        return
    msg = str(data.get("message"))
    raise HTTPException(status_code=502, detail=f"OpenDART status={st} message={msg}")


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


async def _corp_from_stock_code(stock_code: str) -> Dict[str, str]:
    """corpCode.xml 매핑만 수행(= company.json 호출 없음)."""
    await _refresh_corp_map_if_needed()
    info = CORP_MAP.get(stock_code)
    if not info:
        raise HTTPException(status_code=404, detail=f"Unknown stock_code: {stock_code}")
    return info


async def _resolve_stock_code(stock_code: str) -> Dict[str, Any]:
    info = await _corp_from_stock_code(stock_code)

    # company.json to get acc_mt etc. (resolve에서만 호출)
    comp = await _get_json(
        "/api/company.json",
        {"crtfc_key": OPENDART_API_KEY, "corp_code": info["corp_code"]},
    )
    _guard_dart_status(comp, allow_013_empty=False)

    return {
        "op": "resolve",
        "status": str(comp.get("status")),
        "message": str(comp.get("message")),
        "stock_code": stock_code,
        "corp_code": info["corp_code"],
        "corp_name": info["corp_name"],
        "modify_date": info["modify_date"],
        "acc_mt": comp.get("acc_mt"),
        "company_status": comp.get("status"),
        "company_message": comp.get("message"),
    }


async def _list_annual_business_reports(stock_code: str, lookback_years: int) -> Dict[str, Any]:
    """
    Annual-only:
    - pblntf_ty="A"(정기공시)
    - pblntf_detail_ty="A001"(사업보고서) 고정
    """
    info = await _corp_from_stock_code(stock_code)
    corp_code = info["corp_code"]

    lookback_years = max(3, min(10, int(lookback_years)))

    params = {
        "crtfc_key": OPENDART_API_KEY,
        "corp_code": corp_code,
        "bgn_de": _years_ago_yyyymmdd(lookback_years),
        "end_de": _today_yyyymmdd(),
        "last_reprt_at": "Y",                 # prefer latest corrected if exists
        "pblntf_ty": "A",                     # regular disclosures
        "pblntf_detail_ty": ANNUAL_PBLNTF_DETAIL_TY,  # A001 only (사업보고서)
        "sort": "date",
        "sort_mth": "desc",
        "page_no": "1",
        "page_count": "100",                  # limit=100은 여기서 적용
    }
    data = await _get_json("/api/list.json", params)
    _guard_dart_status(data, allow_013_empty=False)

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
        "scope_lock_applied": {
            "pblntf_ty": "A",
            "pblntf_detail_ty": ANNUAL_PBLNTF_DETAIL_TY,
            "page_count": 100,
        },
        "stock_code": stock_code,
        "corp_code": corp_code,
        "lookback_years": lookback_years,
        "items": items,
    }


async def _fnltt_all_page(
    stock_code: str,
    bsns_year: str,
    cursor: int,
    limit: int,
    sj_div: Optional[str],
    sj_div_in: Optional[str],
) -> Dict[str, Any]:
    info = await _corp_from_stock_code(stock_code)
    corp_code = info["corp_code"]

    # locks
    fs_div = FS_DIV_LOCK
    reprt_code = ANNUAL_REPRT_CODE_LOCK  # annual only

    key = _fs_key(corp_code, bsns_year, reprt_code, fs_div)
    now = _utcnow()

    cached = FS_CACHE.get(key)
    ts = FS_CACHE_TS.get(key)

    if not cached or not ts or (now - ts).total_seconds() > FS_CACHE_TTL_SEC:
        fetched = await _get_json(
            "/api/fnlttSinglAcntAll.json",
            {
                "crtfc_key": OPENDART_API_KEY,
                "corp_code": corp_code,
                "bsns_year": bsns_year,
                "reprt_code": reprt_code,
                "fs_div": fs_div,
            },
        )

        # 013(조회된 데이터 없음)만 빈 list로 허용
        _guard_dart_status(fetched, allow_013_empty=True)
        if str(fetched.get("status")) == "013":
            fetched["list"] = []

        # 성공/013만 캐시 저장
        FS_CACHE[key] = fetched
        FS_CACHE_TS[key] = now
        cached = fetched

    rows = cached.get("list") or []

    # ===== requested/effective (감사/재현성) =====
    requested = _parse_sj_div_requested(sj_div, sj_div_in)
    effective = _effective_sj_div(requested)

    # ===== missing: 실제 데이터 기준 =====
    missing = _missing_required_from_rows(rows)

    # ===== III. 재무 스코프 락(연결 5종만) =====
    allowlist = set([x.upper() for x in effective])
    filtered = [r for r in rows if (str(r.get("sj_div") or "").upper() in allowlist)]

    total_rows = len(filtered)
    end = min(cursor + limit, total_rows)
    page = filtered[cursor:end]
    next_cursor = end if end < total_rows else None

    return {
        "op": "fnltt_all",
        "status": str(cached.get("status")),
        "message": str(cached.get("message")),
        "hint": f"Client should avoid >{MAX_CURSOR_PAGES_HINT} pages per report to prevent abuse/timeouts.",
        "scope_lock_applied": {
            "fs_div_lock": FS_DIV_LOCK,
            "reprt_code_lock": ANNUAL_REPRT_CODE_LOCK,
            "sj_div_allowlist_effective": sorted(list(allowlist)),
        },
        "sj_div_in_requested": requested,
        "sj_div_in_effective": effective,
        "sj_div_missing_required_in_data": missing,
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
    # reprt_code 파라미터는 호환성 위해 남기되 "11011만" 허용.
    reprt_code: Optional[str] = None,
    # fs_div 파라미터는 받더라도 서버에서 CFS로 락(호환성).
    fs_div: str = "CFS",
    sj_div: Optional[str] = Query(
        default=None,
        description="fnltt_all only. Single sj_div shard: BS/IS/CIS/SCE/CF. Mutually exclusive with sj_div_in.",
    ),
    sj_div_in: Optional[str] = Query(
        default=None,
        description="Comma-separated sj_div filter. ex) BS,IS,CIS,SCE,CF (default = all 5). Mutually exclusive with sj_div.",
    ),
    cursor: int = 0,
    limit: int = 100,  # limit 기본값 100 (프록시 내부 paging 단위)
    authorization: Optional[str] = Header(default=None),
):
    # optional bearer auth
    _require_token(authorization)

    # basic validations
    if op not in ("resolve", "list", "fnltt_all"):
        raise HTTPException(status_code=400, detail="op must be resolve | list | fnltt_all")

    if not (stock_code.isdigit() and len(stock_code) == 6):
        raise HTTPException(status_code=400, detail="stock_code must be 6 digits")

    fs_div = (fs_div or "").strip().upper()
    if fs_div and fs_div not in ("CFS", "OFS"):
        raise HTTPException(status_code=400, detail="fs_div must be CFS or OFS")

    if cursor < 0:
        raise HTTPException(status_code=400, detail="cursor must be >= 0")

    if limit < MIN_LIMIT or limit > MAX_LIMIT:
        raise HTTPException(status_code=400, detail=f"limit must be {MIN_LIMIT}..{MAX_LIMIT}")

    # sj_div / sj_div_in are fnltt_all only
    if op != "fnltt_all" and (sj_div or sj_div_in):
        raise HTTPException(status_code=400, detail="sj_div/sj_div_in is only allowed when op=fnltt_all")

    if op == "resolve":
        return await _resolve_stock_code(stock_code)

    if op == "list":
        # Annual-only list
        return await _list_annual_business_reports(stock_code, lookback_years)

    # op == fnltt_all
    if not bsns_year or not (bsns_year.isdigit() and len(bsns_year) == 4):
        raise HTTPException(status_code=400, detail="bsns_year(YYYY) required for op=fnltt_all")

    # Annual-only reprt_code lock:
    if reprt_code is None:
        reprt_code = ANNUAL_REPRT_CODE_LOCK
    reprt_code = reprt_code.strip()
    if reprt_code != ANNUAL_REPRT_CODE_LOCK:
        raise HTTPException(status_code=400, detail="reprt_code is locked to 11011 (annual business report)")

    # mutual exclusive
    if sj_div and sj_div_in:
        raise HTTPException(status_code=400, detail="Provide either sj_div or sj_div_in, not both")

    if sj_div:
        sj_div = sj_div.strip().upper()
        if sj_div not in SJ_DIV_ALLOWLIST_DEFAULT:
            raise HTTPException(status_code=400, detail="sj_div must be one of BS/IS/CIS/SCE/CF")

    # 강제: 연결(CFS)만 허용 (입력값이 뭐든 서버에서 락)
    if fs_div and fs_div != "CFS":
        raise HTTPException(status_code=400, detail="fs_div is locked to CFS (consolidated) in this proxy")

    return await _fnltt_all_page(stock_code, bsns_year, cursor, limit, sj_div, sj_div_in)
