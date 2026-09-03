# -*- coding: utf-8 -*-
"""추석 명절 고속도로 휴게소 통합 조회기 — 한국도로공사 오픈API 연동.

data.ex.co.kr openapi 6개 엔드포인트를 수집·캐싱하고 stdRestCd 기준으로 merge해
휴게소 하나당 통합 객체를 만든다. Claude API 미사용 — 순수 공공데이터 API 호출.

캐시: tools/restarea_cache/*.json (fetched_at 타임스탬프 포함, TTL 지나면 재수집)
  - locationinfo.json / restconv.json / restbestfood.json / resttheme.json : TTL 24h
  - curstate.json (유가) : TTL 4~6h
  - totalspeed.json (구간 평균속도) : TTL 5~10min
  - holiday_specials.json : 수작업 명절 특선메뉴 배지 매핑(비어있으면 배지 없음)
"""
import json
import math
import os
import re
import threading
import time
from pathlib import Path

import requests

BASE = Path(__file__).resolve().parent.parent  # golf_automation
CACHE_DIR = Path(__file__).resolve().parent / "restarea_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

API_BASE = "https://data.ex.co.kr/openapi/"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}
TIMEOUT = 20

# 도로공사 오픈API 월별 호출 한도(사용자 확인, 정확한 숫자 미확정 — 보수적으로
# 월 1,000건으로 가정) 안에 안전하게 들어오도록 TTL을 완화한다(2026-09-01).
# 계산 근거 — "하루 호출횟수 = 24h / TTL(h)" × "1회 갱신당 페이지 수"로 추정하고
# 30일을 곱한다. refresh_all()이 백엔드에서 TTL 만료 시에만 상류 API를 호출하므로
# (프론트 5분 폴링은 우리 서버 캐시를 보는 것이지 상류를 매번 부르지 않음),
# 아래 갱신주기만이 실제 월 호출량을 결정한다.
#
#   locationinfo  : 7일(168h) TTL → 월 30/7 ≈ 4.3회 갱신 × 페이지 수(약 5p) ≈ 22건
#   restconv      : 7일(168h) TTL → 월 4.3회 × 13p(1286건/99) ≈ 56건
#   restbestfood  : 7일(168h) TTL → 월 4.3회 × 74p(7321건/99) ≈ 318건  (가장 큰 비중)
#   resttheme     : 7일(168h) TTL → 월 4.3회 × 소수 페이지(≈2p) ≈ 9건
#   units         : 7일(168h) TTL → 월 4.3회 × 소수 페이지(≈3p) ≈ 13건
#   curstate(유가): 12h TTL     → 하루 2회 × 30일 × 페이지 수(≈3p) ≈ 180건
#   totalspeed    : 6h TTL      → 하루 4회 × 30일 × 1p(고정 11건 단일 응답) = 120건
#                    (실측상 파라미터 무관 고정 샘플이라 자주 부를 이유가 전혀 없음 —
#                     그래도 서비스 중단 감지용으로 하루 4회 정도는 유지)
#   burstinfo     : 1h TTL      → 하루 24회 × 30일 × 3p(≈270건/99) ≈ 2,160건  ← 초과!
#                    → 아래처럼 실제로는 1시간 TTL이 burstinfo 한 항목만으로도 한도를
#                       넘길 수 있어, 정적 5종 합계(약 418건) + curstate(180) +
#                       totalspeed(120) = 718건을 쓰고 나면 burstinfo에 남는 예산은
#                       약 282건/월 → 3페이지 기준 약 94회 = 대략 3.9시간에 1회 꼴.
#                       안전 마진을 위해 burstinfo TTL은 "1시간"이 아니라 좀 더
#                       보수적으로 유지하되, 요청 사양대로 우선 1시간으로 설정하고
#                       실사용 중 /api/restarea/refresh 강제호출(관리자 수동 갱신)
#                       빈도를 낮게 유지하는 것으로 마진을 확보한다.
#   route-traffic(온디맨드): 24h 캐시 + 톨게이트쌍 캡 15개 → 노선 1회 조회당 최대
#                    15건 호출. 하루 10개 노선을 눌러봐도 150건 → 월 4,500건까지
#                    치솟을 수 있으므로 실제로는 사용 빈도에 좌우된다(사용자가 노선을
#                    자주 바꿔가며 탐색하지 않는 한 문제 없음 — 24h 캐시라 같은 노선
#                    재조회는 0건).
#
#   정적 5종 + curstate + totalspeed 합계 ≈ 718건/월, burstinfo(1시간 TTL 그대로 적용
#   시 이론상 최대 2,160건)까지 더하면 이론상 월 1,000건을 넘을 수 있다 — 다만 이는
#   "burstinfo가 항상 3페이지 꽉 채워 매시간 호출"되는 최악 가정이고, 실제로는
#   TTL 만료 시점에 요청이 들어올 때만 호출되므로(사용자 접속이 뜸하면 자동으로
#   덜 호출됨) 트래픽이 적은 시간대엔 훨씬 적게 나간다. 그래도 안전 마진을 위해
#   burstinfo를 요청 사양의 "1시간"으로 설정하되, 이 파일을 만지는 사람은 실사용
#   호출 로그를 봐서 초과 조짐이 있으면 burstinfo TTL을 2~3시간으로 더 늘릴 것.
TTL = {
    "locationinfo": 7 * 24 * 3600,   # 24h → 7일: 공공기관 정적 데이터, 자주 안 바뀜
    "restconv": 7 * 24 * 3600,       # 24h → 7일: 페이지 수 많음(13p) — 호출량 절감 핵심
    "restbestfood": 7 * 24 * 3600,   # 24h → 7일: 페이지 수 가장 많음(74p) — 호출량 절감 핵심
    "resttheme": 7 * 24 * 3600,      # 24h → 7일
    "curstate": 12 * 3600,           # 5h → 12h: 유가, 하루 2회면 충분
    "totalspeed": 6 * 3600,          # 5min → 6h: 실측상 파라미터 무관 고정 11건 샘플이라
                                       # 5분마다 다시 부를 이유가 없음(2026-09-01 확인)
    "units": 7 * 24 * 3600,          # 24h → 7일: 영업소(톨게이트) 좌표, 거의 안 바뀜
    "burstinfo": 3600,               # 8min → 1h: 실시간성은 유지하되 과도한 호출 방지
    # 2026-09-02 신규 4종 API 도입 — sectionTrafficRouteDirection(콘존 교통량)은
    # 실측상 15분 단위로 갱신되므로 15분 TTL, LCS(차로제어) 상태는 그보다 느리게
    # 바뀌어 30분 TTL로 잡는다. trafficRoute는 스킵(사용자 결정, 완성도 우선순위 밖).
    "conzone_traffic": 15 * 60,      # sectionTrafficRouteDirection 전국 — 15분 TTL
    "lcs_status": 30 * 60,           # lcsRtLineGrp 전국 — 30분 TTL
    # trtm/realUnitTrtm(톨게이트쌍 구간 평균 통행시간) — 실측(2026-09-02) 조회
    # 파라미터가 전부 무시돼 매 요청 최대 300페이지(약 3만행)를 다시 긁어야 해서
    # 무겁다. 사용자 요청 TTL 범위(5~10분) 중간값인 8분으로 잡는다.
    "realunittrtm": 8 * 60,
}

# vmsMessageSrchByRoute는 routeNo(4자리)별로 따로 불러야 해서 _FETCHERS 공통 패턴이
# 아니라 conzone_traffic 방식과 유사한 라우트별 메모리 캐시를 쓴다(get_route_traffic의
# _ROUTE_TRAFFIC_CACHE와 동일한 스타일).
_VMS_CACHE = {}  # route_no_4digit -> {"at": ts, "data": [...]}
_VMS_TTL = 40 * 60  # 30~60분 권장 범위 중간값

HOLIDAY_SPECIALS_FILE = CACHE_DIR / "holiday_specials.json"
if not HOLIDAY_SPECIALS_FILE.exists():
    HOLIDAY_SPECIALS_FILE.write_text("{}", encoding="utf-8")


def _api_key() -> str:
    return os.getenv("EX_OPENAPI_KEY", "")


def _cache_path(name: str) -> Path:
    return CACHE_DIR / f"{name}.json"


def _load_cache(name: str):
    p = _cache_path(name)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_cache(name: str, data):
    p = _cache_path(name)
    payload = {"fetched_at": time.time(), "data": data}
    p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return payload


def _is_fresh(name: str) -> bool:
    c = _load_cache(name)
    if not c:
        return False
    ttl = TTL.get(name, 24 * 3600)
    return (time.time() - c.get("fetched_at", 0)) < ttl


def _get(endpoint: str, params: dict) -> dict:
    p = {"key": _api_key(), "type": "json"}
    p.update(params)
    r = requests.get(API_BASE + endpoint, params=p, headers=UA, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def _fetch_simple_list(endpoint: str) -> list:
    """페이지네이션 없이 list 전체가 한 번에 오는 엔드포인트."""
    d = _get(endpoint, {})
    return d.get("list") or []


def _fetch_paginated_list(endpoint: str, page_size: int = 500, max_pages: int = 200) -> list:
    """count가 커서 pageNo로 나눠 받아야 하는 엔드포인트. 전체 수집.

    주의: data.ex.co.kr는 numOfRows를 크게 요청해도 실제로는 페이지당 최대
    ~99건만 내려준다(실측 확인, 2026-09-01). 그래서 반환된 개수가 요청보다
    적어도 곧바로 멈추지 않고, count(총건수)에 도달하거나 빈 페이지가 올
    때까지 계속 다음 페이지를 요청한다.
    """
    out = []
    page = 1
    total = None
    while page <= max_pages:
        d = _get(endpoint, {"numOfRows": page_size, "pageNo": page})
        items = d.get("list") or []
        if total is None:
            total = d.get("count")
        if not items:
            break
        out.extend(items)
        if total is not None and len(out) >= int(total):
            break
        page += 1
    return out


def _fetch_locationinfo():
    return _fetch_paginated_list("locationinfo/locationinfoRest")


def _fetch_restconv():
    return _fetch_paginated_list("restinfo/restConvList")


def _fetch_restbestfood():
    return _fetch_paginated_list("restinfo/restBestfoodList")


def _fetch_resttheme():
    return _fetch_paginated_list("restinfo/restThemeList")


def _fetch_curstate():
    return _fetch_paginated_list("business/curStateStation")


def _fetch_totalspeed():
    """totalSpeed 엔드포인트는 list가 아니라 totalSpeedLists 배열로 온다.

    주의(2026-09-01 실측): routeNo/numOfRows/pageNo/stdDate 등 어떤 파라미터를
    넣어도 응답이 바뀌지 않는다 — 항상 동일한 고정 11건 샘플만 온다. 전국망
    실시간 구간 데이터가 아니라 제한된 샘플이므로, 이 11건 범위 안에서만
    구간(폴리라인)을 그린다 — 전국 커버리지가 있는 것처럼 과장하지 않는다.
    """
    d = _get("trafficOprgPrcd/totalSpeed", {})
    return d.get("totalSpeedLists") or d.get("list") or []


def _fetch_units():
    """영업소(톨게이트) 좌표 — locationinfoUnit. totalSpeed의 stUnitCd/edUnitCd와
    unitCode로 조인해 구간 좌표를 구하는 데 쓴다."""
    return _fetch_paginated_list("locationinfo/locationinfoUnit")


def _fetch_burst_info():
    """burstInfo/realTimeSms — 사고/공사/기상특보 등 돌발정보 전체 페이지네이션 수집.

    이 엔드포인트는 다른 곳과 달리 리스트 키가 "list"가 아니라 "realTimeSMSList"라
    _fetch_paginated_list을 그대로 못 쓴다. 페이지당 최대 ~99건(실측)이라 count(약
    270여 건)에 도달할 때까지 계속 다음 페이지를 받는다.

    좌표 필드 판별(2026-09-01 실측): "latitude" 필드가 33~43 범위(실제 위도),
    "altitude" 필드가 124~132 범위(실제 경도)로 나온다 — 필드명이 "altitude"이지만
    실제로는 경도값이다. 좌표가 둘 다 있는 항목만 남기고 lat/lng로 정규화한다.
    실측 결과 273건 중 193건(약 71%)이 좌표를 갖고 있었다(예상보다 비율이 높음).
    """
    out = []
    page = 1
    total = None
    page_size = 99
    max_pages = 60
    while page <= max_pages:
        d = _get("burstInfo/realTimeSms", {"numOfRows": page_size, "pageNo": page})
        items = d.get("realTimeSMSList") or []
        if total is None:
            total = d.get("count")
        if not items:
            break
        out.extend(items)
        if total is not None and len(out) >= int(total):
            break
        page += 1

    normalized = []
    for it in out:
        lat = _to_float(it.get("latitude"))
        lng = _to_float(it.get("altitude"))  # 필드명은 altitude이지만 실측상 경도값
        if lat is None or lng is None:
            continue
        # 값 범위로 한 번 더 검증(위도 33~43, 경도 124~132) — 혹시 순서가 뒤집힌
        # 항목이 섞여 있으면 걸러낸다.
        if not (32 <= lat <= 44):
            continue
        if not (123 <= lng <= 133):
            continue
        normalized.append({
            "accHour": it.get("accHour"),
            "accDate": it.get("accDate"),
            "accTypeCode": it.get("accTypeCode"),
            "accType": it.get("accType"),
            "startEndTypeCode": it.get("startEndTypeCode"),
            "smsText": it.get("smsText"),
            "accProcessCode": it.get("accProcessCode"),
            "accProcessNM": it.get("accProcessNM"),
            "accPointNM": it.get("accPointNM"),
            "nosunNM": it.get("nosunNM"),
            "roadNM": it.get("roadNM"),
            "lat": lat,
            "lng": lng,
        })
    return normalized


def _round_down_15min(dt):
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)


def _fetch_conzone_traffic():
    """trafficapi/sectionTrafficRouteDirection — 전국 콘존(conzone, 도로공사 구간
    단위) 실시간 교통량. 15분 단위 집계라 15분 TTL로 캐시한다.

    실측(2026-09-02): collectDate/collectTime을 "오늘/현재시각 15분 내림"으로 넣으면
    당일 데이터는 아직 집계 전이라 count=0으로 비어 있는 경우가 많고, "어제 같은
    시각"은 항상 채워져 있었다 — 데이터가 약 하루 지연되어 확정되는 것으로 보인다.
    그래서 오늘 시각으로 먼저 시도하고 비어 있으면 어제 같은 시각으로 한 번 더
    시도한다(폴백 1회, 과호출 방지). trafficCount == -1(결측)은 제거한다.
    conzoneId 구조는 "{lineCode 4자리}CZE{일련번호}"(정방향, 콘존명이 A-B 순서) 또는
    "...CZS{일련번호}"(역방향, 콘존명이 B-A로 뒤집힘) — lineCode가 곧 노선코드다."""
    import datetime
    now = datetime.datetime.now()
    rounded = _round_down_15min(now)
    attempts = [rounded, rounded - datetime.timedelta(days=1)]
    for dt in attempts:
        collect_date = dt.strftime("%Y%m%d")
        collect_time = dt.strftime("%H%M")
        out = []
        page = 1
        total = None
        while page <= 40:
            d = _get("trafficapi/sectionTrafficRouteDirection", {
                "collectDate": collect_date, "collectTime": collect_time,
                "numOfRows": 99, "pageNo": page,
            })
            items = d.get("list") or []
            if total is None:
                total = d.get("count")
            if not items:
                break
            out.extend(items)
            if total is not None and len(out) >= int(total):
                break
            page += 1
        if out:
            filtered = [r for r in out if _to_int(r.get("trafficCount")) not in (None, -1)]
            for r in filtered:
                r["_collectDate"] = collect_date
                r["_collectTime"] = collect_time
            return filtered
    return []


def _fetch_lcs_status():
    """lcs/lcsRtLineGrp — 차로제어시스템(LCS) 설치 콘존의 적용 여부(applyYn).
    전국 콘존 전체가 아니라 LCS가 설치된 일부 구간만 나온다(실측 183건, 2026-09-02).
    conzone_traffic과 conzoneId로 조인해 "이 구간은 LCS 표시 중"만 덧붙이는 용도."""
    out = []
    page = 1
    total = None
    while page <= 20:
        d = _get("lcs/lcsRtLineGrp", {"numOfRows": 99, "pageNo": page})
        items = d.get("list") or []
        if total is None:
            total = d.get("count")
        if not items:
            break
        out.extend(items)
        if total is not None and len(out) >= int(total):
            break
        page += 1
    # 같은 conzoneId가 중복으로 오는 경우가 있어(실측) conzoneId 기준으로 dedupe,
    # applyYn="Y"가 하나라도 있으면 그걸 우선한다.
    by_id = {}
    for r in out:
        cid = r.get("conzoneId")
        if not cid:
            continue
        if cid not in by_id or (r.get("applyYn") == "Y" and by_id[cid].get("applyYn") != "Y"):
            by_id[cid] = r
    return list(by_id.values())


# trtm/realUnitTrtm 페이지 상한. sumTmUnitTypeCode=3(시간단위 집계)을 써도 전국
# 전체는 약 124,374건(2026-09-02 실측, 페이지당 최대 99건 고정 → 약 1,257페이지)이라
# 매 TTL(8분)마다 전량을 긁으면 응답이 몇 분씩 걸린다. 게다가 이 엔드포인트 자체가
# 페이지당 약 5초로 느려(2026-09-02 실측, 15페이지 78.6초 = 페이지당 5.24초) 300페이지면
# 약 26분 걸린다 — 요청 스레드를 그만큼 블로킹할 수 없다. 데이터는 (톨게이트쌍,차종)
# 블록 단위로 최근 시각→과거 시각 내림차순 정렬되어 오므로, 앞쪽 페이지일수록
# 온전한 (쌍,차종) 블록을 확보하기 좋다는 성질을 이용해 페이지 수를 실용적으로
# 제한한다 — 그만큼 뒤쪽(나중 등장) 톨게이트쌍은 매칭이 안 될 수 있고, 이는
# 지어내지 않고 그대로 None(미매칭)으로 남긴다. 실측(2026-09-02) 40페이지 수집에
# 209초 걸렸고(페이지당 약 5.2초), 톨게이트쌍 309개를 얻었지만 콘존 매칭은 1,105개
# 구간 중 4개(0.4%)뿐이었다 — 톨게이트 코드가 낮은 번호(101~1xx대) 구간에 커버리지가
# 몰려 있어서다. 60페이지(약 5분)로 완화해 커버리지를 좀 더 넓히되, 이 수집 자체는
# 백그라운드 스레드에서 돌려 요청을 블로킹하지 않는다(_realunittrtm_cached 참고 —
# 수집 완료 전에는 이전 캐시나 빈 dict를 즉시 반환). 매칭률이 낮은 근본 원인은
# 매 요청 파라미터 필터가 통하지 않는 이 API 자체의 한계이며, 더 늘려도 페이지당
# 지연은 그대로라 응답 시간만 늘어난다 — 필요시 이 값을 조정할 것.
_RUT_MAX_PAGES = 60


def _fetch_real_unit_trtm():
    """trtm/realUnitTrtm — 인접 톨게이트쌍 구간의 평균 통행시간(분). 실측(2026-09-02):
    key/type 외의 모든 조회 파라미터(stdDate, startUnitCode/endUnitCode, tcsCarTypeCode,
    stdTime, sumTmUnitTypeCode 값별 필터 등)는 응답 내용을 바꾸지 않는다 — 단,
    sumTmUnitTypeCode 자체는 예외로 값에 따라 집계 단위가 달라진다(1=5분 단위
    643,319건 / 2=10분 단위 354,126건 / 3=시간 단위 124,374건 확인) — 가장 적은
    3(시간단위)을 쓴다. pageNo 페이지네이션은 정상 동작한다(실측 확인).

    startUnitCode/endUnitCode는 locationinfoUnit의 unitCode와 동일 체계라 이름
    매칭 없이 바로 조인 가능(사용자 확인). 승용차(1종, tcsCarTypeCode=='1')만
    남기고, (startUnitCode, endUnitCode) 쌍별로 가장 먼저 등장한(=가장 최근 시각)
    행 하나만 저장한다. 반환: {"{start}|{end}": {...}}"""
    out = {}
    page = 1
    while page <= _RUT_MAX_PAGES:
        d = _get("trtm/realUnitTrtm", {"sumTmUnitTypeCode": 3, "numOfRows": 99, "pageNo": page})
        items = d.get("realUnitTrtmVO") or []
        if not items:
            break
        for it in items:
            if (it.get("tcsCarTypeCode") or "").strip() != "1":
                continue
            sc = (it.get("startUnitCode") or "").strip()
            ec = (it.get("endUnitCode") or "").strip()
            if not sc or not ec:
                continue
            key = f"{sc}|{ec}"
            if key in out:
                continue  # 이미 이 쌍의 가장 최근 시각 행을 저장함(내림차순 정렬 전제)
            ta = _to_float(it.get("timeAvg"))
            if ta is None or ta <= 0:
                continue
            out[key] = {
                "startUnitCode": sc, "endUnitCode": ec,
                "timeAvg": ta, "stdTime": (it.get("stdTime") or "").strip(),
                "startUnitNm": it.get("startUnitNm"), "endUnitNm": it.get("endUnitNm"),
            }
        page += 1
    return out


_RUT_CACHE = {"data": None, "at": 0}
_RUT_FETCH_LOCK = threading.Lock()
_RUT_FETCHING = {"flag": False}


def _realunittrtm_background_refresh():
    try:
        data = _fetch_real_unit_trtm()
        _save_cache("realunittrtm", data)
        _RUT_CACHE["data"] = data
        _RUT_CACHE["at"] = time.time()
        print(f"[_realunittrtm_background_refresh] 완료 — {len(data)}쌍")
    except Exception as e:
        print(f"[_realunittrtm_background_refresh] 수집 실패: {e}")
    finally:
        with _RUT_FETCH_LOCK:
            _RUT_FETCHING["flag"] = False


def _realunittrtm_cached():
    """realUnitTrtm은 페이지당 약 5초로 느려(_RUT_MAX_PAGES=40이면 약 3.5분) 요청
    스레드를 블로킹할 수 없다 — 캐시가 없거나 만료됐으면 백그라운드 스레드에서
    수집을 시작(중복 시작 방지)하고, 이번 호출은 "직전까지 알던 값"(디스크 캐시
    또는 빈 dict)을 즉시 반환한다. 아직 한 번도 수집된 적 없으면 빈 dict를 반환해
    이번 요청의 모든 구간이 정직하게 미매칭(None)으로 처리되고, 백그라운드 수집이
    끝나면 다음 폴링(프론트 5분 주기)부터 실제 속도가 채워진다. _cached_data()는
    list 전용이라(dict는 형태가 달라) 별도 접근자를 둔다."""
    now = time.time()
    if _RUT_CACHE["data"] is not None and (now - _RUT_CACHE["at"]) < TTL["realunittrtm"]:
        return _RUT_CACHE["data"]
    c = _load_cache("realunittrtm")
    if c:
        _RUT_CACHE["data"] = c.get("data") or {}
        _RUT_CACHE["at"] = c.get("fetched_at", 0)
        fresh = (now - c.get("fetched_at", 0)) < TTL["realunittrtm"]
    else:
        fresh = False
        if _RUT_CACHE["data"] is None:
            _RUT_CACHE["data"] = {}
    if not fresh:
        with _RUT_FETCH_LOCK:
            already = _RUT_FETCHING["flag"]
            if not already:
                _RUT_FETCHING["flag"] = True
        if not already:
            threading.Thread(target=_realunittrtm_background_refresh, daemon=True).start()
    return _RUT_CACHE["data"]


# 직선거리(Haversine) 대비 실제 도로거리 보정계수. 근거: 이 파일의 OSRM 우회 검증
# 로직(_OSRM_DETOUR_RATIO_MAX=1.6)에서 정상적인 인접 톨게이트 구간의 실제/직선
# 거리 비율이 대체로 1.0~1.3대로 관찰됐다(2026-09-02) — 그 범위의 중간값 근사로
# 1.1을 채택한다. 실제 도로 곡선(get_road_subpath)을 구하지 못했을 때만 쓰는 최후
# 폴백이다.
_HAVERSINE_ROAD_FACTOR = 1.1


def _path_length_km(path):
    if not path or len(path) < 2:
        return None
    total = 0.0
    for a, b in zip(path, path[1:]):
        d = _haversine(a[0], a[1], b[0], b[1])
        if d is not None:
            total += d
    return total if total > 0 else None


def _pair_speed_kmh(u1, u2, route_no=None):
    """u1→u2 방향 구간의 실제 평균속도(km/h) = 거리(km) / (timeAvg(분)/60).
    거리는 get_road_subpath()의 실제 도로 곡선 길이를 우선 쓰고, 실패하면
    Haversine 직선거리 × _HAVERSINE_ROAD_FACTOR로 폴백한다. realUnitTrtm에
    이 톨게이트쌍 데이터가 없으면(_RUT_MAX_PAGES 상한으로 미수집 포함) None —
    지어내지 않는다."""
    if not u1 or not u2:
        return None
    code1, code2 = (u1.get("unitCode") or "").strip(), (u2.get("unitCode") or "").strip()
    if not code1 or not code2:
        return None
    rut = _realunittrtm_cached()
    row = rut.get(f"{code1}|{code2}")
    if not row:
        return None
    ta = row.get("timeAvg")
    if not ta or ta <= 0:
        return None
    dist_km = None
    if route_no:
        try:
            path = get_road_subpath(route_no, code1, code2)
            if path:
                dist_km = _path_length_km(path)
        except Exception:
            dist_km = None
    if dist_km is None:
        lat1, lng1 = _to_float(u1.get("yValue")), _to_float(u1.get("xValue"))
        lat2, lng2 = _to_float(u2.get("yValue")), _to_float(u2.get("xValue"))
        straight = _haversine(lat1, lng1, lat2, lng2)
        if straight is None or straight <= 0:
            return None
        dist_km = straight * _HAVERSINE_ROAD_FACTOR
    if not dist_km or dist_km <= 0:
        return None
    speed = dist_km / (ta / 60.0)
    if speed <= 0 or speed > 200:  # 비정상치(데이터 결측/오조인 의심) 방어
        return None
    return round(speed, 1)


def _fetch_vms_messages(route_no_4digit):
    """vms/vmsMessageSrchByRoute — 특정 노선(4자리 routeNo)의 VMS 전광판 메시지.
    routeNo는 반드시 4자리(예: "0010") — 3자리로 넣으면 0건(실측)."""
    out = []
    page = 1
    total = None
    while page <= 20:
        d = _get("vms/vmsMessageSrchByRoute", {
            "routeNo": route_no_4digit, "numOfRows": 99, "pageNo": page,
        })
        items = d.get("vmsMessageSrchByRouteLists") or d.get("list") or []
        if total is None:
            total = d.get("count")
        if not items:
            break
        out.extend(items)
        if total is not None and len(out) >= int(total):
            break
        page += 1
    return out


def get_vms_messages(route_no):
    """route_no(3자리/4자리/키 어느 형태든)를 4자리 형태로 정규화해 VMS를 캐시 조회.
    간단한 중복 제거(같은 vmsMessage+vmsMessage2 조합)만 하고, 어떤 메시지가
    "실질 정보"이고 어떤 게 "안전 슬로건"인지는 프런트에서 가볍게 표시한다."""
    key = _route_key(route_no)
    # 4자리 형태 복원: units 데이터에서 같은 route_key를 가진 원본 routeNo를 찾아
    # 4자리(예: "010" → "0100")로 만든다. 못 찾으면 원본을 그대로 4자리로 zero-pad.
    units = _cached_data("units")
    raw4 = None
    for u in units:
        if _route_key(u.get("routeNo")) == key:
            rn = (u.get("routeNo") or "").strip()
            raw4 = rn if len(rn) == 4 else (rn + "0" if len(rn) == 3 else rn.zfill(4))
            break
    if not raw4:
        raw4 = str(route_no).zfill(4)

    now = time.time()
    cached = _VMS_CACHE.get(raw4)
    if cached and (now - cached["at"]) < _VMS_TTL:
        return cached["data"]
    try:
        rows = _fetch_vms_messages(raw4)
    except Exception:
        rows = []
    seen = set()
    dedup = []
    for r in rows:
        sig = (r.get("vmsMessage"), r.get("vmsMessage2"), r.get("updownType"))
        if sig in seen:
            continue
        seen.add(sig)
        dedup.append({
            "vmsId": r.get("vmsId"),
            "updownType": r.get("updownType"),
            "vmsMessage": (r.get("vmsMessage") or "").strip(),
            "vmsMessage2": (r.get("vmsMessage2") or "").strip(),
        })
    _VMS_CACHE[raw4] = {"at": now, "data": dedup}
    return dedup


# ── VMS 실질 공지 티커 (2026-09-03) ─────────────────────────────────────────
# 전 노선(휴게소 있는 22개) VMS에서 "실질 공지"만 걸러 프론트 전광판 티커에
# 공급한다. 필터 기준은 2026-09-03 전 노선 실측(1,145건)으로 캘리브레이션:
#   - 슬로건(안전띠·범칙금·졸음운전·사망 통계·보이스피싱 등)이 전체의 약 60%
#   - vmsMessage/vmsMessage2에 "실질 공지 + 슬로건"이 섞여 오는 경우가 많아
#     문장(서브메시지) 단위로 각각 판정한다 — 통짜 판정하면 "전면차단 예정 +
#     안전띠" 같은 진짜 공지가 슬로건에 딸려 통째로 버려진다(실측 확인).
#   - 매칭은 공백 제거 후 부분일치("절 대 금 지", "차로 축소" 등 띄어쓰기 변형 흡수).
_VMS_ALERT_INCLUDE = [
    "작업", "공사", "사고", "통제", "차단", "폐쇄", "정체", "지정체", "지체",
    "서행", "우회", "결빙", "안개", "낙하물", "장애물", "고장", "차로축소",
    "진입금지", "돌발", "포트홀", "화재", "침수", "강풍", "폭설", "대설",
    "제설", "호우", "특보", "낙석", "역주행", "영업중단", "이용불가", "임시폐쇄",
]
_VMS_ALERT_EXCLUDE = [
    # 슬로건·캠페인·계도 문구 — 서브메시지 하나에 하나라도 있으면 그 문장은 제외
    "안전띠", "범칙금", "벌점", "과태료", "사망", "졸음운전", "보이스피싱",
    "캠페인", "TEST", "테스트", "문안표출", "감사합니다", "우선대피", "1588",
    "절대금지", "무조건감속", "안전운전", "안전운행", "전방주시", "생명",
]
_VMS_ALERTS_CACHE = {"at": 0.0, "data": None}
_VMS_ALERTS_TTL = 10 * 60  # 결과 캐시 — 노선별 원본은 _VMS_CACHE(40분)가 별도 관리


def _vms_alert_text(msg):
    """서브메시지 1건 정제·판정 — 실질 공지면 공백 정돈한 문구, 아니면 None."""
    txt = " ".join((msg or "").split())
    if not txt:
        return None
    flat = txt.replace(" ", "")
    if not any(k in flat for k in _VMS_ALERT_INCLUDE):
        return None
    if any(k in flat for k in _VMS_ALERT_EXCLUDE):
        return None
    return txt


def get_vms_alerts(force=False):
    """휴게소가 있는 전 노선의 VMS 실질 공지 목록.
    [{routeNo(3자리 키), routeName, updownType, text}] — 문구 기준 노선 내 중복 제거.
    노선별 원본은 get_vms_messages()의 40분 캐시를 그대로 타므로, 10분 주기 발행
    스레드가 불러도 실제 상류 API 스윕은 40분에 1회(22노선 × 약 35페이지)다.
    updownType(E/S)은 공식 상/하행 대응이 코드상 확정돼 있지 않아 참고 필드로만
    내려보낸다(프론트는 방향 매칭에 쓰지 않음 — 2026-09-03 결정)."""
    now = time.time()
    c = _VMS_ALERTS_CACHE
    if not force and c["data"] is not None and (now - c["at"]) < _VMS_ALERTS_TTL:
        return c["data"]
    location = _cached_data("locationinfo")
    routes = {}  # routeNo(원본) -> routeName
    for l in location:
        rn = (l.get("routeNo") or "").strip()
        if rn and rn not in routes:
            routes[rn] = l.get("routeName") or rn
    out = []
    for rn in sorted(routes):
        try:
            rows = get_vms_messages(rn)
        except Exception:
            continue
        seen = set()
        for r in rows:
            parts = []
            for m in (r.get("vmsMessage"), r.get("vmsMessage2")):
                t = _vms_alert_text(m)
                if t and t not in parts:
                    parts.append(t)
            if not parts:
                continue
            text = " / ".join(parts)
            key = (r.get("updownType"), text)
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "routeNo": _route_key(rn),
                "routeName": routes[rn],
                "updownType": r.get("updownType"),
                "text": text,
            })
    _VMS_ALERTS_CACHE["at"] = now
    _VMS_ALERTS_CACHE["data"] = out
    return out


_FETCHERS = {
    "locationinfo": _fetch_locationinfo,
    "restconv": _fetch_restconv,
    "restbestfood": _fetch_restbestfood,
    "resttheme": _fetch_resttheme,
    "curstate": _fetch_curstate,
    "totalspeed": _fetch_totalspeed,
    "units": _fetch_units,
    "burstinfo": _fetch_burst_info,
    "conzone_traffic": _fetch_conzone_traffic,
    "lcs_status": _fetch_lcs_status,
    # realunittrtm은 dict 캐시(리스트 아님)라 _cached_data 공용 경로 대신
    # _realunittrtm_cached()로 별도 관리한다 — refresh_all()의 주기적 TTL
    # 갱신 대상에는 넣지 않는다(무거운 300페이지 수집을 주기 작업에 얹지 않고
    # 실제 요청이 들어올 때만 지연 수집).
}


def refresh_all(force: bool = False) -> dict:
    """모든 소스를 TTL 확인 후 필요하면 재수집. 결과 요약 반환."""
    result = {}
    for name, fn in _FETCHERS.items():
        if not force and _is_fresh(name):
            result[name] = {"status": "cached", "count": len(_load_cache(name)["data"])}
            continue
        try:
            data = fn()
            _save_cache(name, data)
            result[name] = {"status": "ok", "count": len(data)}
        except Exception as e:
            result[name] = {"status": "error", "error": str(e)}
    return result


def _cached_data(name: str) -> list:
    c = _load_cache(name)
    if not c:
        refresh_all(force=False)
        c = _load_cache(name)
    return (c or {}).get("data") or []


def _holiday_specials() -> dict:
    try:
        return json.loads(HOLIDAY_SPECIALS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _to_float(v):
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v):
    try:
        return int(v)
    except Exception:
        return None


_DIR_CITY_RE = re.compile(r"\(([^)]+)\)")


def _direction_city(name):
    """휴게소명 괄호 안 지명(예: '서울만남(부산)휴게소' → '부산')을 그 휴게소가
    바라보는 방향 도시명으로 추출한다. 도로공사 API에는 '상행/하행' 필드가 없고
    이 지명(curstate의 direction 필드와 동일한 값)만 있다."""
    m = _DIR_CITY_RE.search(name or "")
    return m.group(1) if m else None


def _build_direction_map(location):
    """노선(routeNo)별로 등장하는 방향 지명 2개를 찾아 하나는 '상행', 하나는
    '하행'으로 배정한다. 실제 공식 상행/하행 고시와 일치한다는 보장은 없고
    지도 마커 색상·리스트 배지를 방향별로 시각 구분하기 위한 이 도구 내부의
    일관된 규칙이다.

    배정 규칙(2026-09-01 사용자 정정 — 한국 고속도로 표준 관례): 두 지명 중
    "서울"이 있으면 그 쪽을 무조건 '상행'으로, 나머지를 '하행'으로 배정한다.
    "서울"이 없고 "부산"이 있으면 "부산" 쪽을 '하행'으로 배정한다(나머지가 상행).
    둘 다 해당 없는 노선(지방 도시끼리인 경우, 예: 부산-순천)은 공식 상/하행
    표준을 이 지명만으로 판단할 수 없어 기존처럼 가나다순 첫 지명=상행,
    두번째=하행으로 배정한다. 지명이 1개뿐이거나 3개 이상 섞인 노선은
    구분하지 않는다(None)."""
    cities_by_route = {}
    for loc in location:
        city = _direction_city(loc.get("unitName"))
        if not city:
            continue
        cities_by_route.setdefault(loc.get("routeNo"), set()).add(city)

    dir_map = {}  # routeNo -> {city: "상행"/"하행"}
    for route_no, cities in cities_by_route.items():
        if len(cities) != 2:
            continue
        ordered = sorted(cities)
        if "서울" in cities:
            other = [c for c in ordered if c != "서울"][0]
            dir_map[route_no] = {"서울": "상행", other: "하행"}
        elif "부산" in cities:
            other = [c for c in ordered if c != "부산"][0]
            dir_map[route_no] = {other: "상행", "부산": "하행"}
        else:
            dir_map[route_no] = {ordered[0]: "상행", ordered[1]: "하행"}
    return dir_map


def build_merged_restareas() -> list:
    """stdRestCd 기준으로 5개 소스를 merge해 휴게소 통합 리스트를 만든다."""
    location = _cached_data("locationinfo")
    convs = _cached_data("restconv")
    foods = _cached_data("restbestfood")
    themes = _cached_data("resttheme")
    curstate = _cached_data("curstate")
    specials = _holiday_specials()
    dir_map = _build_direction_map(location)

    conv_by_code, food_by_code, theme_by_code = {}, {}, {}
    for c in convs:
        conv_by_code.setdefault(c.get("stdRestCd"), []).append(c)
    for f in foods:
        food_by_code.setdefault(f.get("stdRestCd"), []).append(f)
    for t in themes:
        theme_by_code.setdefault(t.get("stdRestCd"), []).append(t)

    # 유가 정보는 stdRestCd가 없어 serviceAreaName(휴게소명)으로 근접 매칭한다.
    fuel_by_name = {}
    for f in curstate:
        nm = (f.get("serviceAreaName") or "").strip()
        if nm:
            fuel_by_name.setdefault(nm, []).append(f)

    def _norm_name(n):
        n = (n or "").replace(" ", "")
        for suf in ("휴게소", "주유소"):
            if n.endswith(suf):
                n = n[: -len(suf)]
        return n

    fuel_by_norm = {}
    for nm, items in fuel_by_name.items():
        fuel_by_norm.setdefault(_norm_name(nm), []).extend(items)

    merged = []
    for loc in location:
        std = loc.get("stdRestCd")
        name = loc.get("unitName") or ""
        amenities = []
        for c in conv_by_code.get(std, []):
            amenities.append({
                "name": c.get("psName"),
                "desc": c.get("psDesc"),
                "stime": c.get("stime"),
                "etime": c.get("etime"),
                "address": c.get("svarAddr"),
            })
        food_list = []
        for f in food_by_code.get(std, []):
            food_list.append({
                "name": f.get("foodNm"),
                "cost": f.get("foodCost"),
                "etc": f.get("etc"),
                "recommend": (f.get("recommendyn") == "Y"),
                "best": (f.get("bestfoodyn") == "Y"),
                "premium": (f.get("premiumyn") == "Y"),
                "season": f.get("seasonMenu"),
            })
        theme_list = []
        for t in theme_by_code.get(std, []):
            theme_list.append({"name": t.get("itemNm"), "detail": t.get("detail")})

        fuel_matches = fuel_by_norm.get(_norm_name(name)) or []
        fuel = None
        if fuel_matches:
            f0 = fuel_matches[0]
            fuel = {
                "gasoline": f0.get("gasolinePrice"),
                "diesel": f0.get("diselPrice"),
                "lpg": f0.get("lpgPrice") if f0.get("lpgYn") == "Y" else None,
                "oil_company": f0.get("oilCompany"),
                "direction": f0.get("direction"),
            }

        direction_city = _direction_city(name)
        direction = (dir_map.get(loc.get("routeNo")) or {}).get(direction_city)

        merged.append({
            "stdRestCd": std,
            "name": name,
            "routeNo": loc.get("routeNo"),
            "routeName": loc.get("routeName"),
            "lat": _to_float(loc.get("yValue")),
            "lng": _to_float(loc.get("xValue")),
            "serviceAreaCode": loc.get("serviceAreaCode"),
            "address": (amenities[0]["address"] if amenities else None),
            "amenities": amenities,
            "foods": food_list,
            "themes": theme_list,
            "nearby_fuel": fuel,
            "holiday_special": specials.get(std) or specials.get(name),
            "direction_city": direction_city,   # 방향 지명(예: "부산") — API 원본값
            "direction": direction,             # "상행"/"하행"/None — 이 도구 내부 시각구분용(비공식)
        })
    return merged


_MERGED_CACHE = {"data": None, "at": 0}
_MERGED_TTL = 300  # 5분 — 개별 소스 TTL과 별개로 merge 결과 재계산 캐시


def get_all_restareas(route=None, direction=None):
    now = time.time()
    if _MERGED_CACHE["data"] is None or (now - _MERGED_CACHE["at"]) > _MERGED_TTL:
        _MERGED_CACHE["data"] = build_merged_restareas()
        _MERGED_CACHE["at"] = now
    data = _MERGED_CACHE["data"]
    if route:
        data = [d for d in data if (d.get("routeNo") == route or d.get("routeName") == route)]
    if direction:
        data = [d for d in data if d.get("direction") == direction]
    return data


def get_restarea_detail(std_rest_cd):
    for d in get_all_restareas():
        if d.get("stdRestCd") == std_rest_cd:
            return d
    return None


def _haversine(lat1, lng1, lat2, lng2):
    if None in (lat1, lng1, lat2, lng2):
        return None
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def get_nearby(lat, lng, limit=20):
    lat, lng = _to_float(lat), _to_float(lng)
    data = get_all_restareas()
    scored = []
    for d in data:
        dist = _haversine(lat, lng, d.get("lat"), d.get("lng"))
        if dist is None:
            continue
        item = dict(d)
        item["distance_km"] = round(dist, 2)
        scored.append(item)
    scored.sort(key=lambda x: x["distance_km"])
    return scored[:limit]


def _speed_level(v):
    """평균속도 → 소통 등급(3단계, 구버전 — 현재 미사용, 하위호환용으로만 유지).
    80↑ 원활 / 40~80 서행 / 40↓ 정체."""
    if v is None:
        return None
    if v >= 80:
        return "smooth"
    if v >= 40:
        return "slow"
    return "jam"


def _speed_level_5(v):
    """실제 평균속도(km/h) → 5단계 절대기준 등급(2026-09-03 사용자 재확정):
    80↑ 매우원활 / 50~80 원활 / 30~50 서행 / 20~30 정체임박 / 20↓ 정체.
    ("소통 속도 50 이하가 서행, 30 이하 심한 서행, 20 이하면 정체" 지시 반영 —
    이전 90/70/50/30 기준은 고속도로에서 서행·정체가 과하게 표시됐음.)
    프론트 SPEED_SCALE(rest-stop-finder.html)과 반드시 같은 임계값·순서를 유지할 것."""
    if v is None:
        return None
    if v >= 80:
        return "vsmooth"
    if v >= 50:
        return "smooth"
    if v >= 30:
        return "slow"
    if v >= 20:
        return "nearjam"
    return "jam"


_UNIT_CACHE = {"by_code": None, "at": 0}
_UNIT_TTL = 24 * 3600


def _units_by_code():
    now = time.time()
    if _UNIT_CACHE["by_code"] is None or (now - _UNIT_CACHE["at"]) > _UNIT_TTL:
        units = _cached_data("units")
        by_code = {}
        for u in units:
            code = (u.get("unitCode") or "").strip()
            if not code:
                continue
            by_code[code] = {
                "name": u.get("unitName"),
                "routeNo": u.get("routeNo"),
                "routeName": u.get("routeName"),
                "lat": _to_float(u.get("yValue")),
                "lng": _to_float(u.get("xValue")),
            }
        _UNIT_CACHE["by_code"] = by_code
        _UNIT_CACHE["at"] = now
    return _UNIT_CACHE["by_code"]


_ROUTE_LINES_CACHE = {"data": None, "at": 0}
_ROUTE_LINES_TTL = 24 * 3600


def _route_key(route_no):
    """locationinfoRest(휴게소, 예: "0100")와 locationinfoUnit(톨게이트, 예: "010")의
    routeNo 자릿수 표기가 서로 다르다 — rest쪽이 unit쪽 끝에 "0"이 하나 더 붙은
    형태다(예: unit "010" ↔ rest "0100"). 예전엔 끝자리 0을 전부 제거(rstrip)해서
    맞췄는데, 이러면 서로 다른 노선(예: "001"/"010"/"100")이 전부 "1"로 뭉개져
    엉뚱한 노선끼리 충돌하는 버그가 있었다(2026-09-02 발견 — 남해선/수도권1순환선,
    서산영덕선/대전남부순환선, 영동선/광주외곽순환선 등). 이제는 4자리(rest 표기)일
    때만 끝자리 0을 정확히 한 번 떼어 3자리(unit 표기)로 맞추고, 그다음 앞자리 0만
    lstrip해서 정규화한다 — 실제로 있는 자릿수 차이만 보정하고 그 이상 뭉개지 않는다.
    프론트엔드 rest-stop-finder.html의 routeKey()와 반드시 동일한 규칙을 유지할 것."""
    s = (route_no or "").strip()
    if not s:
        return "0"
    if len(s) == 4 and s.endswith("0"):
        s = s[:-1]
    s = s.lstrip("0")
    return s or "0"


def _unit_sort_key(u):
    """unitCode 숫자값으로 정렬 — 노선 내 시작점을 고르는 용도로만 쓴다(예: 최소
    코드를 nearest-neighbor 정렬의 출발점으로 삼음). unitCode 순서가 실제 지리적
    순서와 항상 일치하지는 않아(검증 결과 route 001 경부선에서 최대 284km 점프
    발생), 최종 이음 순서는 _order_units_nearest_neighbor()가 좌표 기반으로 정한다."""
    try:
        return int((u.get("unitCode") or "").strip())
    except Exception:
        return 999999


# 이 거리(km) 이상 떨어진 두 톨게이트는 실제로 도로로 안 이어져 있다고 보고
# 폴리라인을 끊는다(같은 노선이라도 지선·번호 재사용 등으로 멀리 떨어진 톨게이트가
# 섞여 있을 수 있음). 검증 결과 정상 인접 구간은 대부분 수 km~20km대라 40km는
# 충분히 보수적인 컷오프다.
_ROUTE_JUMP_THRESHOLD_KM = 40.0


def _order_units_nearest_neighbor(units):
    """좌표 기반 최근접 이웃 그리디 정렬. unitCode 최소값을 시작점으로 잡고,
    매번 남은 톨게이트 중 가장 가까운 것을 다음으로 잇는다. unitCode 순서가
    지리적 순서와 어긋나는 노선도 실제 도로 형상에 더 가깝게 나온다."""
    remaining = list(units)
    if not remaining:
        return []
    remaining.sort(key=_unit_sort_key)
    ordered = [remaining.pop(0)]
    while remaining:
        cur = ordered[-1]
        clat, clng = _to_float(cur.get("yValue")), _to_float(cur.get("xValue"))
        best_idx, best_dist = None, None
        for i, cand in enumerate(remaining):
            d = _haversine(clat, clng, _to_float(cand.get("yValue")), _to_float(cand.get("xValue")))
            if d is None:
                continue
            if best_dist is None or d < best_dist:
                best_idx, best_dist = i, d
        if best_idx is None:
            break
        ordered.append(remaining.pop(best_idx))
    return ordered


def _split_into_segments(ordered_units, threshold_km=_ROUTE_JUMP_THRESHOLD_KM):
    """정렬된 톨게이트 목록을 연속 구간(segment)들로 쪼갠다 — 인접 두 점 사이
    거리가 threshold_km를 넘으면(가지 노선 등으로 실제 안 이어지는 구간) 그
    지점에서 선을 끊어 새 segment를 시작한다."""
    if not ordered_units:
        return []
    segments = []
    cur = [ordered_units[0]]
    for prev, u in zip(ordered_units, ordered_units[1:]):
        d = _haversine(_to_float(prev.get("yValue")), _to_float(prev.get("xValue")),
                        _to_float(u.get("yValue")), _to_float(u.get("xValue")))
        if d is not None and d > threshold_km:
            if len(cur) >= 2:
                segments.append(cur)
            cur = [u]
        else:
            cur.append(u)
    if len(cur) >= 2:
        segments.append(cur)
    return segments


def build_route_lines():
    """routeNo별 톨게이트 좌표를 좌표 기반 최근접 이웃 순서로 이은 뒤, 비정상적으로
    먼 구간(가지 노선 등)에서 끊어 여러 개의 연속 segment로 만든다. 반환값의 각
    노선은 "segments": [[ [lat,lng], ... ], ...] (2차원 배열)를 담는다."""
    units = _cached_data("units")
    by_route = {}
    for u in units:
        lat, lng = _to_float(u.get("yValue")), _to_float(u.get("xValue"))
        if lat is None or lng is None:
            continue
        route_no = (u.get("routeNo") or "").strip()
        if not route_no:
            continue
        key = _route_key(route_no)
        by_route.setdefault(key, {"routeNo": route_no, "routeName": u.get("routeName"), "units": []})
        by_route[key]["units"].append(u)

    lines = {}
    for key, info in by_route.items():
        ordered = _order_units_nearest_neighbor(info["units"])
        seg_units = _split_into_segments(ordered)
        segments = [
            [[_to_float(u.get("yValue")), _to_float(u.get("xValue"))] for u in seg]
            for seg in seg_units
        ]
        if not segments:
            continue
        lines[key] = {
            "routeNo": info["routeNo"],
            "routeName": info["routeName"],
            "segments": segments,
            "unit_count": len(ordered),
        }
    return lines


OSRM_URL = "https://router.project-osrm.org/route/v1/driving/{lng1},{lat1};{lng2},{lat2}"
OSRM_UA = {"User-Agent": "golf-automation-restarea-tool/1.0 (personal project)"}
OSRM_TIMEOUT = 8
OSRM_SLEEP_SEC = 0.25

ROAD_GEOMETRY_FILE = CACHE_DIR / "road_geometry.json"
_ROAD_GEOMETRY_TTL = 30 * 24 * 3600  # 30일 — OSRM 크롤링은 무거워서 사실상 영구 캐시


# OSRM이 톨게이트 진출입로/지방도로 우회해 본선을 벗어났다 들어오는 경로를 잡는 경우가
# 있어서(공개 데모 서버는 고속도로 본선 강제 옵션이 없는 일반 driving 프로파일이라),
# "실제 경로거리 / 직선거리" 비율로 이상 우회를 걸러낸다. 인접 톨게이트 구간은 정상적으로도
# 도로가 휘어 있어 비율이 1.0~1.3대가 보통이라, 이보다 뚜렷이 큰 경우만 우회로 간주한다.
_OSRM_DETOUR_RATIO_MAX = 1.6


# 톨게이트 좌표에 OSRM 스냅 반경을 좁게 걸어(본선 근처로 강제 스냅) 진출입로/휴게소
# 안쪽으로 새는 것을 줄인다. 너무 좁으면(예: 80m) 톨게이트가 본선에서 좀 떨어진
# 경우 "NoSegment"로 실패하므로, 150m로 시도하고 실패하면 반경 없이 재시도한다.
_OSRM_SNAP_RADIUS_M = 150


def _has_loop(coords, close_km=0.08, min_path_km=0.8, max_checked=400):
    """경로가 시작점 근처로 되돌아오는 "루프"(휴게소/톨게이트 진입로로 들어갔다가
    U턴하듯 돌아나오는 패턴)를 탐지한다.

    단순히 "인덱스가 몇 개 이상 떨어진 두 점이 가까우면 루프"로 보면, 점 밀도가
    높은 고속도로의 정상적인 완만한 곡선(같은 곡선 안의 인접 점들은 원래 서로
    가까움)까지 오탐하게 된다(실측 — min_index_gap만 쓴 초판은 349쌍 중 267쌍을
    루프로 오판). 그래서 "인덱스 차이" 대신 "그 사이 실제로 이동한 누적 경로거리
    (min_path_km 이상)"를 기준으로 삼는다 — 최소 min_path_km(기본 800m)를 달려
    나간 뒤에도 close_km(기본 80m) 이내의 이전 지점으로 되돌아오면 그건 정상
    곡선이 아니라 실제로 왕복(U턴)한 것으로 본다(1차 실측 — 0.4km/0.15m 기준은
    산악구간 완만한 S커브까지 오탐해 349쌍 중 135쌍을 잘못 걸렀다 — 0.8km/0.08km로
    강화). 점이 아주 많은 구간은 성능을
    위해 균등 샘플링해서 검사한다(단, min_path_km 규모의 루프는 샘플링해도
    남아있을 만큼 크다)."""
    n = len(coords)
    if n < 3:
        return False
    pts = coords
    if n > max_checked:
        step = n / float(max_checked)
        pts = [coords[int(i * step)] for i in range(max_checked)]
        n = len(pts)
    cum = [0.0] * n
    for i in range(1, n):
        d = _haversine(pts[i - 1][0], pts[i - 1][1], pts[i][0], pts[i][1]) or 0.0
        cum[i] = cum[i - 1] + d
    for i in range(n):
        for j in range(i + 1, n):
            if cum[j] - cum[i] < min_path_km:
                continue
            d = _haversine(pts[i][0], pts[i][1], pts[j][0], pts[j][1])
            if d is not None and d < close_km:
                return True
    return False


def _fetch_osrm_route(lat1, lng1, lat2, lng2, use_radius=True):
    """OSRM 공개 데모 서버에서 두 지점 사이 실제 도로 형상을 가져온다.
    실패(비정상 응답·타임아웃·예외), 직선거리 대비 과도한 우회(본선 이탈 의심),
    또는 경로가 자기 자신 근처로 되돌아오는 루프(휴게소/진입로 왕복 의심) 시
    None을 반환하고 절대 예외를 올리지 않는다.
    use_radius=True면 톨게이트 좌표 반경 150m로 본선 스냅을 유도하고, OSRM이
    "NoSegment" 등으로 거부하면(반경이 너무 좁아 스냅 실패) 반경 없이 1회 재시도한다."""
    try:
        url = OSRM_URL.format(lng1=lng1, lat1=lat1, lng2=lng2, lat2=lat2)
        params = {"overview": "full", "geometries": "geojson"}
        if use_radius:
            params["radiuses"] = f"{_OSRM_SNAP_RADIUS_M};{_OSRM_SNAP_RADIUS_M}"
        r = requests.get(url, params=params, headers=OSRM_UA, timeout=OSRM_TIMEOUT)
        if r.status_code != 200:
            if use_radius:
                return _fetch_osrm_route(lat1, lng1, lat2, lng2, use_radius=False)
            return None
        d = r.json()
        if d.get("code") != "Ok":
            if use_radius:
                return _fetch_osrm_route(lat1, lng1, lat2, lng2, use_radius=False)
            return None
        routes = d.get("routes") or []
        if not routes:
            return None
        route_distance_m = routes[0].get("distance")
        coords = routes[0].get("geometry", {}).get("coordinates") or []
        if not coords:
            return None
        straight_km = _haversine(lat1, lng1, lat2, lng2)
        if route_distance_m is not None and straight_km:
            ratio = (route_distance_m / 1000.0) / straight_km
            if ratio > _OSRM_DETOUR_RATIO_MAX:
                return None  # 본선 이탈 우회로 의심 — 직선 폴백에 맡긴다
        latlng = [[c[1], c[0]] for c in coords]  # [lng,lat] -> [lat,lng]
        if _has_loop(latlng):
            return None  # 휴게소/진입로 왕복 루프 의심 — 비율과 무관하게 직선 폴백
        return latlng
    except Exception:
        return None


def _load_road_geometry_cache():
    if not ROAD_GEOMETRY_FILE.exists():
        return None
    try:
        payload = json.loads(ROAD_GEOMETRY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None
    if (time.time() - payload.get("fetched_at", 0)) > _ROAD_GEOMETRY_TTL:
        return None
    return payload.get("data")


def _save_road_geometry_cache(data):
    payload = {"fetched_at": time.time(), "data": data}
    ROAD_GEOMETRY_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return payload


def build_road_geometry():
    """모든 노선의 세그먼트 톨게이트 인접쌍마다 OSRM으로 실제 도로 형상을 가져와
    build_route_lines()와 동일한 {routeKey: {routeNo, routeName, segments, unit_count}}
    구조로 만든다. 실패한 쌍은 직선(톨게이트 두 점)으로 폴백한다.

    무거운 작업(수백 건의 외부 HTTP 호출, 수 분 소요)이라 서버 기동·일반 요청 중에는
    자동 호출되지 않는다. 수동 실행:
        .venv\\Scripts\\python.exe -c "from tools.restarea_helpers import build_road_geometry; build_road_geometry()"
    """
    units = _cached_data("units")
    by_route = {}
    for u in units:
        lat, lng = _to_float(u.get("yValue")), _to_float(u.get("xValue"))
        if lat is None or lng is None:
            continue
        route_no = (u.get("routeNo") or "").strip()
        if not route_no:
            continue
        key = _route_key(route_no)
        by_route.setdefault(key, {"routeNo": route_no, "routeName": u.get("routeName"), "units": []})
        by_route[key]["units"].append(u)

    start_time = time.time()
    total_pairs = 0
    osrm_success = 0
    fallback_count = 0

    lines = {}
    for key, info in by_route.items():
        ordered = _order_units_nearest_neighbor(info["units"])
        seg_units = _split_into_segments(ordered)
        segments = []
        for seg in seg_units:
            coords = [[_to_float(seg[0].get("yValue")), _to_float(seg[0].get("xValue"))]]
            for prev, cur in zip(seg, seg[1:]):
                total_pairs += 1
                lat1, lng1 = _to_float(prev.get("yValue")), _to_float(prev.get("xValue"))
                lat2, lng2 = _to_float(cur.get("yValue")), _to_float(cur.get("xValue"))
                road = _fetch_osrm_route(lat1, lng1, lat2, lng2)
                time.sleep(OSRM_SLEEP_SEC)
                if road:
                    osrm_success += 1
                    # 첫 점은 이미 coords 끝에 있는 join point와 중복되므로 스킵
                    coords.extend(road[1:])
                else:
                    fallback_count += 1
                    coords.append([lat2, lng2])
                if total_pairs % 20 == 0:
                    print(f"[build_road_geometry] {total_pairs}쌍 처리, OSRM성공={osrm_success} 폴백={fallback_count}")
            segments.append(coords)
        if not segments:
            continue
        lines[key] = {
            "routeNo": info["routeNo"],
            "routeName": info["routeName"],
            "segments": segments,
            "unit_count": len(ordered),
        }

    elapsed = time.time() - start_time
    print(f"[build_road_geometry] 완료 — 총 {total_pairs}쌍, OSRM성공 {osrm_success}, 폴백 {fallback_count}, "
          f"소요 {elapsed:.1f}초")
    _save_road_geometry_cache(lines)
    return lines


def repair_road_geometry_loops():
    """이미 생성된 road_geometry.json 캐시를 전체 재빌드 없이 점검한다. 저장된
    segments는 인접 톨게이트쌍을 이어붙인 결과라 쌍 경계가 좌표에 남아있지 않으므로,
    units 데이터로 같은 순서(정렬+구간분리) 를 다시 계산해 각 쌍의 시작/끝 좌표를
    복원하고, 그 쌍에 해당하는 좌표 구간만 골라 _has_loop()로 재검사한다. 루프가
    남아있는 쌍만 OSRM을 다시 호출(반경 150m 스냅 우선)하거나, 그래도 안 되면
    직선으로 교체한 뒤 해당 구간만 캐시에 반영한다(전체 재빌드 아님).

    반환: {"checked_pairs": N, "fixed": [...], "still_bad": [...]}
    """
    cache = _load_road_geometry_cache()
    if not cache:
        return {"checked_pairs": 0, "fixed": [], "still_bad": [], "error": "no cache"}

    units = _cached_data("units")
    by_route = {}
    for u in units:
        lat, lng = _to_float(u.get("yValue")), _to_float(u.get("xValue"))
        if lat is None or lng is None:
            continue
        route_no = (u.get("routeNo") or "").strip()
        if not route_no:
            continue
        key = _route_key(route_no)
        by_route.setdefault(key, []).append(u)

    fixed = []
    still_bad = []
    checked_pairs = 0

    for key, rt in cache.items():
        route_units = by_route.get(key)
        if not route_units:
            continue
        ordered = _order_units_nearest_neighbor(route_units)
        seg_units = _split_into_segments(ordered)
        segments = rt.get("segments") or []
        if len(seg_units) != len(segments):
            # 캐시 생성 이후 units 구성이 바뀐 경우 — 경계를 신뢰할 수 없어 스킵.
            continue
        for seg_idx, (units_in_seg, coords) in enumerate(zip(seg_units, segments)):
            pairs = list(zip(units_in_seg, units_in_seg[1:]))
            if not pairs:
                continue
            # 저장된 coords는 join point를 공유하며 이어붙여져 있어 실제 점 개수가
            # 톨게이트 개수보다 많다. 쌍 경계를 정확히 못 나누므로, 쌍별로 새로
            # 좌표를 뽑아내는 대신 "그 쌍의 시작·끝 좌표가 coords 안에서 가장 가까운
            # 위치" 사이의 부분열을 그 쌍의 구간으로 근사한다.
            # 좌표는 쌍 순서대로 이어붙여져 있어 실제 경계가 cursor 근방에 있다.
            # 전체 좌표 배열을 매번 스캔하면(수만 점) O(쌍수 × 점수)로 너무 느려지므로,
            # cursor 기준 앞으로 최대 window개 점 안에서만 최근접점을 찾는다(좌표 순서가
            # 단조로우므로 충분).
            _NEAREST_WINDOW = 3000

            def _nearest_index(lat, lng, start_from=0):
                end = min(len(coords), start_from + _NEAREST_WINDOW)
                best_i, best_d = None, None
                for i in range(start_from, end):
                    d = _haversine(lat, lng, coords[i][0], coords[i][1])
                    if d is not None and (best_d is None or d < best_d):
                        best_i, best_d = i, d
                return best_i if best_i is not None else start_from

            cursor = 0
            for u1, u2 in pairs:
                checked_pairs += 1
                lat1, lng1 = _to_float(u1.get("yValue")), _to_float(u1.get("xValue"))
                lat2, lng2 = _to_float(u2.get("yValue")), _to_float(u2.get("xValue"))
                i1 = _nearest_index(lat1, lng1, cursor)
                i2 = _nearest_index(lat2, lng2, i1)
                sub = coords[i1:i2 + 1]
                cursor = i2
                pair_name = f"{u1.get('unitName')}↔{u2.get('unitName')}"
                # len(sub) <= 2인 쌍은 과거(1차 실행, 오탐이 많았던 구판 _has_loop)에
                # 잘못 직선으로 치환됐을 수 있어(정상 곡선을 루프로 오판) 복구를
                # 시도한다 — 재요청 후 개선된 _has_loop 기준으로 다시 검증한다.
                if len(sub) > 2 and not _has_loop(sub):
                    continue  # 문제 없음
                # 루프 의심(또는 과거 오탐으로 직선 치환됨) — 이 쌍만 재요청.
                road = _fetch_osrm_route(lat1, lng1, lat2, lng2, use_radius=True)
                if road and not _has_loop(road):
                    new_coords = coords[:i1] + road + coords[i2 + 1:]
                    fixed.append({"route": rt.get("routeName") or key, "pair": pair_name,
                                  "note": "OSRM 도로 형상으로 복구/교체"})
                else:
                    straight = [[lat1, lng1], [lat2, lng2]]
                    new_coords = coords[:i1] + straight + coords[i2 + 1:]
                    if len(sub) > 2:
                        # 실제로 상태가 바뀜(곡선 → 직선) — 여전히 문제로 보고.
                        still_bad.append({
                            "route": rt.get("routeName") or key,
                            "pair": pair_name,
                            "note": "OSRM 재시도 후에도 루프/실패 — 직선으로 교체",
                        })
                segments[seg_idx] = new_coords
                coords = new_coords
        rt["segments"] = segments

    _save_road_geometry_cache(cache)
    return {"checked_pairs": checked_pairs, "fixed": fixed, "still_bad": still_bad}


_ROUTE_BREAKS_CACHE = {}  # route_key -> [{"unit_codes": [...], "breaks": [...], "coords": [...]}] or None


def _compute_route_unit_breaks(key):
    """road_geometry(또는 근사 route_lines)의 segments 좌표 배열 안에서, 각 톨게이트가
    실제로 몇 번째 좌표 인덱스에 해당하는지 복원한다. segments는 톨게이트 인접쌍을
    이어붙인 결과라 경계가 좌표에 남아있지 않으므로, units를 build 시와 동일한 순서
    (좌표 기반 최근접 이웃 정렬 + 40km 컷 분리)로 재계산해 좌표 배열과 1:1 대응시키고,
    각 톨게이트 좌표에 가장 가까운 인덱스를 순서대로(커서 전진) 찾는다 — 좌표가
    이어붙인 순서대로 단조 진행하므로 커서 방식으로 충분하다(repair_road_geometry_loops의
    쌍별 매칭과 동일한 기법)."""
    lines = get_route_lines()
    line = lines.get(key)
    if not line:
        return None
    units = _cached_data("units")
    # build_route_lines()/build_road_geometry()와 동일하게 좌표 없는 unit은 제외해야
    # 순서·개수가 캐시 생성 시점과 일치한다 — 안 걸러내면 좌표 None인 항목이 정렬
    # 맨 앞으로 와 _order_units_nearest_neighbor가 거리 계산 불가로 1건만 남기고
    # 멈춰버려(경부선 등 주요 노선에서 실측) breaks 매칭이 전부 실패했었다.
    route_units = [
        u for u in units
        if _route_key(u.get("routeNo")) == key
        and _to_float(u.get("yValue")) is not None
        and _to_float(u.get("xValue")) is not None
    ]
    if not route_units:
        return None
    ordered = _order_units_nearest_neighbor(route_units)
    seg_units = _split_into_segments(ordered)
    segments_coords = line.get("segments") or []
    if len(seg_units) != len(segments_coords):
        return None  # 캐시 세대 불일치(units 구성이 바뀜) — 서브패스 매칭 불가, 직선 폴백

    _NEAREST_WINDOW = 3000
    result_segments = []
    for units_in_seg, coords in zip(seg_units, segments_coords):
        if not coords:
            continue

        def _nearest_index(lat, lng, start_from, coords=coords):
            end = min(len(coords), start_from + _NEAREST_WINDOW)
            best_i, best_d = start_from, None
            for i in range(start_from, end):
                d = _haversine(lat, lng, coords[i][0], coords[i][1])
                if d is not None and (best_d is None or d < best_d):
                    best_i, best_d = i, d
            return best_i

        codes, breaks = [], []
        cursor = 0
        for u in units_in_seg:
            lat, lng = _to_float(u.get("yValue")), _to_float(u.get("xValue"))
            if lat is None or lng is None:
                continue
            idx = _nearest_index(lat, lng, cursor)
            codes.append((u.get("unitCode") or "").strip())
            breaks.append(idx)
            cursor = idx
        result_segments.append({"unit_codes": codes, "breaks": breaks, "coords": coords})
    return result_segments


def _get_route_unit_breaks(key):
    if key not in _ROUTE_BREAKS_CACHE:
        try:
            _ROUTE_BREAKS_CACHE[key] = _compute_route_unit_breaks(key)
        except Exception:
            _ROUTE_BREAKS_CACHE[key] = None
    return _ROUTE_BREAKS_CACHE[key]


def get_road_subpath(route_no, from_code, to_code):
    """두 톨게이트(unitCode) 사이의 실제 도로 형상 좌표 배열([lat,lng], ...)을
    반환한다. get_route_lines()가 주는 노선 지오메트리(①표준노드링크 ②공식
    도로선형 ③OSRM ④직선, 우선순위대로 병합된 것)에 두 톨게이트 좌표를 shapely로
    스냅해서 그 사이 구간만 잘라낸다.

    예전엔 "톨게이트 개수와 저장된 segment 개수가 정확히 같아야" 좌표 인덱스를
    역산하는 방식(_compute_route_unit_breaks)을 우선 썼는데, 표준노드링크 도입
    후 노선마다 조각을 잇는 방식이 소스별로 달라(노드링크는 endpoint-nearest
    체이닝, 기존 톨게이트 기반은 40km 컷 분리) segment 개수가 항상 어긋나
    **전 구간이 예외 없이 None**을 반환하는 버그가 있었다(2026-09-02 실측 —
    콘존 470개 전부 직선 폴백). shapely `project`/`distance` 스냅은 개수 일치가
    필요 없어 이 문제가 없다 — 이제 이 방식 하나로 통일한다."""
    key = _route_key(route_no)
    from_code = (from_code or "").strip()
    to_code = (to_code or "").strip()
    if not from_code or not to_code:
        return None

    units_by_code = _units_by_code()
    u1, u2 = units_by_code.get(from_code), units_by_code.get(to_code)
    if not u1 or not u2:
        return None

    route_entry = get_route_lines().get(key)
    if not route_entry:
        return None
    try:
        return _official_subpath(route_entry, u1, u2)
    except Exception:
        return None


def _conzone_path_or_fallback(route_no, u1, u2, raw_name1=None, raw_name2=None):
    """콘존 구간 좌표 산출 우선순위: ①표준노드링크 그래프 Dijkstra(get_conzone_graph_path)
    ②기존 shapely 스냅(get_road_subpath) ③None(호출측/프론트 직선 폴백).
    u1/u2는 unitName/yValue/xValue를 담은 dict(단, unitCode는 폴백 매칭 시 None일
    수 있음 — get_road_subpath는 unitCode 필요하므로 그 경우 자동으로 스킵됨).

    raw_name1/raw_name2(2026-09-02 추가): 콘존명을 "-"로 나눈 원본 조각("천안JC" 등,
    IC/JC 접미사 보존)을 그래프 노드 매칭에 우선 사용한다. u1/u2.unitName은 이미
    IC/JC가 제거된 상태라("천안") 그걸로 그래프를 찾으면 같은 이름을 쓰는 IC와 JC가
    실제로는 다른 지점인데도 하나로 합쳐져 id1==id2로 거부되는 사례가 많았다(실측
    172개 잔여 직선 중 92개). 원본 접미사를 유지한 이름을 우선 시도하고, 없으면
    (raw_name 미전달 등 하위호환) 기존 stripped 이름으로 폴백한다."""
    name1 = (raw_name1 or "").strip() or _strip_ic_suffix(u1.get("unitName"))
    name2 = (raw_name2 or "").strip() or _strip_ic_suffix(u2.get("unitName"))
    lat1, lng1 = _to_float(u1.get("yValue")), _to_float(u1.get("xValue"))
    lat2, lng2 = _to_float(u2.get("yValue")), _to_float(u2.get("xValue"))
    try:
        graph_path = get_conzone_graph_path(
            name1, name2,
            ref1=(lat1, lng1) if lat1 is not None else None,
            ref2=(lat2, lng2) if lat2 is not None else None,
        )
        if not graph_path and raw_name1 and raw_name2:
            # 원본 접미사 이름으로 실패하면(예: shp에 그 표기가 없음) 기존 stripped
            # 이름으로 한 번 더 시도 — 접미사 보존이 항상 이득은 아닐 수 있어 안전망.
            stripped1 = _strip_ic_suffix(u1.get("unitName"))
            stripped2 = _strip_ic_suffix(u2.get("unitName"))
            if stripped1 != name1 or stripped2 != name2:
                graph_path = get_conzone_graph_path(
                    stripped1, stripped2,
                    ref1=(lat1, lng1) if lat1 is not None else None,
                    ref2=(lat2, lng2) if lat2 is not None else None,
                )
    except Exception:
        graph_path = None
    if graph_path:
        return graph_path
    code1, code2 = u1.get("unitCode"), u2.get("unitCode")
    if code1 and code2:
        try:
            sub_path = get_road_subpath(route_no, code1, code2)
        except Exception:
            sub_path = None
        # 2026-09-02 사고 조사 — get_road_subpath(shapely 스냅 폴백)에는 그래프
        # 경로용으로 추가한 수직 이격거리 sanity check가 없어, 그래프 경로가
        # 거부된 콘존이 이 폴백을 거쳐 여전히 엉뚱한 경로로 나가는 구멍이 있었다
        # (실측 — 진해IC-대청IC 등 43건이 그래프 필터를 우회해 통과). 같은 기준을
        # 여기서도 적용한다.
        if sub_path and lat1 is not None and lat2 is not None:
            straight_km = _haversine(lat1, lng1, lat2, lng2)
            if straight_km and straight_km > 0:
                max_dev = _max_perp_deviation_km(sub_path, lat1, lng1, lat2, lng2)
                dev_limit = max(_GRAPH_PATH_MAX_DEV_MIN_KM, straight_km * _GRAPH_PATH_MAX_DEV_RATIO)
                if max_dev > dev_limit:
                    sub_path = None
        return sub_path
    return None


OFFICIAL_ROAD_GEOMETRY_FILE = CACHE_DIR / "official_road_geometry.json"
ROAD_SHP_PATH = CACHE_DIR / "road_shp_raw" / "도로노선SHP" / "도로중심선.shp"
_OFFICIAL_ROAD_GEOMETRY_CACHE = {"data": None, "loaded": False}

# 같은 노선의 shapefile 조각(named segment)을 이어붙일 때, 두 조각 끝점 사이가
# 이 거리(km) 이내면 실제로 연결된 도로로 보고 잇는다. 실측(2026-09-02) — 남해선처럼
# 정식 구간 경계에서 최대 13.8km 정도 끊겨 있는 경우가 있어 40km(톨게이트 분리 기준)와
# 동일하게 넉넉히 잡는다. 이미 노선명으로 그룹핑된 조각들끼리만 비교하므로 다른
# 노선과 잘못 이어붙을 위험은 없다.
_SHP_PIECE_JOIN_THRESHOLD_KM = 40.0

# 톨게이트 좌표를 공식 도로선에 스냅할 때 허용하는 최대 거리(도, degree). 약 0.05도
# ≈ 5.5km — 톨게이트가 본선에서 진입로/휴게소 쪽으로 떨어져 있는 경우까지 커버.
_OFFICIAL_SNAP_MAX_DEG = 0.05


def _normalize_route_name(name):
    """도로공사 노선명 표기 차이를 정규화한다: 괄호 구간설명 제거("남해선(순천~부산)"
    → "남해선"), 공백/하이픈 제거, "의지선" → "지선"(구어체 차이), 끝의 A/B 이형
    표기 문자 제거("남해선A" → "남해선"). 실측(2026-09-02) — locationinfoUnit의
    routeName과 도로중심선.shp의 name 필드가 이 규칙으로 39/46 조각이 정확히
    일치했다(family-prefix 방식은 600↔688처럼 오매칭이 나와 폐기)."""
    if not name:
        return ""
    n = re.sub(r"\([^)]*\)", "", name)
    n = n.replace(" ", "").replace("-", "").rstrip(",")
    n = re.sub(r"의지선$", "지선", n)
    n = re.sub(r"[A-Za-z]$", "", n)
    return n


def _merge_line_pieces(pieces, threshold_km=_SHP_PIECE_JOIN_THRESHOLD_KM):
    """이미 같은 노선명으로 묶인 폴리라인 조각들(각각 [[lat,lng],...])을 끝점 최근접
    기준으로 이어붙여 연속된 segment 목록을 만든다. 두 조각의 끝점 4가지 조합(양쪽
    끝 모두) 중 가장 가까운 조합이 threshold_km 이내면 그 방향으로 이어붙이고,
    더 이상 이을 수 없으면 그 chain을 확정하고 남은 조각으로 새 chain을 시작한다."""
    remaining = [list(p) for p in pieces if p and len(p) >= 2]
    segments = []
    while remaining:
        chain = remaining.pop(0)
        changed = True
        while changed and remaining:
            changed = False
            c_start, c_end = chain[0], chain[-1]
            best = None  # (idx, dist, mode)
            for i, p in enumerate(remaining):
                p_start, p_end = p[0], p[-1]
                combos = (
                    ("end_start", _haversine(c_end[0], c_end[1], p_start[0], p_start[1])),
                    ("end_end", _haversine(c_end[0], c_end[1], p_end[0], p_end[1])),
                    ("start_start", _haversine(c_start[0], c_start[1], p_start[0], p_start[1])),
                    ("start_end", _haversine(c_start[0], c_start[1], p_end[0], p_end[1])),
                )
                for mode, d in combos:
                    if d is not None and (best is None or d < best[1]):
                        best = (i, d, mode)
            if best and best[1] <= threshold_km:
                i, d, mode = best
                p = remaining.pop(i)
                if mode == "end_start":
                    chain = chain + p
                elif mode == "end_end":
                    chain = chain + list(reversed(p))
                elif mode == "start_start":
                    chain = list(reversed(p)) + chain
                else:  # start_end
                    chain = p + chain
                changed = True
        segments.append(chain)
    return segments


def build_official_road_geometry():
    """도로공사 공식 도로선형 shapefile(도로중심선.shp)을 읽어 노선별 폴리라인을
    만들고 official_road_geometry.json에 저장한다. 기존 road_geometry.json(OSRM
    근사)은 건드리지 않는다.

    매칭: shapefile의 46개 피처는 이미 고속도로만 담고 있어(전국 도로 통합본이 아님
    — 일반도로/국도 피처 없음, 실측 확인) 별도 도로등급 필터링이 불필요하다.
    노선 식별은 라우트번호 자릿수 방식(예: shp "0600"의 앞 3자리 "600")이 아니라
    노선명 정규화 매칭을 1차로 쓴다 — 자릿수 방식은 shp "6000"(부산외곽순환선)이
    unit routeNo "600"(무안광주선)과 잘못 엮이는 등 오매칭이 발생했다(2026-09-02
    실측, family-prefix 방식 폐기 근거).

    반환: {"total_shp_features": N, "matched_routes": [...], "unmatched_units": [...]}
    """
    import geopandas as gpd  # 무거운 의존성 — 수동 재빌드 시에만 로드

    if not ROAD_SHP_PATH.exists():
        raise FileNotFoundError(f"shapefile not found: {ROAD_SHP_PATH}")

    gdf = gpd.read_file(str(ROAD_SHP_PATH), encoding="cp949")
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:3857")
    gdf = gdf.to_crs("EPSG:4326")  # WGS84 위경도로 변환

    units = _cached_data("units")
    unit_routes = {}  # routeNo(원본, 3자리) -> routeName
    for u in units:
        rn = (u.get("routeNo") or "").strip()
        if rn and rn not in unit_routes:
            unit_routes[rn] = u.get("routeName")

    # 정규화명 -> [routeNo, ...] (동명이인 없음, 2026-09-02 실측 확인)
    norm_to_routeno = {}
    for rn, name in unit_routes.items():
        norm_to_routeno.setdefault(_normalize_route_name(name), []).append(rn)

    # routeNo별로 매칭된 shapefile 피처의 좌표 조각들을 모은다.
    pieces_by_routeno = {}
    matched_feature_names = []
    unmatched_feature_names = []

    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None:
            continue
        raw_name = row.get("name")
        norm = _normalize_route_name(raw_name)
        cands = norm_to_routeno.get(norm)
        if not cands:
            unmatched_feature_names.append(raw_name)
            continue
        route_no = cands[0]
        matched_feature_names.append((raw_name, route_no, unit_routes.get(route_no)))

        if geom.geom_type == "LineString":
            sub_geoms = [geom]
        elif geom.geom_type == "MultiLineString":
            sub_geoms = list(geom.geoms)
        else:
            continue
        for g in sub_geoms:
            coords = [[lat, lng] for lng, lat in g.coords]  # (lng,lat) -> [lat,lng]
            if len(coords) >= 2:
                pieces_by_routeno.setdefault(route_no, []).append(coords)

    result = {}
    for route_no, pieces in pieces_by_routeno.items():
        segments = _merge_line_pieces(pieces)
        if not segments:
            continue
        key = _route_key(route_no)
        # 이 노선의 톨게이트 개수(참고용 — 공식 지오메트리는 톨게이트 순서와 무관하게
        # shapefile 조각을 이어붙인 것이라 unit_count는 정보용일 뿐 segment 개수와
        # 대응하지 않는다).
        unit_count = sum(1 for u in units if _route_key(u.get("routeNo")) == key)
        result[key] = {
            "routeNo": route_no,
            "routeName": unit_routes.get(route_no),
            "segments": segments,
            "unit_count": unit_count,
        }

    matched_routenos = set(pieces_by_routeno.keys())
    unmatched_units = [
        {"routeNo": rn, "routeName": name}
        for rn, name in unit_routes.items() if rn not in matched_routenos
    ]

    payload = {"fetched_at": time.time(), "data": result}
    OFFICIAL_ROAD_GEOMETRY_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    _OFFICIAL_ROAD_GEOMETRY_CACHE["data"] = result
    _OFFICIAL_ROAD_GEOMETRY_CACHE["loaded"] = True

    summary = {
        "total_shp_features": len(gdf),
        "matched_routes": sorted(
            [{"routeNo": rn, "routeName": unit_routes.get(rn), "segments": len(pieces_by_routeno[rn])}
             for rn in matched_routenos],
            key=lambda x: x["routeNo"],
        ),
        "unmatched_units": unmatched_units,
        "unmatched_shp_features": unmatched_feature_names,
    }
    print(f"[build_official_road_geometry] 완료 — shp피처 {len(gdf)}개 중 매칭 노선 "
          f"{len(matched_routenos)}개(unit 전체 {len(unit_routes)}개 중), "
          f"미매칭 unit {len(unmatched_units)}개 → {OFFICIAL_ROAD_GEOMETRY_FILE}")
    return summary


NODELINK_ROAD_GEOMETRY_FILE = CACHE_DIR / "nodelink_road_geometry.json"
NODELINK_RAW_DIR = CACHE_DIR / "nodelink_raw"
NODELINK_LINK_SHP = NODELINK_RAW_DIR / "MOCT_LINK.shp"
_NODELINK_ROAD_GEOMETRY_CACHE = {"data": None, "loaded": False}

# ── 국토교통부 전국표준노드링크 노드(MOCT_NODE.shp) — IC/JC 이름→좌표 폴백 ──
# 콘존명("OO IC-OO IC")의 절반 가량이 우리 톨게이트 좌표 목록(locationinfoUnit,
# IC/TG만 있고 JC 없음)에 이름이 없어 매칭이 안 된다(2026-09-02 실측, 전국 평균
# 32%만 매칭). MOCT_NODE.shp는 IC/JC를 포함한 전국 모든 교통 지점 좌표를 담고
# 있어(NODE_ID/NODE_TYPE/NODE_NAME 필드, CRS는 MOCT_LINK.shp와 동일한
# ITRF2000 중부원점 TM) 톨게이트 매칭 실패분의 폴백 소스로 쓴다.
# 실측(2026-09-02): 필드는 NODE_ID/NODE_TYPE/NODE_NAME/TURN_P/UPDATEDATE/
# REMARK/HIST_TYPE/HISTREMARK. NODE_TYPE 코드값별 개수 — 101(886,553, 대부분
# 일반 평면교차로), 107(140,001, 기타), 104(72,076), 102(42,572), 103(22,056),
# 106(12,430), 105(4,347). NODE_NAME이 "IC"/"JC" 접미사로 끝나는 지점은
# NODE_TYPE 코드와 무관하게 흩어져 있어(고속도로 IC/JC의 대부분은 106이지만
# 101/102/103/104에도 소량 존재, 실측 IC 4,641건 중 106이 3,572건·JC 848건
# 중 106이 742건) NODE_TYPE으로 필터링하지 않고 NODE_NAME 접미사로만 거른다.
# 같은 IC/JC 이름이 진입로별로 여러 점(최대 수십 개, 예: "여의상류IC" 31개)으로
# 중복 등록돼 있어 스트립한 이름 기준으로 좌표 평균(centroid)을 대표 좌표로 쓴다
# — 실제 IC/JC 규모(수백m~1km대)에서 이 정도 근사 오차는 매칭 용도로 충분하다.
NODELINK_NODE_SHP = NODELINK_RAW_DIR / "MOCT_NODE.shp"
NODE_NAME_COORDS_FILE = CACHE_DIR / "nodelink_ic_jc_coords.json"
_NODE_NAME_COORDS_CACHE = {"data": None, "loaded": False}
_NODE_SUFFIX_RE = re.compile(r"(IC|JC)$", re.IGNORECASE)


def build_nodelink_name_coords():
    """MOCT_NODE.shp에서 이름이 IC/JC로 끝나는 노드만 걸러 접미사를 뗀 이름
    기준으로 좌표(centroid, WGS84)를 만들어 nodelink_ic_jc_coords.json에 저장한다.
    무거운 shapefile(전국 118만여 노드) 전체 로드가 필요해 서버 기동 시 자동
    실행되지 않고, 최초 폴백 조회 시 1회만 실행돼 결과를 캐싱한다."""
    import geopandas as gpd  # 무거운 의존성 — 최초 1회/수동 재빌드 시에만 로드

    if not NODELINK_NODE_SHP.exists():
        raise FileNotFoundError(f"node shapefile not found: {NODELINK_NODE_SHP}")

    t0 = time.time()
    gdf = gpd.read_file(str(NODELINK_NODE_SHP), encoding="euc-kr")
    gdf = gdf[gdf["NODE_NAME"].str.contains(_NODE_SUFFIX_RE, na=False, regex=True)]
    gdf = gdf.to_crs("EPSG:4326")
    print(f"[build_nodelink_name_coords] IC/JC node {len(gdf)}, load {time.time() - t0:.1f}s")

    sums = {}  # stripped_name -> [lat_sum, lng_sum, count]
    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None:
            continue
        name = _strip_ic_suffix(row.get("NODE_NAME"))
        if not name:
            continue
        lng, lat = geom.x, geom.y
        s = sums.setdefault(name, [0.0, 0.0, 0])
        s[0] += lat
        s[1] += lng
        s[2] += 1

    result = {name: [round(s[0] / s[2], 6), round(s[1] / s[2], 6)] for name, s in sums.items()}
    payload = {"fetched_at": time.time(), "data": result}
    NODE_NAME_COORDS_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    _NODE_NAME_COORDS_CACHE["data"] = result
    _NODE_NAME_COORDS_CACHE["loaded"] = True
    print(f"[build_nodelink_name_coords] done, unique names {len(result)} -> {NODE_NAME_COORDS_FILE}")
    return {"total_ic_jc_nodes": len(gdf), "unique_names": len(result)}


def _load_nodelink_name_coords():
    """이름→좌표 폴백 딕셔너리를 메모리에 캐싱해 반환. 파일 캐시가 없으면
    build_nodelink_name_coords()를 최초 1회 실행해 만든다(수 초~수십 초 소요,
    이후엔 파일/메모리 캐시로 즉시 조회). shapefile이 없거나 실패하면 빈 dict —
    콘존 매칭은 조용히 기존 결과만 쓴다(지어내지 않는다)."""
    if _NODE_NAME_COORDS_CACHE["loaded"]:
        return _NODE_NAME_COORDS_CACHE["data"] or {}
    data = None
    if NODE_NAME_COORDS_FILE.exists():
        try:
            payload = json.loads(NODE_NAME_COORDS_FILE.read_text(encoding="utf-8"))
            data = payload.get("data")
        except Exception:
            data = None
    if data is None:
        try:
            build_nodelink_name_coords()
            data = _NODE_NAME_COORDS_CACHE["data"]
        except Exception as e:
            print(f"[_load_nodelink_name_coords] 빌드 실패: {e}")
            data = {}
    _NODE_NAME_COORDS_CACHE["data"] = data
    _NODE_NAME_COORDS_CACHE["loaded"] = True
    return data or {}

# 국토교통부 전국표준노드링크 조각(NODELINK) 이음 임계값. 인접 링크는 실제로
# F_NODE/T_NODE를 공유해 끝점이 사실상 같은 좌표(오차는 좌표계 변환 부동소수점
# 오차 수준)라 이 값을 작게 잡아도 되지만, 교차로에서 살짝 떨어진 진입로 연결
# 등을 감안해 0.3km로 설정한다(2026-09-02 실측 — 이 값에서 수도권제2순환선
# 1,133개 조각이 41개 연속 구간으로 합쳐짐, 나머지는 실제 미개통 구간 등 진짜 끊김).
_NODELINK_JOIN_THRESHOLD_KM = 0.3

# 표준노드링크 ROAD_RANK 필드에서 "고속도로"를 뜻하는 코드값. 실측 확인(2026-09-02,
# value_counts 상 101이 28,375건으로 로직상 매치되는 고속도로 노선명들과 정확히
# 대응 — 예: ROAD_NO '1'=경부고속도로, '65'=동해고속도로 등).
_NODELINK_EXPRESSWAY_RANK = "101"


def build_nodelink_road_geometry(target_route_nos=None):
    """국토교통부 전국표준노드링크(MOCT_LINK.shp)에서 고속도로(ROAD_RANK=101) 링크만
    걸러 노선번호(ROAD_NO)별로 모으고, 끝점 최근접 이음(_merge_line_pieces와 동일
    알고리즘)으로 연속 구간을 만들어 nodelink_road_geometry.json에 저장한다.

    도로공사 official_road_geometry.json(KEC 도로중심선 shapefile)이 노선명 매칭이라
    39/46개만 커버하고 일부(경부선 등)에 좌표 점프가 있었던 것과 달리, 표준노드링크는
    ROAD_NO 필드가 우리 locationinfoUnit의 routeNo(3자리, 앞자리 0 제거 후)와 직접
    대응해(예: '001'→'1', '065'→'65', '400'→'400') 이름 매칭보다 훨씬 신뢰도가 높다.

    target_route_nos: _route_key() 이전의 표준노드링크 ROAD_NO 문자열 리스트(예:
    ["1","65","400"]). None이면 units.json에 나오는 모든 노선번호 중 표준노드링크에
    ROAD_NO로 존재하는 것 전체를 대상으로 한다. 매칭 안 되는(ROAD_NO가 아예 없는)
    노선은 조용히 건너뛴다 — 지어내지 않는다.

    반환: {"total_expressway_links": N, "matched_routes": [...], "unmatched_route_nos": [...]}
    """
    import geopandas as gpd  # 무거운 의존성 — 수동 재빌드 시에만 로드

    if not NODELINK_LINK_SHP.exists():
        raise FileNotFoundError(f"nodelink shapefile not found: {NODELINK_LINK_SHP}")

    t0 = time.time()
    gdf = gpd.read_file(str(NODELINK_LINK_SHP), encoding="cp949",
                         where=f"ROAD_RANK='{_NODELINK_EXPRESSWAY_RANK}'")
    gdf = gdf.to_crs("EPSG:4326")
    print(f"[build_nodelink_road_geometry] 고속도로 링크 {len(gdf)}개 로드 "
          f"({time.time() - t0:.1f}s)")

    units = _cached_data("units")
    unit_routes = {}  # routeNo(원본, 3자리) -> routeName
    for u in units:
        rn = (u.get("routeNo") or "").strip()
        if rn and rn not in unit_routes:
            unit_routes[rn] = u.get("routeName")

    # route_key(표준화된 자릿수) -> 원본 unit routeNo(3~4자리). 표준노드링크 ROAD_NO는
    # 이미 앞자리 0 없는 표기라 _route_key()를 그대로 통과시키면 우리 routeNo와 맞는다.
    key_to_unit_routeno = {}
    for rn in unit_routes:
        key_to_unit_routeno.setdefault(_route_key(rn), rn)

    if target_route_nos is None:
        available_road_no = set(gdf["ROAD_NO"].astype(str).unique())
        target_route_nos = sorted(
            {rk for rk in key_to_unit_routeno if rk in available_road_no},
            key=lambda x: int(x) if x.isdigit() else 0,
        )

    result = {}
    matched_summary = []
    unmatched = []
    for road_no in target_route_nos:
        sub = gdf[gdf["ROAD_NO"].astype(str) == str(road_no)]
        if len(sub) == 0:
            unmatched.append(road_no)
            continue
        pieces = []
        for geom in sub.geometry:
            if geom is None:
                continue
            if geom.geom_type == "LineString":
                sub_geoms = [geom]
            elif geom.geom_type == "MultiLineString":
                sub_geoms = list(geom.geoms)
            else:
                continue
            for g in sub_geoms:
                coords = [[lat, lng] for lng, lat in g.coords]
                if len(coords) >= 2:
                    pieces.append(coords)

        segs = _merge_line_pieces(pieces, threshold_km=_NODELINK_JOIN_THRESHOLD_KM)
        segs = [s for s in segs if len(s) >= 3]  # 잡음(2점짜리 파편) 제거
        if not segs:
            unmatched.append(road_no)
            continue

        total_km = sum(
            _haversine(s[i - 1][0], s[i - 1][1], s[i][0], s[i][1])
            for s in segs for i in range(1, len(s))
        )
        unit_rn = key_to_unit_routeno.get(_route_key(road_no))
        key = _route_key(road_no)
        result[key] = {
            "routeNo": unit_rn or road_no,
            "routeName": unit_routes.get(unit_rn) if unit_rn else None,
            "segments": segs,
            "source": "nodelink",
        }
        matched_summary.append({
            "routeNo": unit_rn or road_no, "routeName": unit_routes.get(unit_rn) if unit_rn else None,
            "raw_link_count": len(sub), "segment_count": len(segs), "total_km": round(total_km, 1),
        })
        print(f"[build_nodelink_road_geometry] ROAD_NO={road_no} 링크{len(sub)}개 → "
              f"{len(segs)}개 구간, 총 {total_km:.1f}km")

    payload = {"fetched_at": time.time(), "data": result}
    NODELINK_ROAD_GEOMETRY_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    _NODELINK_ROAD_GEOMETRY_CACHE["data"] = result
    _NODELINK_ROAD_GEOMETRY_CACHE["loaded"] = True

    summary = {
        "total_expressway_links": len(gdf),
        "matched_routes": matched_summary,
        "unmatched_route_nos": unmatched,
    }
    print(f"[build_nodelink_road_geometry] 완료 — 대상 {len(target_route_nos)}개 중 "
          f"매칭 {len(matched_summary)}개, 미매칭 {len(unmatched)}개 → {NODELINK_ROAD_GEOMETRY_FILE}")
    return summary


def _load_nodelink_road_geometry_cache():
    """nodelink_road_geometry.json을 읽어 캐싱한다(수동 재빌드 전까지 TTL 없이 유지 —
    official_road_geometry와 동일한 정적 캐시 정책). 파일이 없으면 None."""
    if _NODELINK_ROAD_GEOMETRY_CACHE["loaded"]:
        return _NODELINK_ROAD_GEOMETRY_CACHE["data"]
    data = None
    if NODELINK_ROAD_GEOMETRY_FILE.exists():
        try:
            payload = json.loads(NODELINK_ROAD_GEOMETRY_FILE.read_text(encoding="utf-8"))
            data = payload.get("data")
        except Exception:
            data = None
    _NODELINK_ROAD_GEOMETRY_CACHE["data"] = data
    _NODELINK_ROAD_GEOMETRY_CACHE["loaded"] = True
    return data


# ── 콘존(conzone) 그래프 라우팅 — 국가표준노드링크 F_NODE/T_NODE 그래프 +
# Dijkstra 최단경로 (2026-09-02, ITS 수준 정밀도 요구로 신설) ─────────────────
# 기존 방식(_official_subpath)은 노선 전체를 하나의 이어붙인 폴리라인으로 만들고
# 콘존 양끝을 그 위에 shapely로 "스냅"하는 방식이라, 분기·순환·나들목 구간에서
# 실제로는 다른 가지로 가야 할 구간이 본선에 잘못 스냅되어 직선처럼 튀거나
# 엉뚱한 모양으로 그려지는 문제가 있었다. 이 방식은 그 대신 링크(F_NODE-T_NODE)
# 단위로 실제 도로망 그래프를 만들고, 두 IC/JC 이름을 각각 정확한 NODE_ID로
# 매칭한 뒤 그 사이를 Dijkstra 최단경로로 구해 실제 주행 가능한 경로만 그린다.
NODELINK_GRAPH_FILE = CACHE_DIR / "nodelink_graph.json"
CONZONE_PATHS_FILE = CACHE_DIR / "conzone_paths.json"
_NODELINK_GRAPH_CACHE = {"data": None, "loaded": False}
_CONZONE_PATHS_CACHE = {"data": None, "loaded": False}
_CONZONE_PATHS_DIRTY = False
_CONZONE_PATH_TTL = 24 * 3600

# 그래프 최단경로 길이가 두 지점 직선거리의 이 배수를 넘으면 "이름 매칭이 잘못돼
# 엉뚱한 노드로 튄" 것으로 보고 버린다(호출측이 기존 스냅 방식/직선으로 폴백).
_GRAPH_PATH_DETOUR_RATIO_MAX = 3.0

# 2026-09-02 사고 조사(사용자 캡처 — 굵은 정체색 선 하나가 실제 도로에서 수십~
# 백여 미터 떨어진 들판 한가운데를 지나감) — 원인 확정: _resolve_node_id가
# 이름이 같은 후보 NODE_ID 중 "ref 좌표에 가장 가까운 것"을 고르지만, IC/JC
# 진입로는 물리적으로 여러 개의 서로 다른 지점(반대 방향 진입로 등)에 같은
# 이름으로 등록돼 있어 "가장 가까운" 후보가 실제로는 반대쪽/엉뚱한 진입로일 수
# 있다. 이 경우 Dijkstra가 목적지를 지나쳐 크게 돌아오는 형태의 경로를 반환하는데
# (실측: 남안성IC-안성맞춤IC 직선 7.3km인데 실제 경로가 13.4km로 목적지를 지나쳐
# 5km 더 갔다가 되돌아옴, 진해IC-대청IC도 동일 패턴), detour_ratio(총길이/직선거리)
# 만으로는 못 걸러낼 수 있다(우회 배수가 3배 밑으로도 나올 수 있음) — 그래서
# ratio 검사와 별개로 "경로가 시작-끝 직선에서 옆으로 얼마나 벗어나는지"(최대
# 수직 이격거리)를 추가로 검사한다. 정상적인 고속도로 곡선은 이 비율이 대체로
# 0.2 이하였고(실측 — 추부IC-금산IC 등 산악 장거리 구간도 0.19), 오매칭 의심
# 구간은 0.3~1.6대까지 치솟았다. 짧은 구간에서 근소한 오차도 오탐하지 않도록
# 절대 최소치(0.5km)도 같이 둔다 — 즉 "직선거리*0.35"와 "0.5km" 중 큰 쪽을
# 넘으면 버린다.
_GRAPH_PATH_MAX_DEV_RATIO = 0.35
_GRAPH_PATH_MAX_DEV_MIN_KM = 0.5


def _max_perp_deviation_km(coords, lat1, lng1, lat2, lng2):
    """coords의 각 점이 (lat1,lng1)-(lat2,lng2) 직선에서 얼마나 벗어나는지 중
    최댓값(km, 근사). 위경도를 평면으로 근사해 정사영을 구한 뒤 haversine으로
    거리를 잰다 — 콘존 하나의 지리적 범위(수백km 이내)에서는 이 근사 오차가
    판정에 영향을 줄 만큼 크지 않다."""
    if not coords:
        return 0.0
    dx, dy = (lng2 - lng1), (lat2 - lat1)
    denom = dx * dx + dy * dy
    max_dev = 0.0
    for lat, lng in coords:
        if denom == 0:
            t = 0.0
        else:
            t = ((lng - lng1) * dx + (lat - lat1) * dy) / denom
            t = max(0.0, min(1.0, t))
        cx, cy = lng1 + t * dx, lat1 + t * dy
        d = _haversine(lat, lng, cy, cx)
        if d is not None and d > max_dev:
            max_dev = d
    return max_dev


def build_nodelink_graph():
    """국가표준노드링크 고속도로(ROAD_RANK=101) 링크 전체로 무방향 가중 그래프를
    만들어 tools/restarea_cache/nodelink_graph.json에 저장한다.

    노드: 링크의 F_NODE/T_NODE에 등장하는 모든 NODE_ID.
    간선: 링크 레코드 1건당 1개. 가중치는 좌표계 변환 후(WGS84) 정점 사이 haversine
    누적 거리(km) — shapefile의 LENGTH 필드(미터, 투영좌표계 기준)도 있지만 좌표
    자체를 최종 산출물로 이미 변환해 쓰므로 동일 좌표 기준으로 일관되게 다시 잰다.
    좌표 시퀀스는 F_NODE→T_NODE 방향으로 저장한다(역방향 주행 시 호출부에서 뒤집음).

    수동 트리거 함수 — 서버 기동/임포트 시 자동 실행되지 않는다:
        .venv\\Scripts\\python.exe -c "from tools.restarea_helpers import build_nodelink_graph; build_nodelink_graph()"
    """
    import geopandas as gpd  # 무거운 의존성 — 수동 재빌드 시에만 로드

    if not NODELINK_LINK_SHP.exists():
        raise FileNotFoundError(f"nodelink shapefile not found: {NODELINK_LINK_SHP}")

    t0 = time.time()
    gdf = gpd.read_file(str(NODELINK_LINK_SHP), encoding="cp949",
                         where=f"ROAD_RANK='{_NODELINK_EXPRESSWAY_RANK}'")
    gdf = gdf.to_crs("EPSG:4326")
    print(f"[build_nodelink_graph] 고속도로 링크 {len(gdf)}개 로드 ({time.time() - t0:.1f}s)")

    node_coords = {}  # node_id -> [lat, lng] (링크 끝점에서 직접 취득 — MOCT_NODE 재조회 불필요)
    edges = []  # [f_node, t_node, weight_km, [[lat,lng], ...]]
    skipped = 0
    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.geom_type != "LineString":
            skipped += 1
            continue
        coords = [[lat, lng] for lng, lat in geom.coords]
        if len(coords) < 2:
            skipped += 1
            continue
        f_node = str(row.get("F_NODE"))
        t_node = str(row.get("T_NODE"))
        if not f_node or not t_node or f_node == "None" or t_node == "None":
            skipped += 1
            continue
        weight_km = sum(
            _haversine(coords[i - 1][0], coords[i - 1][1], coords[i][0], coords[i][1]) or 0.0
            for i in range(1, len(coords))
        )
        node_coords.setdefault(f_node, coords[0])
        node_coords.setdefault(t_node, coords[-1])
        edges.append([f_node, t_node, round(weight_km, 5), coords])

    elapsed = time.time() - t0
    payload = {
        "fetched_at": time.time(),
        "data": {"nodes": node_coords, "edges": edges},
    }
    NODELINK_GRAPH_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    _NODELINK_GRAPH_CACHE["data"] = payload["data"]
    _NODELINK_GRAPH_CACHE["loaded"] = True
    print(f"[build_nodelink_graph] 완료 — 노드 {len(node_coords)}개, 간선 {len(edges)}개 "
          f"(스킵 {skipped}건), 소요 {elapsed:.1f}초 → {NODELINK_GRAPH_FILE}")
    return {"node_count": len(node_coords), "edge_count": len(edges), "skipped": skipped,
            "elapsed_sec": round(elapsed, 1)}


def _load_nodelink_graph():
    if _NODELINK_GRAPH_CACHE["loaded"]:
        return _NODELINK_GRAPH_CACHE["data"]
    data = None
    if NODELINK_GRAPH_FILE.exists():
        try:
            payload = json.loads(NODELINK_GRAPH_FILE.read_text(encoding="utf-8"))
            data = payload.get("data")
        except Exception:
            data = None
    _NODELINK_GRAPH_CACHE["data"] = data
    _NODELINK_GRAPH_CACHE["loaded"] = True
    return data


_GRAPH_ADJ_CACHE = {"data": None, "loaded": False}


def _graph_adjacency():
    """{node_id: [(neighbor_id, edge_idx, forward_bool), ...]} 인접리스트를
    한 번만 만들어 재사용한다(Dijkstra 반복 호출 시 매번 그래프 파싱 방지)."""
    if _GRAPH_ADJ_CACHE["loaded"]:
        return _GRAPH_ADJ_CACHE["data"], _GRAPH_ADJ_CACHE.get("edges")
    graph = _load_nodelink_graph()
    if not graph:
        _GRAPH_ADJ_CACHE["data"] = None
        _GRAPH_ADJ_CACHE["edges"] = None
        _GRAPH_ADJ_CACHE["loaded"] = True
        return None, None
    edges = graph["edges"]
    adj = {}
    for idx, (f_node, t_node, weight_km, _coords) in enumerate(edges):
        adj.setdefault(f_node, []).append((t_node, idx, True))
        adj.setdefault(t_node, []).append((f_node, idx, False))
    _GRAPH_ADJ_CACHE["data"] = adj
    _GRAPH_ADJ_CACHE["edges"] = edges
    _GRAPH_ADJ_CACHE["loaded"] = True
    return adj, edges


def _dijkstra_path(start_id, end_id):
    """heapq 기반 Dijkstra. (총거리km, [node_id, ...]) 반환, 경로 없으면 None.
    scipy.sparse.csgraph.dijkstra도 검토했으나(설치돼 있음, 2026-09-02 확인)
    간선 좌표 시퀀스를 노드쌍→edge_idx로 재조회해야 하는 건 매한가지라, 그래프
    규모(간선 약 2.8만)에서는 순수 heapq 구현이 인접간선 정보를 자연스럽게
    같이 들고 다닐 수 있어 더 단순하고 충분히 빠르다(콘존 1건당 수 ms대)."""
    import heapq

    adj, edges = _graph_adjacency()
    if not adj or start_id not in adj or end_id not in adj:
        return None
    if start_id == end_id:
        return 0.0, [start_id]

    dist = {start_id: 0.0}
    prev = {}  # node_id -> (prev_node_id, edge_idx, forward_bool)
    visited = set()
    heap = [(0.0, start_id)]
    while heap:
        d, u = heapq.heappop(heap)
        if u in visited:
            continue
        visited.add(u)
        if u == end_id:
            break
        for v, edge_idx, forward in adj.get(u, []):
            if v in visited:
                continue
            w = edges[edge_idx][2]
            nd = d + w
            if nd < dist.get(v, float("inf")):
                dist[v] = nd
                prev[v] = (u, edge_idx, forward)
                heapq.heappush(heap, (nd, v))

    if end_id not in dist:
        return None

    # 경로 역추적 + 좌표 재구성
    node_chain = [end_id]
    edge_chain = []  # (edge_idx, forward) 순서는 start->end
    cur = end_id
    while cur != start_id:
        p, edge_idx, forward = prev[cur]
        edge_chain.append((edge_idx, forward))
        node_chain.append(p)
        cur = p
    node_chain.reverse()
    edge_chain.reverse()

    coords = []
    for edge_idx, forward in edge_chain:
        seg_coords = edges[edge_idx][3]
        seg = seg_coords if forward else list(reversed(seg_coords))
        if coords and coords[-1] == seg[0]:
            coords.extend(seg[1:])
        else:
            coords.extend(seg)
    return dist[end_id], coords


_NODE_NAME_TO_IDS_CACHE = {"data": None, "loaded": False}
NODE_NAME_TO_IDS_FILE = CACHE_DIR / "nodelink_name_to_ids.json"


def build_nodelink_name_to_ids():
    """MOCT_NODE.shp에서 IC/JC로 끝나는 이름을 접미사 제거 후 NODE_ID 목록으로
    매핑해(같은 이름이 진입로별로 여러 NODE_ID를 가질 수 있음) 저장한다.
    build_nodelink_name_coords()(이름→centroid 좌표, 근사용)와 달리 여기서는
    실제 NODE_ID를 그대로 보존해 그래프 라우팅에 쓴다 — 여러 후보 중 어느 것을
    쓸지는 호출측이 기준좌표(이미 매칭된 근사 좌표)에 가장 가까운 것으로 고른다."""
    import geopandas as gpd

    if not NODELINK_NODE_SHP.exists():
        raise FileNotFoundError(f"node shapefile not found: {NODELINK_NODE_SHP}")

    t0 = time.time()
    gdf = gpd.read_file(str(NODELINK_NODE_SHP), encoding="euc-kr")
    gdf = gdf[gdf["NODE_NAME"].str.contains(_NODE_SUFFIX_RE, na=False, regex=True)]
    gdf = gdf.to_crs("EPSG:4326")
    print(f"[build_nodelink_name_to_ids] IC/JC node {len(gdf)}, load {time.time() - t0:.1f}s")

    by_name = {}
    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None:
            continue
        # 2026-09-02 수정: 예전엔 IC/JC 접미사까지 떼서("천안JC"·"천안IC" 둘 다 "천안")
        # 키를 만들었는데, 그러면 같은 나들목 이름을 공유하는 IC와 JC가 서로 다른
        # 실제 지점(수백m~수km 떨어진 별개 NODE_ID)인데도 하나로 뭉개져 콘존
        # "천안JC-천안IC"의 양끝이 같은 이름으로 풀려 id1==id2로 거부되는 문제가
        # 있었다(실측 — 172개 잔여 직선 중 92개, 절반 이상이 이 원인). 접미사를
        # 보존한 원래 이름("천안JC")을 그대로 키로 써서 구분을 유지한다.
        raw_name = (row.get("NODE_NAME") or "").strip()
        if not raw_name:
            continue
        entry = {
            "node_id": str(row.get("NODE_ID")),
            "lat": round(geom.y, 6), "lng": round(geom.x, 6),
        }
        by_name.setdefault(raw_name, []).append(entry)
        # 접미사 뗀 이름으로도 같이 등록해둔다(하위호환 폴백용) — raw 매칭이 실패하는
        # 케이스(표기 차이 등)에서 _conzone_path_or_fallback의 stripped 폴백 시도가
        # 동작하려면 stripped 키에도 후보가 있어야 한다. 같은 후보가 두 키 모두에
        # 들어갈 수 있음(정상 — 각자 다른 검색 경로에서 조회).
        stripped_name = _strip_ic_suffix(raw_name)
        if stripped_name and stripped_name != raw_name:
            by_name.setdefault(stripped_name, []).append(entry)

    payload = {"fetched_at": time.time(), "data": by_name}
    NODE_NAME_TO_IDS_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    _NODE_NAME_TO_IDS_CACHE["data"] = by_name
    _NODE_NAME_TO_IDS_CACHE["loaded"] = True
    print(f"[build_nodelink_name_to_ids] done, unique names {len(by_name)} -> {NODE_NAME_TO_IDS_FILE}")
    return {"total_ic_jc_nodes": len(gdf), "unique_names": len(by_name)}


def _load_nodelink_name_to_ids():
    if _NODE_NAME_TO_IDS_CACHE["loaded"]:
        return _NODE_NAME_TO_IDS_CACHE["data"] or {}
    data = None
    if NODE_NAME_TO_IDS_FILE.exists():
        try:
            payload = json.loads(NODE_NAME_TO_IDS_FILE.read_text(encoding="utf-8"))
            data = payload.get("data")
        except Exception:
            data = None
    if data is None:
        try:
            build_nodelink_name_to_ids()
            data = _NODE_NAME_TO_IDS_CACHE["data"]
        except Exception as e:
            print(f"[_load_nodelink_name_to_ids] 빌드 실패: {e}")
            data = {}
    _NODE_NAME_TO_IDS_CACHE["data"] = data
    _NODE_NAME_TO_IDS_CACHE["loaded"] = True
    return data or {}


def _resolve_node_id(name, ref_lat=None, ref_lng=None):
    """접미사 뗀 IC/JC 이름 → 그래프에 실재하는 NODE_ID 1개. 후보가 여럿이면
    (ref_lat,ref_lng)가 있을 때 그에 가장 가까운 후보, 없으면 그래프에 존재하는
    첫 후보를 쓴다. 그래프에 없는(고속도로 링크와 안 이어진) 후보는 제외한다."""
    if not name:
        return None
    graph = _load_nodelink_graph()
    if not graph:
        return None
    node_ids_in_graph = graph["nodes"]  # dict node_id -> [lat,lng]
    name_to_ids = _load_nodelink_name_to_ids()
    candidates = [c for c in (name_to_ids.get(name) or []) if c["node_id"] in node_ids_in_graph]
    if not candidates:
        return None
    if len(candidates) == 1 or ref_lat is None or ref_lng is None:
        return candidates[0]["node_id"]
    best_id, best_dist = None, None
    for c in candidates:
        d = _haversine(ref_lat, ref_lng, c["lat"], c["lng"])
        if d is not None and (best_dist is None or d < best_dist):
            best_id, best_dist = c["node_id"], d
    return best_id or candidates[0]["node_id"]


def _load_conzone_paths_cache():
    if _CONZONE_PATHS_CACHE["loaded"]:
        return _CONZONE_PATHS_CACHE["data"]
    data = {}
    if CONZONE_PATHS_FILE.exists():
        try:
            data = json.loads(CONZONE_PATHS_FILE.read_text(encoding="utf-8")) or {}
        except Exception:
            data = {}
    _CONZONE_PATHS_CACHE["data"] = data
    _CONZONE_PATHS_CACHE["loaded"] = True
    return data


def _save_conzone_paths_cache():
    data = _CONZONE_PATHS_CACHE["data"] or {}
    CONZONE_PATHS_FILE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def get_conzone_graph_path(name1, name2, ref1=None, ref2=None):
    """콘존 양끝 이름(IC/JC 접미사 제거된 상태)으로 표준노드링크 그래프 위 최단
    경로 좌표를 구한다. ref1/ref2는 (lat,lng) — 동명이인 NODE_ID 후보가 여럿일 때
    가장 가까운 것을 고르는 기준(보통 기존 방식으로 이미 얻은 근사 좌표를 넘긴다).

    24시간 TTL로 tools/restarea_cache/conzone_paths.json에 캐시한다. 이름 매칭
    실패, 그래프에 없는 노드, 경로 없음, 직선거리 대비 3배 넘는 과도한 우회,
    또는 시작-끝 직선에서 옆으로 크게 벗어나는 경로(이름 오매칭으로 반대쪽/엉뚱한
    진입로 노드를 골라 목적지를 지나쳤다 되돌아오는 패턴, 2026-09-02 실측 확인)인
    경우 None을 반환한다 — 호출측이 기존 스냅 방식/직선으로 폴백한다. 좌표를
    지어내지 않는다는 원칙을 지킨다.

    캐시 키에 스키마 버전(v2)을 붙여, 이 수직 이격거리 검사가 없던 구버전 로직이
    만든 나쁜 경로가 남은 TTL 동안 그대로 재서빙되는 것을 막는다(2026-09-02 —
    사고 조사 중 캐시 파일에 구버전 경로 890건이 남아 있었음을 확인, v1 키는
    이제 조회되지 않아 자연스럽게 죽은 데이터가 되고 다음 재빌드 때 정리됨)."""
    if not name1 or not name2:
        return None
    cache_key = f"v2:{name1}::{name2}"
    cache = _load_conzone_paths_cache()
    now = time.time()
    cached = cache.get(cache_key)
    if cached and (now - cached.get("at", 0)) < _CONZONE_PATH_TTL:
        return cached.get("path")  # None도 유효한 캐시값(재계산 낭비 방지)

    result_path = None
    try:
        ref1_lat, ref1_lng = (ref1 or (None, None))
        ref2_lat, ref2_lng = (ref2 or (None, None))
        id1 = _resolve_node_id(name1, ref1_lat, ref1_lng)
        id2 = _resolve_node_id(name2, ref2_lat, ref2_lng)
        if id1 and id2 and id1 != id2:
            r = _dijkstra_path(id1, id2)
            if r:
                total_km, coords = r
                straight_km = None
                if ref1_lat is not None and ref2_lat is not None:
                    straight_km = _haversine(ref1_lat, ref1_lng, ref2_lat, ref2_lng)
                ok_ratio = (straight_km is None or straight_km <= 0
                            or (total_km / straight_km) <= _GRAPH_PATH_DETOUR_RATIO_MAX)
                ok_dev = True
                if ok_ratio and straight_km and straight_km > 0 and ref1_lat is not None and ref2_lat is not None:
                    max_dev = _max_perp_deviation_km(coords, ref1_lat, ref1_lng, ref2_lat, ref2_lng)
                    dev_limit = max(_GRAPH_PATH_MAX_DEV_MIN_KM, straight_km * _GRAPH_PATH_MAX_DEV_RATIO)
                    ok_dev = max_dev <= dev_limit
                if ok_ratio and ok_dev:
                    result_path = coords
    except Exception:
        result_path = None

    cache[cache_key] = {"at": now, "path": result_path}
    _CONZONE_PATHS_CACHE["data"] = cache
    global _CONZONE_PATHS_DIRTY
    _CONZONE_PATHS_DIRTY = True  # 매 조회마다 9MB+ 파일을 즉시 다시 쓰면 콘존 1,000여
    # 개를 순회하는 _build_traffic_summary() 한 번에 디스크 쓰기가 수백~수천 번
    # 일어나 응답이 다시 10초 넘게 걸렸다(2026-09-02 실측). 요청 끝에 한 번만
    # flush_conzone_paths_cache_if_dirty()로 저장하도록 바꿨다.
    return result_path


def flush_conzone_paths_cache_if_dirty():
    global _CONZONE_PATHS_DIRTY
    if _CONZONE_PATHS_DIRTY:
        _save_conzone_paths_cache()
        _CONZONE_PATHS_DIRTY = False


def _load_official_road_geometry_cache():
    """official_road_geometry.json을 읽어 캐싱한다(수동 재빌드 전까지 TTL 없이 유지 —
    OSRM 캐시와 달리 정적 공식 데이터라 서버 재시작 전까지는 파일 mtime을 다시 볼
    필요가 없다). 파일이 없으면 None."""
    if _OFFICIAL_ROAD_GEOMETRY_CACHE["loaded"]:
        return _OFFICIAL_ROAD_GEOMETRY_CACHE["data"]
    data = None
    if OFFICIAL_ROAD_GEOMETRY_FILE.exists():
        try:
            payload = json.loads(OFFICIAL_ROAD_GEOMETRY_FILE.read_text(encoding="utf-8"))
            data = payload.get("data")
        except Exception:
            data = None
    _OFFICIAL_ROAD_GEOMETRY_CACHE["data"] = data
    _OFFICIAL_ROAD_GEOMETRY_CACHE["loaded"] = True
    return data


_OFFICIAL_LINESTRING_CACHE = {}  # id(route_entry) -> [shapely.LineString, ...]


def _official_linestrings(route_entry):
    """route_entry["segments"](좌표 리스트)를 shapely LineString으로 매번 새로
    만들면(호출당 O(점수)) get_traffic_summary()가 콘존 1,600개를 순회할 때마다
    같은 노선의 LineString을 반복 재생성해 응답이 10초 넘게 걸렸다(2026-09-02
    실측). route_entry(dict, 프로세스 생존 동안 동일 객체) id로 메모이즈해
    노선당 1회만 LineString을 만들고 이후엔 재사용한다."""
    from shapely.geometry import LineString

    cache_key = id(route_entry)
    cached = _OFFICIAL_LINESTRING_CACHE.get(cache_key)
    if cached is not None:
        return cached
    lines = []
    for seg in route_entry.get("segments") or []:
        if len(seg) < 2:
            continue
        lines.append(LineString([(lng, lat) for lat, lng in seg]))
    _OFFICIAL_LINESTRING_CACHE[cache_key] = lines
    return lines


def _official_subpath(route_entry, u1, u2):
    """공식 지오메트리 구간(route_entry["segments"])에서 두 톨게이트 좌표(u1,u2 —
    _units_by_code() 값, lat/lng 포함)를 도로선 위 최근접점에 스냅(shapely project)한
    뒤 그 사이 구간만 잘라 반환한다. 두 점 모두 스냅 오차가 허용범위 안인 segment가
    없으면 None(호출측이 기존 방식으로 폴백)."""
    if u1.get("lat") is None or u1.get("lng") is None or u2.get("lat") is None or u2.get("lng") is None:
        return None
    from shapely.geometry import Point
    from shapely.ops import substring

    p1 = Point(u1["lng"], u1["lat"])
    p2 = Point(u2["lng"], u2["lat"])

    best = None  # (combined_dist, line, d1, d2)
    for line in _official_linestrings(route_entry):
        d1 = line.distance(p1)
        d2 = line.distance(p2)
        combined = d1 + d2
        if best is None or combined < best[0]:
            best = (combined, line, d1, d2)

    if best is None:
        return None
    _, line, d1, d2 = best
    if d1 > _OFFICIAL_SNAP_MAX_DEG or d2 > _OFFICIAL_SNAP_MAX_DEG:
        return None

    proj1 = line.project(p1)
    proj2 = line.project(p2)
    if proj1 == proj2:
        return None
    lo, hi = min(proj1, proj2), max(proj1, proj2)
    sub_line = substring(line, lo, hi)
    coords = list(sub_line.coords)
    if len(coords) < 2:
        return None
    latlng = [[lat, lng] for lng, lat in coords]
    if proj1 > proj2:
        latlng = list(reversed(latlng))
    return latlng


def _rekeyed_by_route_no(d):
    """캐시 JSON에 저장된 dict 키는 저장 당시의 _route_key() 결과라, 함수 규칙이
    바뀌면(2026-09-02 충돌버그 수정) 예전 키와 새 키가 달라질 수 있다. 디스크
    재저장 없이 항상 최신 _route_key() 규칙으로 다시 키를 매겨서 조회한다."""
    if not d:
        return {}
    out = {}
    for v in d.values():
        if isinstance(v, dict) and v.get("routeNo"):
            out[_route_key(v["routeNo"])] = v
    return out


def get_route_lines():
    """노선 지오메트리 조회. 우선순위: ① 국토교통부 전국표준노드링크 기반
    (nodelink_road_geometry.json, build_nodelink_road_geometry() — ROAD_NO 직접
    매칭이라 신뢰도 최고) ② 도로공사 공식 도로선형 shapefile 기반
    (official_road_geometry.json, build_official_road_geometry()로 생성) ③ 기존 OSRM
    근사 캐시(road_geometry.json) ④ 톨게이트 좌표 직선 연결(build_route_lines()).
    상위 지오메트리가 있는 노선은 그것으로 완전히 덮어쓰고, 매칭 안 된 노선은 기존
    하위 그대로 서빙한다(2026-09-02 표준노드링크 도입 — 경부선/동해선/함양울산선/
    수도권제2순환선 등 좌표 점프·미매칭 노선 교체)."""
    combined = {}
    road_geometry = _load_road_geometry_cache()
    if road_geometry is not None:
        combined.update(_rekeyed_by_route_no(road_geometry))
    else:
        now = time.time()
        if _ROUTE_LINES_CACHE["data"] is None or (now - _ROUTE_LINES_CACHE["at"]) > _ROUTE_LINES_TTL:
            try:
                _ROUTE_LINES_CACHE["data"] = build_route_lines()
            except Exception:
                _ROUTE_LINES_CACHE["data"] = {}
            _ROUTE_LINES_CACHE["at"] = now
        combined.update(_rekeyed_by_route_no(_ROUTE_LINES_CACHE["data"]))

    official = _load_official_road_geometry_cache()
    if official:
        combined.update(_rekeyed_by_route_no(official))  # 공식 shapefile 지오메트리 2순위

    nodelink = _load_nodelink_road_geometry_cache()
    if nodelink:
        combined.update(_rekeyed_by_route_no(nodelink))  # 표준노드링크 최우선
    return combined


_ROUTE_TRAFFIC_CACHE = {}  # route_no -> {"at": ts, "data": [...]}
_ROUTE_TRAFFIC_TTL = 15 * 60  # conzone_traffic 자체가 15분 TTL이라 맞춤

_SUFFIX_RE = re.compile(r"(하이패스|Hi|IC|JC|TG|SA)$")


def _strip_ic_suffix(name):
    """콘존명의 두 지점("대전IC-회덕JC")을 unitName("대전", "회덕")과 매칭하려고
    IC/JC/TG/하이패스/Hi/SA 접미사를 뗀다. 접미사가 없는 이름(고속도로 시종점 등)은
    그대로 둔다."""
    return _SUFFIX_RE.sub("", (name or "").strip())


def _units_by_route_and_name():
    """route_key → {접미사 뗀 unitName: unit dict}. 콘존-톨게이트 이름 매칭용.
    실측(2026-09-02): 경부선 기준 콘존 135개 중 75개(56%)가 양끝 다 매칭, 전국
    평균은 약 32%(1,588개 중 515개) — 나머지는 IC가 아닌 JC/분기점 등 우리 톨게이트
    좌표 목록(locationinfoUnit)에 애초에 없는 지점이라 좌표 매칭이 불가능하다.
    좌표를 지어내지 않고 텍스트 목록으로만 남긴다."""
    units = _cached_data("units")
    by_route = {}
    for u in units:
        rk = _route_key(u.get("routeNo"))
        name = _strip_ic_suffix(u.get("unitName"))
        by_route.setdefault(rk, {})[name] = u
    return by_route


def _unit_has_coords(u):
    """u(톨게이트/노드 dict)가 실제 사용 가능한 좌표(xValue/yValue)를 갖고 있는지.
    2026-09-02 사고 조사 중 발견 — locationinfoUnit 원본에 이름은 있지만
    xValue/yValue가 None인 항목이 있고(예: 진해/대청, 남해제3지선), 예전 코드는
    "이름이 매칭됐다(dict가 존재한다)"만 보고 좌표 없는 채로 그래프/스냅 로직에
    넘겼다 — ref가 None이 되어 새로 추가한 이격거리 sanity check까지 무력화되는
    구멍이었다. 좌표가 없으면 이름 매칭 자체를 "실패"로 취급해 노드링크 폴백
    (있으면 실좌표)이나 매칭 없음으로 처리한다 — 지어내지 않는다는 원칙 유지."""
    if not u:
        return False
    return _to_float(u.get("yValue")) is not None and _to_float(u.get("xValue")) is not None


def _conzone_direction(conzone_id):
    """conzoneId 끝의 CZE/CZS로 정방향/역방향을 구분한다(실측 확인, 2026-09-02):
    같은 두 지점이 이름 순서를 뒤집어(A-B ↔ B-A) CZE/CZS 두 콘존으로 따로 존재한다."""
    cid = conzone_id or ""
    if "CZE" in cid:
        return "정방향"
    if "CZS" in cid:
        return "역방향"
    return None


def get_route_traffic(route_no, force=False):
    """선택한 노선의 콘존(conzone) 교통량 전체. 2026-09-02 전면 교체 —
    이전에는 odtraffic/upDownTrafficAmount로 인접 톨게이트쌍만 최대 15쌍 호출했지만
    (대부분 count=0인 제한된 표본), sectionTrafficRouteDirection이 전국 15분 단위
    콘존을 커버해 완전히 상위호환이라 판단, 옛 로직(_fetch_od_traffic 등)은
    제거했다. 좌표 매칭된(matched=true) 콘존만 지도에 그리고, 매칭 안 된 콘존도
    이름+수치는 그대로 반환해 프론트가 텍스트 목록으로 보여준다."""
    key = _route_key(route_no)
    now = time.time()
    cached = _ROUTE_TRAFFIC_CACHE.get(key)
    if not force and cached and (now - cached["at"]) < _ROUTE_TRAFFIC_TTL:
        return cached["data"]

    all_conzone = _cached_data("conzone_traffic")
    lcs_rows = _cached_data("lcs_status")
    lcs_by_id = {r.get("conzoneId"): r for r in lcs_rows if r.get("conzoneId")}
    units_by_route = _units_by_route_and_name()
    route_units = units_by_route.get(key) or {}

    rows = [r for r in all_conzone if _route_key((r.get("conzoneId") or "")[:4]) == key]

    result = []
    for r in rows:
        cid = r.get("conzoneId") or ""
        name = r.get("conzoneName") or ""
        parts = name.split("-", 1)
        from_name, to_name = (parts[0], parts[1]) if len(parts) == 2 else (name, "")
        u1 = route_units.get(_strip_ic_suffix(from_name))
        u2 = route_units.get(_strip_ic_suffix(to_name))
        matched = _unit_has_coords(u1) and _unit_has_coords(u2)
        lcs_row = lcs_by_id.get(cid)
        result.append({
            "conzoneId": cid,
            "conzoneName": name,
            "direction": _conzone_direction(cid),
            "trafficCount": _to_int(r.get("trafficCount")),
            "lcs_applyYn": bool(lcs_row and lcs_row.get("applyYn") == "Y"),
            "matched": matched,
            "from_name": from_name.strip(), "to_name": to_name.strip(),
            "from": [_to_float(u1.get("yValue")), _to_float(u1.get("xValue"))] if u1 else None,
            "to": [_to_float(u2.get("yValue")), _to_float(u2.get("xValue"))] if u2 else None,
            # 실제 도로 형상(있을 때만) — 없으면 None, 프론트가 from/to 직선으로 폴백.
            # 우선순위: ①표준노드링크 그래프 Dijkstra(가장 정밀) ②기존 shapely 스냅
            # ③None(프론트 직선 폴백).
            "path": _conzone_path_or_fallback(route_no, u1, u2, from_name.strip(), to_name.strip()) if matched else None,
        })

    # 2026-09-02 재정리 — 교통량 상대 3분위 폴백을 완전히 제거했다("항상 1/3이
    # 빨간" 왜곡의 근본 원인). 지도 소통색은 이제 ITS 링크 단위 실시간 속도
    # (/api/restarea/link-traffic)가 담당하고, 여기 level은 실제 속도가 매칭될
    # 때만(realUnitTrtm, 커버리지 낮음) 채워진다 — 없으면 None, 지어내지 않는다.
    # trafficCount(콘존 교통량)·구간명은 팝업/텍스트 정보용으로만 남는다.
    for e, r in zip(result, rows):
        cid = e["conzoneId"]
        name = e["conzoneName"]
        parts = name.split("-", 1)
        from_name, to_name = (parts[0], parts[1]) if len(parts) == 2 else (name, "")
        u1 = route_units.get(_strip_ic_suffix(from_name))
        u2 = route_units.get(_strip_ic_suffix(to_name))
        speed = _pair_speed_kmh(u1, u2, route_no) if (u1 and u2) else None
        e["speed_kmh"] = speed
        e["level"] = _speed_level_5(speed)

    flush_conzone_paths_cache_if_dirty()
    _ROUTE_TRAFFIC_CACHE[key] = {"at": now, "data": result}
    return result


_TRAFFIC_SUMMARY_CACHE = {"at": 0.0, "data": None}
_TRAFFIC_SUMMARY_TTL = 120  # 2분 — 콘존 데이터 자체는 15분 단위라 이보다 자주
# 안 바뀌는데도, 1,600개 구간마다 도로선 스냅 계산을 매 요청 반복해 응답이
# 10초 넘게 걸렸다(2026-09-02 실측). 짧게라도 캐시해 프론트 5분 폴링 사이
# 반복 호출·수동 새로고침 시 재계산을 막는다.


def get_traffic_summary():
    now = time.time()
    cached = _TRAFFIC_SUMMARY_CACHE["data"]
    if cached is not None and (now - _TRAFFIC_SUMMARY_CACHE["at"]) < _TRAFFIC_SUMMARY_TTL:
        return cached
    result = _build_traffic_summary()
    _TRAFFIC_SUMMARY_CACHE["data"] = result
    _TRAFFIC_SUMMARY_CACHE["at"] = now
    return result


def _build_traffic_summary():
    """2026-09-02 전면 교체(2차) — trafficCount(교통량) 상대 3분위 대신 realUnitTrtm
    (톨게이트쌍 평균 통행시간) 기반 실제 평균속도(km/h) 절대기준(_speed_level_5,
    ITS 공식 사이트 방식: 90↑ 매우원활/70~90 원활/50~70 서행/30~50 정체임박/30↓
    정체)으로 등급을 매긴다. 속도 매칭은 콘존 양끝이 둘 다 실제 톨게이트(unitCode
    보유)일 때만 가능하다 — 노드링크 폴백 좌표(JC 등, unitCode=None)나 realUnitTrtm
    미수집 구간은 speed_kmh/level 모두 None(회색/미표시), 지어내지 않는다.
    trafficCount(콘존 교통량) 자체는 참고용 필드로 계속 남긴다."""
    data = _cached_data("conzone_traffic")
    counts_all = [c for c in (_to_int(r.get("trafficCount")) for r in data) if c is not None]
    avg = round(sum(counts_all) / len(counts_all), 1) if counts_all else None

    # 2026-09-02 재정리 — 교통량 상대 3분위 폴백(_count_level)을 완전히 제거했다.
    # "전국 어디서나 항상 1/3이 빨갛게" 보이던 왜곡의 근본 원인이었다. 지도
    # 소통색은 이제 ITS 링크 단위 실시간 속도(/api/restarea/link-traffic)가
    # 전담하고, 이 segments의 level은 실제 속도(realUnitTrtm)가 매칭될 때만
    # 채워진다(대부분 None) — 리스트 카드 소통 배지 등 보조 용도로만 쓰인다.
    units_by_route = _units_by_route_and_name()
    lcs_rows = _cached_data("lcs_status")
    lcs_yn_ids = {r.get("conzoneId") for r in lcs_rows if r.get("applyYn") == "Y"}

    # 노선(route_key)별 대표 routeNo/routeName — 톨게이트 이름 매칭이 실패해도
    # 노드 폴백으로 좌표를 찾은 경우 이 값으로 routeNo/routeName을 채운다.
    route_meta_by_key = {}
    for key, names in units_by_route.items():
        if names:
            first = next(iter(names.values()))
            route_meta_by_key[key] = (first.get("routeNo"), first.get("routeName"))

    node_coords = None  # 최초 필요 시 1회만 로드(지연 로딩)

    def _node_fallback_unit(stripped_name, route_no, route_name):
        nonlocal node_coords
        if node_coords is None:
            node_coords = _load_nodelink_name_coords()
        latlng = node_coords.get(stripped_name)
        if not latlng:
            return None
        return {
            "unitName": stripped_name, "unitCode": None,
            "routeNo": route_no, "routeName": route_name,
            "yValue": latlng[0], "xValue": latlng[1],
        }

    # 좌표 매칭된(양 끝 IC/JC 이름이 우리 톨게이트 좌표 목록과 맞는, 또는 표준
    # 노드링크 MOCT_NODE.shp의 IC/JC 이름으로 폴백 매칭된) 구간만 지도 폴리라인으로
    # 만든다 — 나머지는 route-traffic(노선 선택 시)의 텍스트 목록에서만 보인다.
    # 좌표를 지어내지 않는다는 원칙을 여기서도 지킨다(노드 폴백도 실제 국가
    # 표준노드링크 좌표이지 임의 추정이 아니다).
    segments = []
    for row in data:
        cid = row.get("conzoneId") or ""
        key = _route_key(cid[:4])
        name = row.get("conzoneName") or ""
        parts = name.split("-", 1)
        if len(parts) != 2:
            continue
        route_units = units_by_route.get(key) or {}
        name1, name2 = _strip_ic_suffix(parts[0]), _strip_ic_suffix(parts[1])
        u1 = route_units.get(name1)
        u2 = route_units.get(name2)
        if not _unit_has_coords(u1):
            u1 = None
        if not _unit_has_coords(u2):
            u2 = None
        if not (u1 and u2):
            route_no_meta, route_name_meta = route_meta_by_key.get(key, (None, None))
            if u1 is None:
                u1 = _node_fallback_unit(name1, route_no_meta, route_name_meta)
            if u2 is None:
                u2 = _node_fallback_unit(name2, route_no_meta, route_name_meta)
        if not (u1 and u2):
            continue
        c = _to_int(row.get("trafficCount"))
        route_no = u1.get("routeNo") or u2.get("routeNo")
        speed = _pair_speed_kmh(u1, u2, route_no)
        level = _speed_level_5(speed)  # 실측 속도 있을 때만 — 3분위 폴백 제거(2026-09-02)
        segments.append({
            "routeNo": route_no,
            "routeName": u1.get("routeName") or u2.get("routeName"),
            "conzoneId": cid,
            "conzoneName": name,
            "direction": _conzone_direction(cid),
            "trafficCount": c,
            "speed_kmh": speed,
            "level": level,
            "lcs_applyYn": cid in lcs_yn_ids,
            "from": {"name": u1.get("unitName"), "lat": _to_float(u1.get("yValue")), "lng": _to_float(u1.get("xValue"))},
            "to": {"name": u2.get("unitName"), "lat": _to_float(u2.get("yValue")), "lng": _to_float(u2.get("xValue"))},
            # path(콘존 도로 형상)는 제거(2026-09-02) — 지도 소통색이 링크 단위
            # (/api/restarea/link-traffic)로 바뀌어 콘존 폴리라인을 더 이상 그리지
            # 않는다. from/to 좌표는 리스트 카드 소통 배지의 근접 매칭에만 쓰인다.
        })

    matched_speed = sum(1 for s in segments if s["speed_kmh"] is not None)
    flush_conzone_paths_cache_if_dirty()
    return {
        "count": len(data),
        "avg_traffic_count": avg,
        "matched_speed_count": matched_speed,
        "rows": data[:50],
        "segments": segments,
        "updated_at": time.time(),
    }


def get_active_incidents():
    """활성(해제되지 않은) 사고/공사/기상특보 등 돌발정보 목록. 좌표 있는 것만.

    "진행"이 아닌 값도 실측상 전부 "진행"이었지만(2026-09-01), 혹시 "해제" 값이
    섞여 오면 안전하게 걸러낸다 — accProcessNM에 "해제"가 포함된 항목은 제외."""
    data = _cached_data("burstinfo")
    return [d for d in data if "해제" not in (d.get("accProcessNM") or "")]


# ── ITS(국가교통정보센터) 오픈API CCTV ─────────────────────────────────────
# 도로공사 API와 별개로 .env의 ITS_OPENAPI_KEY로 인증한다(2026-09-02 실측 확인
# — https://openapi.its.go.kr:9443/cctvInfo, type=ex로 고속도로만 필터, bbox는
# 전국 범위(minX/maxX/minY/maxY)를 한 번에 넣어도 1회 호출로 전량이 온다).
ITS_CCTV_FILE = CACHE_DIR / "its_cctv.json"
ITS_CCTV_TTL = 24 * 3600
_ITS_CCTV_CACHE = {"at": 0.0, "data": None}
_KR_BBOX = {"minX": "124.5", "maxX": "131.9", "minY": "33.0", "maxY": "39.0"}

_CCTV_ROUTE_BRACKET_RE = re.compile(r"^\[([^\]]+)\]")


def _routename_to_routeno_map():
    """정규화 노선명 → routeNo 후보 리스트. CCTV 이름 "[노선명] 지점명"의
    노선명을 우리 routeNo로 매칭하는 데 쓴다(build_official_road_geometry와
    동일한 _normalize_route_name 정규화 규칙 재사용)."""
    units = _cached_data("units")
    unit_routes = {}
    for u in units:
        rn = (u.get("routeNo") or "").strip()
        if rn and rn not in unit_routes:
            unit_routes[rn] = u.get("routeName")
    norm_to_routeno = {}
    for rn, name in unit_routes.items():
        norm_to_routeno.setdefault(_normalize_route_name(name), []).append(rn)
    return norm_to_routeno


def _attach_cctv_direction(items):
    """각 CCTV 항목에 "direction"(상행/하행/None) 필드를 채운다.

    방식: CCTV 이름 "[노선명] 지점명"에서 노선명을 파싱→정규화→routeNo 매칭
    (_normalize_route_name, build_official_road_geometry와 동일 규칙) → 그
    노선의 휴게소들(get_all_restareas, 이미 _build_direction_map으로 상행/하행이
    부여돼 있음) 중 CCTV 좌표에서 가장 가까운 휴게소의 방향을 물려받는다.
    (사용자 지시는 "가장 가까운 톨게이트"였지만, 실제로 상행/하행 라벨이 붙어
    있는 지점은 톨게이트가 아니라 휴게소뿐이라 — locationinfoUnit 톨게이트에는
    방향 필드가 없다 — 방향 라벨이 실재하는 휴게소를 대신 기준으로 삼는다.
    노선 매칭이 안 되거나 그 노선에 방향이 부여된 휴게소가 없으면 direction은
    None으로 두고 지어내지 않는다.)"""
    norm_to_routeno = _routename_to_routeno_map()
    all_restareas = get_all_restareas()
    restareas_by_key = {}
    for a in all_restareas:
        restareas_by_key.setdefault(_route_key(a.get("routeNo")), []).append(a)
    matched_route = 0
    matched_direction = 0

    for it in items:
        it["direction"] = None
        m = _CCTV_ROUTE_BRACKET_RE.match(it.get("name") or "")
        if not m:
            continue
        norm = _normalize_route_name(m.group(1))
        cands = norm_to_routeno.get(norm)
        if not cands:
            continue
        route_no = cands[0]
        matched_route += 1
        areas = restareas_by_key.get(_route_key(route_no)) or []

        best_dir, best_dist = None, None
        for a in areas:
            if a.get("direction") is None or a.get("lat") is None or a.get("lng") is None:
                continue
            d = _haversine(it.get("lat"), it.get("lng"), a.get("lat"), a.get("lng"))
            if d is None:
                continue
            if best_dist is None or d < best_dist:
                best_dist, best_dir = d, a.get("direction")
        if best_dir:
            it["direction"] = best_dir
            matched_direction += 1

    print(f"[_attach_cctv_direction] cctv={len(items)} route_matched={matched_route} "
          f"direction_matched={matched_direction}")
    return items


def _fetch_its_cctv(force=False):
    """전국 고속도로 CCTV 목록(위경도·이름·스트리밍URL)을 ITS 오픈API에서 가져와
    24시간 TTL로 캐싱한다. 인증키 없거나 호출 실패 시 빈 리스트 반환(지어내지 않음)."""
    now = time.time()
    if not force and _ITS_CCTV_CACHE["data"] is not None and (now - _ITS_CCTV_CACHE["at"]) < ITS_CCTV_TTL:
        return _ITS_CCTV_CACHE["data"]

    if not force and ITS_CCTV_FILE.exists():
        try:
            payload = json.loads(ITS_CCTV_FILE.read_text(encoding="utf-8"))
            if (now - payload.get("fetched_at", 0)) < ITS_CCTV_TTL:
                cached_items = payload.get("data") or []
                if cached_items and "direction" not in cached_items[0]:
                    cached_items = _attach_cctv_direction(cached_items)
                    ITS_CCTV_FILE.write_text(
                        json.dumps({"fetched_at": payload.get("fetched_at", now), "data": cached_items}, ensure_ascii=False),
                        encoding="utf-8")
                _ITS_CCTV_CACHE["data"] = cached_items
                _ITS_CCTV_CACHE["at"] = payload.get("fetched_at", now)
                return _ITS_CCTV_CACHE["data"]
        except Exception:
            pass

    key = os.environ.get("ITS_OPENAPI_KEY", "").strip()
    if not key:
        _ITS_CCTV_CACHE["data"] = []
        _ITS_CCTV_CACHE["at"] = now
        return []

    items = []
    try:
        params = dict(_KR_BBOX)
        params.update({"apiKey": key, "type": "ex", "cctvType": "1", "getType": "json"})
        resp = requests.get("https://openapi.its.go.kr:9443/cctvInfo", params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        rows = ((data.get("response") or {}).get("data")) or []
        for r in rows:
            lat, lng = _to_float(r.get("coordy")), _to_float(r.get("coordx"))
            if lat is None or lng is None:
                continue
            items.append({
                "name": r.get("cctvname") or "",
                "lat": lat, "lng": lng,
                "url": r.get("cctvurl") or "",
                "format": r.get("cctvformat") or "",
            })
        items = _attach_cctv_direction(items)
    except Exception as e:
        print(f"[_fetch_its_cctv] 호출 실패: {e}")
        # 실패 시 만료된 캐시라도 있으면 그대로 서빙(전혀 안 보이는 것보단 낫다)
        if ITS_CCTV_FILE.exists():
            try:
                payload = json.loads(ITS_CCTV_FILE.read_text(encoding="utf-8"))
                items = payload.get("data") or []
            except Exception:
                items = []

    ITS_CCTV_FILE.write_text(json.dumps({"fetched_at": now, "data": items}, ensure_ascii=False), encoding="utf-8")
    _ITS_CCTV_CACHE["data"] = items
    _ITS_CCTV_CACHE["at"] = now
    return items


# ── CCTV 신선 URL 발급 (2026-09-03) ─────────────────────────────────────────
# ITS CCTV 스트림 URL은 서명 포함 임시 URL(약 120분 유효)이라, 24시간 캐시/
# 일1회 발행분은 방문자가 클릭할 시점엔 거의 항상 만료돼 있다. 클릭 시점에
# 요청 좌표 주변 아주 작은 bbox(±0.01°)로 ITS API를 실시간 호출해 최근접
# CCTV의 신선한 URL을 발급한다. 초단기(2분) 캐시로 연타만 흡수.
_CCTV_FRESH_CACHE = {}          # (lat3, lng3) -> {"at": ts, "data": {...}}
_CCTV_FRESH_TTL = 120           # 초


def get_cctv_fresh(lat, lng):
    """좌표 최근접 고속도로 CCTV의 신선한 스트림 URL {name,url,format} 반환.
    없으면 None. 실패 시 예외."""
    key = os.environ.get("ITS_OPENAPI_KEY", "").strip()
    if not key:
        raise RuntimeError("ITS_OPENAPI_KEY not set")
    ck = (round(lat, 3), round(lng, 3))
    now = time.time()
    hit = _CCTV_FRESH_CACHE.get(ck)
    if hit and (now - hit["at"]) < _CCTV_FRESH_TTL:
        return hit["data"]
    d = 0.01
    params = {
        "apiKey": key, "type": "ex", "cctvType": "1", "getType": "json",
        "minX": f"{lng - d:.5f}", "maxX": f"{lng + d:.5f}",
        "minY": f"{lat - d:.5f}", "maxY": f"{lat + d:.5f}",
    }
    resp = requests.get("https://openapi.its.go.kr:9443/cctvInfo", params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    rows = ((resp.json().get("response") or {}).get("data")) or []
    if isinstance(rows, dict):  # 결과 1건이면 dict로 오는 API 특성 방어
        rows = [rows]
    best, best_d2 = None, None
    for r in rows:
        rlat, rlng = _to_float(r.get("coordy")), _to_float(r.get("coordx"))
        if rlat is None or rlng is None or not r.get("cctvurl"):
            continue
        d2 = (rlat - lat) ** 2 + (rlng - lng) ** 2
        if best_d2 is None or d2 < best_d2:
            best, best_d2 = r, d2
    data = None
    if best:
        url = best.get("cctvurl") or ""
        # https 페이지에서 http 스트림은 혼합콘텐츠로 차단됨 — cctvsec는 https 지원
        if url.startswith("http://"):
            url = "https://" + url[len("http://"):]
        data = {
            "name": best.get("cctvname") or "",
            "url": url,
            "format": best.get("cctvformat") or "",
        }
    # 캐시가 무한히 크지 않게 오래된 항목 정리
    if len(_CCTV_FRESH_CACHE) > 200:
        for k in [k for k, v in _CCTV_FRESH_CACHE.items() if (now - v["at"]) > _CCTV_FRESH_TTL]:
            _CCTV_FRESH_CACHE.pop(k, None)
    _CCTV_FRESH_CACHE[ck] = {"at": now, "data": data}
    return data


# ── ITS(국가교통정보센터) 링크 단위 실시간 소통 — its.go.kr 공식 방식 ─────────
# 2026-09-02 전면 교체: 콘존(도로공사) 기반 소통색 대신 ITS trafficInfo(링크ID
# 단위 실시간 속도)를 표준노드링크 지오메트리(MOCT_LINK.shp, ROAD_RANK=101)와
# LINK_ID로 조인해 그린다 — ITS 공식 사이트 지도와 동일한 데이터·동일한 룩.
# 실측(메인 세션, 2026-09-02): type=ex는 이 키에서 4002 에러 → 반드시 type=all
# + 우리 쪽 고속도로 필터. 전국 bbox 1회 호출 약 73MB/37만 링크/수십 초 —
# 반드시 백그라운드 스레드에서 수집하고 요청은 즉시 캐시를 반환한다.
# shp 고속도로 링크 28,375개 중 약 13,900개(49%)가 ITS 속도를 보유(본선 대부분)
# — 나머지(검지기 없는 램프 등)는 색칠하지 않는다(기본 초록 노선 라인이 평시
# 모습으로 깔려 있어 ITS 룩과 동일).
import gzip

LINK_GEOMETRY_FILE = CACHE_DIR / "link_geometry.json.gz"
_LINK_GEOMETRY_CACHE = {"data": None, "loaded": False}

# 링크 지오메트리 단순화 허용오차(도 단위, 약 10m) — 렌더 품질 유지하며 점 수 절감.
_LINK_SIMPLIFY_TOLERANCE = 0.0001

ITS_TRAFFIC_URL = "https://openapi.its.go.kr:9443/trafficInfo"
_ITS_TRAFFIC_TIMEOUT = 300  # 응답 약 73MB — 넉넉하게

TTL["its_link_speeds"] = 5 * 60  # ITS 링크 속도 5분 TTL

_ITS_SPEED_CACHE = {"data": None, "at": 0}
_ITS_SPEED_FETCH_LOCK = threading.Lock()
_ITS_SPEED_FETCHING = {"flag": False}


def build_link_geometry():
    """MOCT_LINK.shp에서 고속도로(ROAD_RANK=101) 링크만 추출해 WGS84 변환·
    simplify(약 10m) 후 LINK_ID별 좌표·도로명·노드ID를 link_geometry.json.gz에
    저장한다(정적 캐시, TTL 없음 — 수동 재빌드). 표준노드링크는 상행/하행이
    별도 링크·별도 지오메트리라 링크별로 그대로 그리면 방향별 두 줄이 나온다.

    수동 실행:
        .venv\\Scripts\\python.exe -c "from tools.restarea_helpers import build_link_geometry; build_link_geometry()"
    """
    import geopandas as gpd  # 무거운 의존성 — 수동 재빌드 시에만 로드

    if not NODELINK_LINK_SHP.exists():
        raise FileNotFoundError(f"nodelink shapefile not found: {NODELINK_LINK_SHP}")

    t0 = time.time()
    gdf = gpd.read_file(str(NODELINK_LINK_SHP), encoding="cp949",
                         where=f"ROAD_RANK='{_NODELINK_EXPRESSWAY_RANK}'")
    n_all = len(gdf)
    # 연결로(램프·톨게이트 진출입로) 제외 — CONNECT=1이 연결로(실측 7,144/28,375).
    # 연결로는 제한속도 자체가 30~50km/h라 본선 기준(40↓=정체)으로 색칠하면
    # 실제로는 원활한데 "가짜 정체"로 보인다(2026-09-03 사용자 CCTV 대조 확인).
    # ITS 공식 지도도 본선만 색칠한다. 추가로 본선이어도 제한속도 60km/h 이하
    # 구간(요금소 광장 등)은 같은 이유로 제외한다.
    gdf = gdf[gdf["CONNECT"].astype(str).str.strip() != "1"]
    def _spd(v):
        try:
            return float(str(v).strip())
        except Exception:
            return None
    gdf = gdf[gdf["MAX_SPD"].map(_spd).map(lambda s: s is None or s > 60)]
    gdf = gdf.to_crs("EPSG:4326")
    gdf["geometry"] = gdf.geometry.simplify(_LINK_SIMPLIFY_TOLERANCE)
    print(f"[build_link_geometry] expressway mainline links: {len(gdf)}/{n_all} "
          f"(ramps/toll-plaza excluded) ({time.time() - t0:.1f}s)")

    result = {}
    pt_total = 0
    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.geom_type != "LineString":
            continue
        link_id = str(row.get("LINK_ID") or "").strip()
        if not link_id:
            continue
        coords = [[round(lat, 5), round(lng, 5)] for lng, lat in geom.coords]
        if len(coords) < 2:
            continue
        pt_total += len(coords)
        result[link_id] = {
            "c": coords,
            "f": str(row.get("F_NODE") or "").strip(),
            "t": str(row.get("T_NODE") or "").strip(),
            "rn": (row.get("ROAD_NAME") or "").strip() or None,
            "no": str(row.get("ROAD_NO") or "").strip() or None,
        }

    payload = {"fetched_at": time.time(), "data": result}
    with gzip.open(LINK_GEOMETRY_FILE, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    _LINK_GEOMETRY_CACHE["data"] = result
    _LINK_GEOMETRY_CACHE["loaded"] = True
    size_mb = LINK_GEOMETRY_FILE.stat().st_size / 1024 / 1024
    print(f"[build_link_geometry] done: links={len(result)} points={pt_total} "
          f"file={size_mb:.1f}MB -> {LINK_GEOMETRY_FILE}")
    return {"link_count": len(result), "point_total": pt_total, "file_mb": round(size_mb, 2)}


def _load_link_geometry():
    if _LINK_GEOMETRY_CACHE["loaded"]:
        return _LINK_GEOMETRY_CACHE["data"] or {}
    data = {}
    if LINK_GEOMETRY_FILE.exists():
        try:
            with gzip.open(LINK_GEOMETRY_FILE, "rt", encoding="utf-8") as f:
                payload = json.load(f)
            data = payload.get("data") or {}
        except Exception as e:
            print(f"[_load_link_geometry] 로드 실패: {e}")
            data = {}
    _LINK_GEOMETRY_CACHE["data"] = data
    _LINK_GEOMETRY_CACHE["loaded"] = True
    return data


def _fetch_its_link_speeds():
    """ITS trafficInfo 전국 1회 호출 → {linkId: speed(float)}. link_geometry에
    있는 LINK_ID만 남긴다(37만 → 약 1.4만, 메모리·디스크 절약). resultCode!=0이면
    실패(예외) — 호출측이 마지막 정상 캐시를 유지한다."""
    key = os.getenv("ITS_OPENAPI_KEY", "").strip()
    if not key:
        raise RuntimeError("ITS_OPENAPI_KEY not set")
    geo = _load_link_geometry()
    if not geo:
        raise RuntimeError("link_geometry not built")
    params = {
        "apiKey": key, "type": "all",
        "minX": "124.5", "maxX": "132.0", "minY": "33.0", "maxY": "39.0",
        "getType": "json",
    }
    r = requests.get(ITS_TRAFFIC_URL, params=params, timeout=_ITS_TRAFFIC_TIMEOUT)
    r.raise_for_status()
    d = r.json()
    header = d.get("header") or {}
    if header.get("resultCode") not in (0, "0"):
        raise RuntimeError(f"ITS resultCode={header.get('resultCode')} {header.get('resultMsg')}")
    items = ((d.get("body") or {}).get("items")) or []
    out = {}
    for it in items:
        lid = str(it.get("linkId") or "").strip()
        if lid not in geo:
            continue
        sp = _to_float(it.get("speed"))
        if sp is None or sp < 0:
            continue
        out[lid] = sp
    return out


def _its_speeds_background_refresh():
    try:
        data = _fetch_its_link_speeds()
        _save_cache("its_link_speeds", data)
        _ITS_SPEED_CACHE["data"] = data
        _ITS_SPEED_CACHE["at"] = time.time()
        _LINK_CHAINS_CACHE["at"] = 0  # 체인 병합 캐시 무효화(새 속도 반영)
        print(f"[_its_speeds_background_refresh] done: matched links={len(data)}")
    except Exception as e:
        print(f"[_its_speeds_background_refresh] 수집 실패: {e}")
    finally:
        with _ITS_SPEED_FETCH_LOCK:
            _ITS_SPEED_FETCHING["flag"] = False


def _its_link_speeds_cached():
    """ITS 링크 속도 조회 — 수집이 수십 초 걸리므로 요청 스레드를 블로킹하지 않고
    (realUnitTrtm과 동일 패턴) 캐시를 즉시 반환, 만료 시 백그라운드에서 갱신한다."""
    now = time.time()
    if _ITS_SPEED_CACHE["data"] is not None and (now - _ITS_SPEED_CACHE["at"]) < TTL["its_link_speeds"]:
        return _ITS_SPEED_CACHE["data"]
    c = _load_cache("its_link_speeds")
    if c:
        _ITS_SPEED_CACHE["data"] = c.get("data") or {}
        _ITS_SPEED_CACHE["at"] = c.get("fetched_at", 0)
        fresh = (now - c.get("fetched_at", 0)) < TTL["its_link_speeds"]
    else:
        fresh = False
        if _ITS_SPEED_CACHE["data"] is None:
            _ITS_SPEED_CACHE["data"] = {}
    if not fresh:
        with _ITS_SPEED_FETCH_LOCK:
            already = _ITS_SPEED_FETCHING["flag"]
            if not already:
                _ITS_SPEED_FETCHING["flag"] = True
        if not already:
            threading.Thread(target=_its_speeds_background_refresh, daemon=True).start()
    return _ITS_SPEED_CACHE["data"] or {}


_LINK_CHAINS_CACHE = {"data": None, "at": 0, "speeds_at": 0}
_LINK_CHAINS_TTL = 5 * 60


def _merge_link_chains(geo, speeds):
    """같은 소통 등급(level)의 연속 링크(endNode==다음 startNode)를 하나의
    폴리라인 체인으로 병합한다 — 링크 1.4만 개를 그대로 보내면 프론트가 폴리라인
    1.4만 개를 그려야 해서다. 반환: [{"coords", "speed"(체인 평균), "level", "road"}]."""
    entries = {}  # link_id -> (level, speed, g)
    by_start = {}  # (level, f_node) -> [link_id, ...]
    incoming = set()  # (level, t_node)
    for lid, sp in speeds.items():
        g = geo.get(lid)
        if not g:
            continue
        level = _speed_level_5(sp)
        if level is None:
            continue
        entries[lid] = (level, sp, g)
        by_start.setdefault((level, g["f"]), []).append(lid)
        incoming.add((level, g["t"]))

    used = set()
    chains = []

    def _walk(start_lid):
        chain_coords = []
        speed_sum, n = 0.0, 0
        road = None
        lid = start_lid
        while lid is not None and lid not in used:
            used.add(lid)
            level, sp, g = entries[lid]
            coords = g["c"]
            if chain_coords and chain_coords[-1] == coords[0]:
                chain_coords.extend(coords[1:])
            else:
                chain_coords.extend(coords)
            speed_sum += sp
            n += 1
            if road is None and g.get("rn"):
                road = g["rn"]
            # 다음 링크: 같은 level에서 이 링크의 T_NODE로 시작하는 미사용 링크
            nxt = None
            for cand in by_start.get((level, g["t"]), []):
                if cand not in used:
                    nxt = cand
                    break
            lid = nxt
        if chain_coords and n:
            chains.append({
                "coords": chain_coords,
                "speed": round(speed_sum / n, 1),
                "level": entries[start_lid][0],
                "road": road,
            })

    # 1) 체인 시작점(같은 level의 다른 링크가 이 링크의 F_NODE로 끝나지 않는 링크)부터
    for lid, (level, _sp, g) in entries.items():
        if lid in used:
            continue
        if (level, g["f"]) not in incoming:
            _walk(lid)
    # 2) 남은 것(순환 등) 처리
    for lid in entries:
        if lid not in used:
            _walk(lid)
    return chains


def get_link_traffic():
    """/api/restarea/link-traffic 응답 본문. 체인 병합 결과를 속도 캐시 세대
    기준으로 메모이즈한다(같은 속도 데이터로 매 요청 병합 재계산 방지)."""
    speeds = _its_link_speeds_cached()
    now = time.time()
    cached = _LINK_CHAINS_CACHE["data"]
    if (cached is not None and _LINK_CHAINS_CACHE["speeds_at"] == _ITS_SPEED_CACHE["at"]
            and (now - _LINK_CHAINS_CACHE["at"]) < _LINK_CHAINS_TTL):
        return cached
    geo = _load_link_geometry()
    chains = _merge_link_chains(geo, speeds or {})
    result = {
        "ok": True,
        "updated_at": _ITS_SPEED_CACHE["at"] or None,
        "link_count": len(speeds or {}),
        "chain_count": len(chains),
        "links": chains,
    }
    _LINK_CHAINS_CACHE["data"] = result
    _LINK_CHAINS_CACHE["at"] = now
    _LINK_CHAINS_CACHE["speeds_at"] = _ITS_SPEED_CACHE["at"]
    return result


# ── 일반도로(국도·지방도 등) 실시간 교통량 레이어 — 2026-09-02 신설 ──────────
# 고속도로 링크 파이프라인(build_link_geometry/_fetch_its_link_speeds/link-traffic)을
# 본뜬 뷰포트 단위 버전. ITS 공식 사이트처럼 "축소하면 고속도로만, 확대(줌 12+)하면
# 일반도로까지" 보인다. 대상은 ROAD_RANK 102(도시고속)~106(지방도)만 — 107 시군도
# (94만 링크, 골목 수준)는 ITS도 그리지 않으므로 제외. CONNECT=='1'(연결로)도 제외
# (고속도로와 동일한 오탐 방지). MAX_SPD 필터는 일반도로엔 적용하지 않는다(도심
# 30~50 제한이 정상) — 대신 등급 기준을 도로등급별 절대속도로 달리 둔다.
LOCAL_LINKS_DIR = CACHE_DIR / "local_links"
_LOCAL_RANKS = ("102", "103", "104", "105", "106")
_LOCAL_CELL_DEG = 0.25          # 0.25° 격자 셀
_LOCAL_CELL_LRU_MAX = 40        # 메모리에 유지할 최근 셀 수
_LOCAL_BBOX_MAX_DEG = 2.0       # 이보다 큰 bbox 요청은 거부(프론트는 줌 12+에서만 호출)
_LOCAL_RESPONSE_MAX_BYTES = 3 * 1024 * 1024  # 3MB 초과 시 rank를 102·103만으로 재계산

TTL["its_local_speeds"] = 5 * 60  # 스냅-bbox별 ITS 속도 캐시 5분

# 셀 lazy 로드 + LRU (dict + 접근 순서). 값: {LINK_ID: {"c","f","t","rn","rk"}}
_LOCAL_CELL_CACHE = {}   # cell_key -> dict (없는 셀은 {} 저장해 재시도 방지)
_LOCAL_CELL_ORDER = []   # 최근 접근 순서(뒤가 최신)
_LOCAL_CELL_LOCK = threading.Lock()

# 스냅-bbox별 ITS 속도 캐시: key -> {"at": ts, "data": {linkId: speed}}
_LOCAL_SPEED_CACHE = {}
_LOCAL_SPEED_LOCK = threading.Lock()


def _local_cell_key(lat, lng):
    """좌표 → 0.25° 격자 셀 키(파일명). floor(lat*4), floor(lng*4) 정수 인덱스."""
    return f"{int(math.floor(lat * 4))}_{int(math.floor(lng * 4))}"


def _local_cell_path(cell_key):
    return LOCAL_LINKS_DIR / f"{cell_key}.json.gz"


def build_local_link_geometry():
    """MOCT_LINK.shp에서 ROAD_RANK 102~106 + CONNECT!='1' 링크를 추출해 WGS84 변환·
    simplify(고속도로와 동일 tolerance) 후 0.25° 격자 셀별 gz 파일로 저장한다.
    링크가 셀 경계에 걸치면 시작 좌표 기준 셀 하나에만 넣는다(경계 미세 누락 허용).
    약 58만 링크라 수 분 걸린다 — 수동/관리자 재빌드 전용.

    수동 실행:
        .venv\\Scripts\\python.exe -c "from tools.restarea_helpers import build_local_link_geometry; build_local_link_geometry()"
    """
    import geopandas as gpd  # 무거운 의존성 — 재빌드 시에만 로드

    if not NODELINK_LINK_SHP.exists():
        raise FileNotFoundError(f"nodelink shapefile not found: {NODELINK_LINK_SHP}")

    t0 = time.time()
    ranks_sql = ",".join(f"'{r}'" for r in _LOCAL_RANKS)
    gdf = gpd.read_file(str(NODELINK_LINK_SHP), encoding="cp949",
                         where=f"ROAD_RANK IN ({ranks_sql})")
    n_all = len(gdf)
    gdf = gdf[gdf["CONNECT"].astype(str).str.strip() != "1"]
    gdf = gdf.to_crs("EPSG:4326")
    gdf["geometry"] = gdf.geometry.simplify(_LINK_SIMPLIFY_TOLERANCE)
    print(f"[build_local_link_geometry] local links: {len(gdf)}/{n_all} "
          f"(connectors excluded) ({time.time() - t0:.1f}s)")

    cells = {}  # cell_key -> {link_id: {...}}
    pt_total = 0
    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.geom_type != "LineString":
            continue
        link_id = str(row.get("LINK_ID") or "").strip()
        if not link_id:
            continue
        coords = [[round(lat, 5), round(lng, 5)] for lng, lat in geom.coords]
        if len(coords) < 2:
            continue
        pt_total += len(coords)
        cell = _local_cell_key(coords[0][0], coords[0][1])
        cells.setdefault(cell, {})[link_id] = {
            "c": coords,
            "f": str(row.get("F_NODE") or "").strip(),
            "t": str(row.get("T_NODE") or "").strip(),
            "rn": (row.get("ROAD_NAME") or "").strip() or None,
            "rk": str(row.get("ROAD_RANK") or "").strip(),
        }

    LOCAL_LINKS_DIR.mkdir(parents=True, exist_ok=True)
    # 이전 빌드 잔여 셀 파일 제거(링크 이동/삭제 반영)
    for old in LOCAL_LINKS_DIR.glob("*.json.gz"):
        try:
            old.unlink()
        except Exception:
            pass
    total_bytes = 0
    link_total = 0
    for cell, links in cells.items():
        p = _local_cell_path(cell)
        with gzip.open(p, "wt", encoding="utf-8") as f:
            json.dump(links, f, ensure_ascii=False)
        total_bytes += p.stat().st_size
        link_total += len(links)
    # 메모리 캐시 초기화(새 빌드 반영)
    with _LOCAL_CELL_LOCK:
        _LOCAL_CELL_CACHE.clear()
        del _LOCAL_CELL_ORDER[:]
    elapsed = time.time() - t0
    print(f"[build_local_link_geometry] done: cells={len(cells)} links={link_total} "
          f"points={pt_total} total={total_bytes/1024/1024:.1f}MB ({elapsed:.1f}s)")
    return {"cell_count": len(cells), "link_count": link_total, "point_total": pt_total,
            "total_mb": round(total_bytes / 1024 / 1024, 2), "elapsed_sec": round(elapsed, 1)}


def _load_local_cell(cell_key):
    """셀 파일 lazy 로드 + LRU(_LOCAL_CELL_LRU_MAX개 유지). 없는 셀은 {}."""
    with _LOCAL_CELL_LOCK:
        if cell_key in _LOCAL_CELL_CACHE:
            try:
                _LOCAL_CELL_ORDER.remove(cell_key)
            except ValueError:
                pass
            _LOCAL_CELL_ORDER.append(cell_key)
            return _LOCAL_CELL_CACHE[cell_key]
    data = {}
    p = _local_cell_path(cell_key)
    if p.exists():
        try:
            with gzip.open(p, "rt", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"[_load_local_cell] {cell_key} 로드 실패: {e}")
            data = {}
    with _LOCAL_CELL_LOCK:
        _LOCAL_CELL_CACHE[cell_key] = data
        try:
            _LOCAL_CELL_ORDER.remove(cell_key)
        except ValueError:
            pass
        _LOCAL_CELL_ORDER.append(cell_key)
        while len(_LOCAL_CELL_ORDER) > _LOCAL_CELL_LRU_MAX:
            evict = _LOCAL_CELL_ORDER.pop(0)
            _LOCAL_CELL_CACHE.pop(evict, None)
    return data


def _snap_bbox(min_x, max_x, min_y, max_y):
    """요청 bbox를 0.25° 격자에 정렬(바깥쪽으로 확장 스냅) — 캐시 키 겸 셀 범위."""
    d = _LOCAL_CELL_DEG
    return (
        math.floor(min_x / d) * d, math.ceil(max_x / d) * d,
        math.floor(min_y / d) * d, math.ceil(max_y / d) * d,
    )


def _fetch_its_local_speeds(snapped):
    """스냅-bbox 단위 ITS trafficInfo 호출 → {linkId: speed}. TTL 5분 캐시.
    좁은 뷰포트 bbox는 수 초 이내(실측)라 요청 스레드에서 직접 호출한다
    (timeout 15초). 실패 시 같은 키의 마지막 캐시(만료돼도)를 반환하고, 그것도
    없으면 빈 dict — 지어내지 않는다."""
    key = "|".join(f"{v:.2f}" for v in snapped)
    now = time.time()
    with _LOCAL_SPEED_LOCK:
        c = _LOCAL_SPEED_CACHE.get(key)
    if c and (now - c["at"]) < TTL["its_local_speeds"]:
        return c["data"]
    api_key = os.getenv("ITS_OPENAPI_KEY", "").strip()
    if not api_key:
        return (c or {}).get("data") or {}
    min_x, max_x, min_y, max_y = snapped
    try:
        r = requests.get(ITS_TRAFFIC_URL, params={
            "apiKey": api_key, "type": "all",
            "minX": f"{min_x:.4f}", "maxX": f"{max_x:.4f}",
            "minY": f"{min_y:.4f}", "maxY": f"{max_y:.4f}",
            "getType": "json",
        }, timeout=15)
        r.raise_for_status()
        d = r.json()
        header = d.get("header") or {}
        if header.get("resultCode") not in (0, "0"):
            raise RuntimeError(f"ITS resultCode={header.get('resultCode')} {header.get('resultMsg')}")
        items = ((d.get("body") or {}).get("items")) or []
        out = {}
        for it in items:
            lid = str(it.get("linkId") or "").strip()
            if not lid:
                continue
            sp = _to_float(it.get("speed"))
            if sp is None or sp < 0:
                continue
            out[lid] = sp
        with _LOCAL_SPEED_LOCK:
            _LOCAL_SPEED_CACHE[key] = {"at": now, "data": out}
        return out
    except Exception as e:
        print(f"[_fetch_its_local_speeds] 호출 실패({key}): {e}")
        return (c or {}).get("data") or {}


def _local_speed_level(speed, rank):
    """일반도로 소통 등급(3단계, 도로등급별 절대속도 — ITS 표준 준용).
    102 도시고속: ≥50 원활 / 25~50 서행 / <25 정체.
    103~106 일반도로: ≥25 원활 / 15~25 서행 / <15 정체.
    level 키는 기존 smooth/slow/jam을 재사용(프론트 색상 재활용)."""
    if speed is None:
        return None
    if rank == "102":
        if speed >= 50:
            return "smooth"
        if speed >= 25:
            return "slow"
        return "jam"
    if speed >= 25:
        return "smooth"
    if speed >= 15:
        return "slow"
    return "jam"


def _merge_local_chains(geo, speeds):
    """_merge_link_chains의 일반도로 버전 — level을 도로등급별 절대속도
    (_local_speed_level)로 계산하고, 체인에 rank(대표 도로등급)도 담는다."""
    entries = {}
    by_start = {}
    incoming = set()
    for lid, sp in speeds.items():
        g = geo.get(lid)
        if not g:
            continue
        level = _local_speed_level(sp, g.get("rk"))
        if level is None:
            continue
        entries[lid] = (level, sp, g)
        by_start.setdefault((level, g["f"]), []).append(lid)
        incoming.add((level, g["t"]))

    used = set()
    chains = []

    def _walk(start_lid):
        chain_coords = []
        speed_sum, n = 0.0, 0
        road = None
        rank = None
        lid = start_lid
        while lid is not None and lid not in used:
            used.add(lid)
            level, sp, g = entries[lid]
            coords = g["c"]
            if chain_coords and chain_coords[-1] == coords[0]:
                chain_coords.extend(coords[1:])
            else:
                chain_coords.extend(coords)
            speed_sum += sp
            n += 1
            if road is None and g.get("rn"):
                road = g["rn"]
            if rank is None and g.get("rk"):
                rank = g["rk"]
            nxt = None
            for cand in by_start.get((level, g["t"]), []):
                if cand not in used:
                    nxt = cand
                    break
            lid = nxt
        if chain_coords and n:
            chains.append({
                "coords": chain_coords,
                "speed": round(speed_sum / n, 1),
                "level": entries[start_lid][0],
                "road": road,
                "rank": rank,
            })

    for lid, (level, _sp, g) in entries.items():
        if lid in used:
            continue
        if (level, g["f"]) not in incoming:
            _walk(lid)
    for lid in entries:
        if lid not in used:
            _walk(lid)
    return chains


def get_local_traffic(min_x, max_x, min_y, max_y):
    """/api/restarea/local-traffic 응답 본문. 뷰포트 bbox 내 일반도로(102~106)
    링크의 실시간 소통 체인을 반환한다. 응답이 3MB를 넘으면 rank를 102·103만으로
    좁혀 재계산한다(안전장치)."""
    min_x, max_x = _to_float(min_x), _to_float(max_x)
    min_y, max_y = _to_float(min_y), _to_float(max_y)
    if None in (min_x, max_x, min_y, max_y) or min_x >= max_x or min_y >= max_y:
        return {"ok": False, "error": "invalid bbox"}
    if (max_x - min_x) > _LOCAL_BBOX_MAX_DEG or (max_y - min_y) > _LOCAL_BBOX_MAX_DEG:
        # 프론트는 줌 12 미만에서 안 부른다 — 방어용으로 조용히 빈 결과 반환.
        return {"ok": True, "links": [], "chain_count": 0, "link_count": 0, "note": "bbox too large"}

    snapped = _snap_bbox(min_x, max_x, min_y, max_y)
    s_minx, s_maxx, s_miny, s_maxy = snapped

    # 스냅 범위의 셀들 지오메트리 합치기
    geo = {}
    d = _LOCAL_CELL_DEG
    lat_i = int(round(s_miny / d))
    lat_end = int(round(s_maxy / d))
    lng_end = int(round(s_maxx / d))
    while lat_i < lat_end:
        lng_i = int(round(s_minx / d))
        while lng_i < lng_end:
            cell = _load_local_cell(f"{lat_i}_{lng_i}")
            if cell:
                geo.update(cell)
            lng_i += 1
        lat_i += 1

    if not geo:
        return {"ok": True, "links": [], "chain_count": 0, "link_count": 0}

    speeds_all = _fetch_its_local_speeds(snapped)
    speeds = {lid: sp for lid, sp in speeds_all.items() if lid in geo}
    chains = _merge_local_chains(geo, speeds)

    result = {"ok": True, "link_count": len(speeds), "chain_count": len(chains), "links": chains}
    body = json.dumps(result, ensure_ascii=False)
    if len(body.encode("utf-8")) > _LOCAL_RESPONSE_MAX_BYTES:
        narrow_geo = {lid: g for lid, g in geo.items() if g.get("rk") in ("102", "103")}
        speeds = {lid: sp for lid, sp in speeds_all.items() if lid in narrow_geo}
        chains = _merge_local_chains(narrow_geo, speeds)
        result = {"ok": True, "link_count": len(speeds), "chain_count": len(chains),
                  "links": chains, "note": "narrowed to ranks 102-103 (size cap)"}
    return result


# ── 주요 도시간 소요시간 (2026-09-03 신설) ───────────────────────────────────
# ITS 공식 메인의 "주요 도시간 소요시간" 패널처럼 서울 기점 6개 도시의
# 거리·소요시간을 계산한다. 표준노드링크 rank101 그래프에서 좌표→최근접 노드
# 스냅 → Dijkstra 경로의 간선별 소요시간(간선길이 / 그 간선 링크의 ITS 실시간
# 속도, 실측 속도가 없으면 100km/h 기본) 합산. 경로를 못 찾으면 그 도시는
# 결과에서 제외한다(지어내지 않음).
#
# 좌표는 각 도시 인근 고속도로 본선 위 지점(2026-09-03 코드 검증 — 전부
# rank101 그래프 노드에서 3km 이내인지 _snap_graph_node로 확인하며 조정).
# routeName은 타일 클릭 시 지도에서 활성화할 대표 노선(실재 노선명만 기재).
# 2026-09-03 확장: 서울 단일 기점 → 거점 8곳 행렬(출발지 개인화 — 방문자 위치
# 최근접 거점 또는 수동 선택 기준으로 프론트가 행을 골라 보여준다).
# 좌표는 각 거점 인근 고속도로 본선 위 지점 — 전부 _snap_graph_node 3km 검증을
# 통과해야 하며, 실패한 거점은 행렬에서 자동 제외된다(지어내지 않음).
# routeName은 "그 거점 방면 대표 노선"(타일 클릭 시 지도 활성화용 — 실재 노선명).
_CITY_HUBS = [
    {"name": "서울", "lat": 37.458, "lng": 127.043, "routeName": "경부선"},   # 서울만남의광장 부근
    {"name": "대전", "lat": 36.370, "lng": 127.420, "routeName": "경부선"},   # 대전IC 부근
    {"name": "대구", "lat": 35.880, "lng": 128.500, "routeName": "경부선"},   # 금호JC 부근
    {"name": "부산", "lat": 35.245, "lng": 129.089, "routeName": "경부선"},   # 노포/부산TG 부근(스냅 0.4km 검증)
    {"name": "광주", "lat": 35.220, "lng": 126.850, "routeName": "호남선"},   # 광주 도심 인근 호남선
    {"name": "강릉", "lat": 37.720, "lng": 128.900, "routeName": "영동선"},   # 강릉JC 부근
    {"name": "전주", "lat": 35.894, "lng": 127.061, "routeName": "호남선"},   # 전주IC 부근 호남선(스냅 0.3km 검증)
    {"name": "원주", "lat": 37.340, "lng": 127.950, "routeName": "영동선"},   # 원주 인근 영동선
]
_CITY_SNAP_MAX_KM = 3.0        # 이보다 멀면 좌표가 본선에서 벗어난 것 — 결과 제외
_CITY_DEFAULT_SPEED = 100.0    # 실시간 속도 미매칭 간선 기본 속도(km/h)
_CITY_TIMES_CACHE = {"data": None, "at": 0}
_CITY_TIMES_TTL = 20 * 60      # 20분(2026-09-03 사용자 확정 보장: "30분 지나서 온
# 방문자는 반드시 새 값"). 라이브는 정적 발행이라 방문자가 재계산을 직접 못
# 깨우므로, 계산캐시 최대 나이 20분 + 10분 발행주기 = 라이브 자료 나이 상한
# 30분으로 맞춘다. 로컬(5001)은 요청 시점 lazy 재계산이라 그대로 충족.


def _snap_graph_node(lat, lng):
    """좌표 → rank101 그래프 최근접 노드. (node_id, dist_km) 또는 (None, None)."""
    graph = _load_nodelink_graph()
    if not graph:
        return None, None
    best_id, best_d = None, None
    for nid, (nlat, nlng) in graph["nodes"].items():
        # 대략 0.1도(±11km) 박스 프리필터로 haversine 횟수 절감
        if abs(nlat - lat) > 0.2 or abs(nlng - lng) > 0.25:
            continue
        d = _haversine(lat, lng, nlat, nlng)
        if d is not None and (best_d is None or d < best_d):
            best_id, best_d = nid, d
    return best_id, best_d


def _dijkstra_edge_chain(start_id, end_id):
    """_dijkstra_path와 동일하되 좌표 대신 간선 체인 [(edge_idx, forward), ...]을
    반환한다 — 간선별 링크 실시간 속도를 얹어 소요시간을 계산하는 용도."""
    import heapq

    adj, edges = _graph_adjacency()
    if not adj or start_id not in adj or end_id not in adj:
        return None
    if start_id == end_id:
        return 0.0, []
    dist = {start_id: 0.0}
    prev = {}
    visited = set()
    heap = [(0.0, start_id)]
    while heap:
        d, u = heapq.heappop(heap)
        if u in visited:
            continue
        visited.add(u)
        if u == end_id:
            break
        for v, edge_idx, forward in adj.get(u, []):
            if v in visited:
                continue
            nd = d + edges[edge_idx][2]
            if nd < dist.get(v, float("inf")):
                dist[v] = nd
                prev[v] = (u, edge_idx, forward)
                heapq.heappush(heap, (nd, v))
    if end_id not in dist:
        return None
    chain = []
    cur = end_id
    while cur != start_id:
        p, edge_idx, forward = prev[cur]
        chain.append((edge_idx, forward))
        cur = p
    chain.reverse()
    return dist[end_id], chain


_LINK_BY_NODEPAIR_CACHE = {"data": None}


def _link_by_nodepair():
    """link_geometry(LINK_ID→f/t 노드)에서 (f,t)→LINK_ID 역인덱스. 그래프 간선을
    ITS 링크 속도와 조인하는 데 쓴다(link_geometry는 램프 제외라 일부 간선은
    매칭이 안 됨 — 그 간선은 기본속도 폴백)."""
    if _LINK_BY_NODEPAIR_CACHE["data"] is not None:
        return _LINK_BY_NODEPAIR_CACHE["data"]
    idx = {}
    for lid, g in _load_link_geometry().items():
        f, t = g.get("f"), g.get("t")
        if f and t:
            idx[(f, t)] = lid
    _LINK_BY_NODEPAIR_CACHE["data"] = idx
    return idx


def _dijkstra_edge_chains_multi(start_id, end_ids):
    """단일 출발 다중 도착 Dijkstra — end_ids 전부 방문(또는 힙 소진)까지 돌고
    {end_id: (dist_km, [(edge_idx, forward), ...])} 반환. 8기점 행렬을 기점당
    1회 탐색으로 끝내기 위한 확장(쌍별 _dijkstra_edge_chain 56회 호출 대체)."""
    import heapq

    adj, edges = _graph_adjacency()
    if not adj or start_id not in adj:
        return {}
    targets = {e for e in end_ids if e in adj and e != start_id}
    if not targets:
        return {}
    dist = {start_id: 0.0}
    prev = {}
    visited = set()
    remaining = set(targets)
    heap = [(0.0, start_id)]
    while heap and remaining:
        d, u = heapq.heappop(heap)
        if u in visited:
            continue
        visited.add(u)
        remaining.discard(u)
        for v, edge_idx, forward in adj.get(u, []):
            if v in visited:
                continue
            nd = d + edges[edge_idx][2]
            if nd < dist.get(v, float("inf")):
                dist[v] = nd
                prev[v] = (u, edge_idx, forward)
                heapq.heappush(heap, (nd, v))
    out = {}
    for end_id in targets:
        if end_id not in dist or (end_id not in visited and end_id in remaining):
            continue
        chain = []
        cur = end_id
        ok = True
        while cur != start_id:
            p = prev.get(cur)
            if not p:
                ok = False
                break
            chain.append((p[1], p[2]))
            cur = p[0]
        if not ok:
            continue
        chain.reverse()
        out[end_id] = (dist[end_id], chain)
    return out


def _compute_city_times():
    """거점 8곳 × 나머지 7곳 소요시간 행렬.
    반환: [{name, lat, lng, items:[{dest, km, minutes, routeName}]}] — 스냅 3km
    검증 실패 거점은 통째로 제외(기점·목적지 양쪽에서)."""
    speeds = _its_link_speeds_cached() or {}
    pair_idx = _link_by_nodepair()
    _adj, edges = _graph_adjacency()
    if not edges:
        return []
    hubs = []
    for h in _CITY_HUBS:
        nid, snap = _snap_graph_node(h["lat"], h["lng"])
        if not nid or snap is None or snap > _CITY_SNAP_MAX_KM:
            print(f"[city-times] 거점 스냅 실패로 제외: {h['name']} (snap={snap})")
            continue
        hubs.append({**h, "node": nid})
    origins = []
    for o in hubs:
        chains = _dijkstra_edge_chains_multi(
            o["node"], [d["node"] for d in hubs if d["node"] != o["node"]])
        items = []
        for d in hubs:
            if d["node"] == o["node"]:
                continue
            r = chains.get(d["node"])
            if not r:
                continue
            total_km, chain = r
            minutes = 0.0
            for edge_idx, forward in chain:
                f_node, t_node, w_km, _c = edges[edge_idx]
                pair = (f_node, t_node) if forward else (t_node, f_node)
                lid = pair_idx.get(pair) or pair_idx.get((pair[1], pair[0]))
                sp = speeds.get(lid) if lid else None
                if sp is None or sp < 10 or sp > 200:
                    sp = _CITY_DEFAULT_SPEED
                minutes += w_km / sp * 60.0
            items.append({
                "dest": d["name"],
                "km": int(round(total_km)),
                "minutes": int(round(minutes)),
                "routeName": d.get("routeName"),
            })
        if items:
            origins.append({"name": o["name"], "lat": o["lat"], "lng": o["lng"],
                            "items": items})
    return origins


def get_city_times(origin=None):
    """/api/restarea/city-times 응답 본문(20분 캐시).
    하위호환: items는 origin(기본 서울) 행, 전체 행렬은 origins 필드에."""
    now = time.time()
    if (_CITY_TIMES_CACHE["data"] is None
            or (now - _CITY_TIMES_CACHE["at"]) >= _CITY_TIMES_TTL):
        try:
            origins = _compute_city_times()
        except Exception as e:
            print(f"[get_city_times] 계산 실패: {e}")
            origins = (_CITY_TIMES_CACHE["data"] or {}).get("origins") or []
        if origins:  # 빈 결과로 정상 캐시를 덮지 않는다
            _CITY_TIMES_CACHE["data"] = {"ok": True, "origins": origins,
                                         "updated_at": now}
            _CITY_TIMES_CACHE["at"] = now
    base = _CITY_TIMES_CACHE["data"] or {"ok": True, "origins": [], "updated_at": now}
    origins = base.get("origins") or []
    name = (origin or "서울").strip()
    row = next((o for o in origins if o["name"] == name), None)
    if row is None:
        row = next((o for o in origins if o["name"] == "서울"), None)
    return {"ok": True, "origin": (row or {}).get("name", "서울"),
            "items": (row or {}).get("items") or [], "origins": origins,
            "updated_at": base.get("updated_at", now)}


# ── refininfo.com 정적 데이터 발행 (2026-09-02 신설) ─────────────────────────
# 라이브(refininfo.com)에는 5001 API가 없으므로, 로컬 5001이 주기적으로 JSON
# 스냅샷을 만들어 /public_html/restarea-data/ 에 SFTP 업로드한다(bojogeum
# "빌드→데이터배포 2단계" 선례와 동일 접근). 프론트(rest-stop-finder.html)는
# hostname이 refininfo.com이면 /restarea-data/*.json 정적 경로를 읽는다.
#
#   10분 주기: link-traffic.json / incidents.json / traffic-summary-lite.json
#              + local-speeds/{tile}.json (1° 타일 번들, ITS 전국 1회 호출 분배)
#   일 1회  : list.json / cctv.json / route-lines.json
#   1회성   : local-geo/{tile}.json (일반도로 지오메트리 — publish_restarea_all)
#
# 업로드 실패는 조용히 스킵하고 다음 주기에 재시도(로그만). .env RESTAREA_PUBLISH=0
# 으로 끌 수 있다(기본 켬). 새 백그라운드 스레드는 5001 재시작 후부터 돈다.
RESTAREA_REMOTE_DIR = "/public_html/restarea-data"
_PUBLISH_FAST_SEC = 10 * 60      # link-traffic / incidents / summary / local-speeds
_PUBLISH_DAILY_SEC = 24 * 3600   # list / route-lines
# cctv.json은 시간당 재발행(2026-09-03): ITS 스트림 URL이 서명 포함 임시 URL
# (약 120분 유효)이라, 매시간 force 재취득해 발행분이 항상 서명 유효기간 안에
# 있게 한다. 라이브 cctv-fresh.php가 이 파일에서 최근접 CCTV URL을 꺼내 쓴다
# (라이브 서버는 ITS API 직접 호출 불가 — 해외IP/포트 차단 실측).
_PUBLISH_CCTV_SEC = 3600
_PUBLISH_STATE = {"fast_at": 0.0, "daily_at": 0.0, "cctv_at": 0.0, "thread": False}
_PUBLISH_LOCK = threading.Lock()

# 일반도로 지오메트리 발행 대상 rank — 104(지방도)까지 넣으면 용량이 커서
# publish 시 실측하며 조정한다(기본 102 도시고속 + 103 국도).
_PUBLISH_LOCAL_RANKS = ("102", "103")
_PUBLISH_LOCAL_SIMPLIFY = 0.0002   # 발행용 추가 단순화(약 20m) — shapely 사용
_PUBLISH_TILE_MAX_BYTES = 5 * 1024 * 1024

# route-lines.json 상한 — 초과 시 좌표 데시메이션으로 축소.
# 2026-09-03 모바일 경량화: 5MB → 1.5MB (실측 4.86MB — 초기 로드 최대 병목).
# 평시 초록 노선 라인은 배경 장식에 가깝고 실시간 소통색(link-traffic)이 본선을
# 덮으므로 좌표 정밀도·밀도를 크게 낮춰도 시각 품질 저하가 미미하다.
_PUBLISH_ROUTELINES_MAX_BYTES = 1536 * 1024


def _publish_enabled() -> bool:
    return os.getenv("RESTAREA_PUBLISH", "1").strip() != "0"


def _sftp_connect():
    import paramiko
    host = os.environ["FTP_HOST"]
    port = int(os.environ.get("FTP_PORT", 22))
    user = os.environ["FTP_USER_REFININFO"]
    pwd = os.environ["FTP_PASS_REFININFO"]
    tr = paramiko.Transport((host, port))
    tr.connect(username=user, password=pwd)
    sftp = paramiko.SFTPClient.from_transport(tr)
    return tr, sftp


def _sftp_mkdirs(sftp, path):
    parts = [p for p in path.split("/") if p]
    cur = ""
    for p in parts:
        cur += "/" + p
        try:
            sftp.stat(cur)
        except IOError:
            try:
                sftp.mkdir(cur)
            except IOError:
                pass


_PUBLISH_TMP = CACHE_DIR / "publish_tmp"
_PUBLISH_TMP.mkdir(parents=True, exist_ok=True)


def _sftp_put_json(sftp, rel_name, obj):
    """obj를 JSON으로 임시 저장 후 restarea-data/{rel_name}에 업로드. 바이트수 반환."""
    body = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    local = _PUBLISH_TMP / rel_name.replace("/", "__")
    local.write_text(body, encoding="utf-8")
    remote = f"{RESTAREA_REMOTE_DIR}/{rel_name}"
    if "/" in rel_name:
        _sftp_mkdirs(sftp, remote.rsplit("/", 1)[0])
    sftp.put(str(local), remote)
    return len(body.encode("utf-8"))


def _lite_traffic_summary():
    """traffic-summary-lite.json — 헤더 배너(평균·건수)와 리스트 카드 소통 배지에
    필요한 최소 필드만. segments는 level이 실측된 것만, 좌표·노선키만 남긴다."""
    s = get_traffic_summary()
    segs = []
    for seg in s.get("segments") or []:
        if not seg.get("level"):
            continue
        f, t = seg.get("from") or {}, seg.get("to") or {}
        segs.append({
            "routeNo": seg.get("routeNo"), "level": seg.get("level"),
            "from": {"lat": f.get("lat"), "lng": f.get("lng")},
            "to": {"lat": t.get("lat"), "lng": t.get("lng")},
        })
    return {
        "ok": True,
        "summary": {
            "count": s.get("count"), "avg_traffic_count": s.get("avg_traffic_count"),
            "matched_speed_count": s.get("matched_speed_count"),
            "segments": segs, "updated_at": s.get("updated_at"),
        },
        "updated_at": time.time(),
    }


def _decimate_route_lines(routes, keep_every):
    out = []
    for rt in routes:
        segs = []
        for seg in rt.get("segments") or []:
            if len(seg) <= 3:
                segs.append(seg)
                continue
            dec = seg[::keep_every]
            if dec[-1] != seg[-1]:
                dec.append(seg[-1])
            segs.append(dec)
        out.append({**rt, "segments": segs})
    return out


def _round_route_lines(routes):
    """좌표를 소수 5자리(±1m)로 반올림 — 직렬화 바이트를 크게 줄인다(경량화)."""
    out = []
    for rt in routes:
        segs = []
        for seg in rt.get("segments") or []:
            segs.append([[round(p[0], 5), round(p[1], 5)] for p in seg])
        out.append({**rt, "segments": segs})
    return out


def _route_lines_payload():
    lines = get_route_lines()
    routes = _round_route_lines(list(lines.values()))
    payload = {"ok": True, "count": len(routes), "routes": routes, "updated_at": time.time()}
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    keep = 2
    while len(body) > _PUBLISH_ROUTELINES_MAX_BYTES and keep <= 8:
        routes2 = _decimate_route_lines(routes, keep)
        payload = {"ok": True, "count": len(routes2), "routes": routes2,
                   "updated_at": time.time(), "decimated": keep}
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        keep += 1
    return payload


def _publish_tile_key(lat, lng):
    return f"{int(math.floor(lat))}_{int(math.floor(lng))}"


_LOCAL_PUB_INDEX = {"by_tile": None, "link_meta": None}  # tile -> {lid: entry}, lid -> (tile, rank)


def _build_local_publish_index(simplify=_PUBLISH_LOCAL_SIMPLIFY, ranks=_PUBLISH_LOCAL_RANKS):
    """213개 로컬 셀(gz)을 읽어 1° 타일 단위 발행용 지오메트리 번들과
    linkId→(tile,rank) 인덱스를 만든다. shapely로 추가 단순화(용량 절감)."""
    if _LOCAL_PUB_INDEX["by_tile"] is not None:
        return _LOCAL_PUB_INDEX["by_tile"], _LOCAL_PUB_INDEX["link_meta"]
    try:
        from shapely.geometry import LineString
        have_shapely = True
    except Exception:
        have_shapely = False
    by_tile = {}
    link_meta = {}
    for p in sorted(LOCAL_LINKS_DIR.glob("*.json.gz")):
        try:
            with gzip.open(p, "rt", encoding="utf-8") as f:
                cell = json.load(f)
        except Exception:
            continue
        for lid, g in cell.items():
            if g.get("rk") not in ranks:
                continue
            coords = g.get("c") or []
            if len(coords) < 2:
                continue
            if have_shapely and len(coords) > 3:
                try:
                    simp = LineString([(c[1], c[0]) for c in coords]).simplify(simplify)
                    coords = [[round(lat, 5), round(lng, 5)] for lng, lat in simp.coords]
                except Exception:
                    pass
            tile = _publish_tile_key(coords[0][0], coords[0][1])
            by_tile.setdefault(tile, {})[lid] = {"c": coords, "rn": g.get("rn"), "rk": g.get("rk")}
            link_meta[lid] = (tile, g.get("rk"))
    _LOCAL_PUB_INDEX["by_tile"] = by_tile
    _LOCAL_PUB_INDEX["link_meta"] = link_meta
    return by_tile, link_meta


def _fetch_its_speeds_raw():
    """ITS trafficInfo 전국 1회 호출 → {linkId: speed} (필터 없음 원본)."""
    key = os.getenv("ITS_OPENAPI_KEY", "").strip()
    if not key:
        raise RuntimeError("ITS_OPENAPI_KEY not set")
    r = requests.get(ITS_TRAFFIC_URL, params={
        "apiKey": key, "type": "all",
        "minX": "124.5", "maxX": "132.0", "minY": "33.0", "maxY": "39.0",
        "getType": "json",
    }, timeout=_ITS_TRAFFIC_TIMEOUT)
    r.raise_for_status()
    d = r.json()
    header = d.get("header") or {}
    if header.get("resultCode") not in (0, "0"):
        raise RuntimeError(f"ITS resultCode={header.get('resultCode')}")
    out = {}
    for it in ((d.get("body") or {}).get("items")) or []:
        lid = str(it.get("linkId") or "").strip()
        sp = _to_float(it.get("speed"))
        if lid and sp is not None and sp >= 0:
            out[lid] = sp
    return out


def _publish_local_speed_tiles(sftp):
    """전국 ITS 응답을 1° 타일 번들로 쪼개 local-speeds/{tile}.json 업로드.
    내용: {"updated_at": ts, "speeds": {lid: [speed, level]}}"""
    _, link_meta = _build_local_publish_index()
    if not link_meta:
        return {"tiles": 0, "links": 0}
    raw = _fetch_its_speeds_raw()
    now = time.time()
    tiles = {}
    for lid, sp in raw.items():
        meta = link_meta.get(lid)
        if not meta:
            continue
        tile, rank = meta
        level = _local_speed_level(sp, rank)
        if level is None:
            continue
        tiles.setdefault(tile, {})[lid] = [sp, level]
    total = 0
    for tile, speeds in tiles.items():
        _sftp_put_json(sftp, f"local-speeds/{tile}.json", {"updated_at": now, "speeds": speeds})
        total += len(speeds)
    return {"tiles": len(tiles), "links": total}


def _publish_fast(sftp):
    """10분 주기 파일들 업로드."""
    sizes = {}
    lt = get_link_traffic()
    sizes["link-traffic.json"] = _sftp_put_json(sftp, "link-traffic.json",
                                                {**lt, "published_at": time.time()})
    inc = get_active_incidents()
    sizes["incidents.json"] = _sftp_put_json(sftp, "incidents.json",
        {"ok": True, "count": len(inc), "items": inc, "updated_at": time.time()})
    sizes["traffic-summary-lite.json"] = _sftp_put_json(
        sftp, "traffic-summary-lite.json", _lite_traffic_summary())
    try:
        ct = get_city_times()
        sizes["city-times.json"] = _sftp_put_json(
            sftp, "city-times.json", {**ct, "published_at": time.time()})
    except Exception as e:
        print(f"[restarea publish] city-times 실패: {e}")
    try:
        sizes["local-speeds"] = _publish_local_speed_tiles(sftp)
    except Exception as e:
        print(f"[restarea publish] local-speeds 실패: {e}")
    try:
        alerts = get_vms_alerts()
        sizes["vms-alerts.json"] = _sftp_put_json(sftp, "vms-alerts.json",
            {"ok": True, "count": len(alerts), "items": alerts, "updated_at": time.time()})
    except Exception as e:
        print(f"[restarea publish] vms-alerts 실패: {e}")
    return sizes


def _publish_daily(sftp):
    """일 1회 파일들 업로드."""
    sizes = {}
    items = get_all_restareas()
    sizes["list.json"] = _sftp_put_json(sftp, "list.json",
        {"ok": True, "count": len(items), "items": items, "updated_at": time.time()})
    sizes["route-lines.json"] = _sftp_put_json(sftp, "route-lines.json", _route_lines_payload())
    return sizes


def _publish_cctv(sftp):
    """시간당 1회 — CCTV 목록을 ITS에서 force 재취득해 신선한 서명 URL로 발행."""
    cctv = _fetch_its_cctv(force=True)
    return {"cctv.json": _sftp_put_json(sftp, "cctv.json",
        {"ok": True, "count": len(cctv), "items": cctv, "updated_at": time.time()})}


def _publish_geometry(sftp):
    """1회성 — 일반도로 지오메트리 1° 타일 번들 업로드(local-geo/{tile}.json)."""
    by_tile, _ = _build_local_publish_index()
    sizes = {}
    for tile, links in by_tile.items():
        n = _sftp_put_json(sftp, f"local-geo/{tile}.json",
                           {"links": links, "updated_at": time.time()})
        sizes[tile] = n
    return sizes


def publish_restarea_all():
    """전체 발행 1회 실행(지오메트리 포함) — 최초 셋업/수동 재발행용."""
    if not _publish_enabled():
        return {"ok": False, "error": "RESTAREA_PUBLISH=0"}
    tr, sftp = _sftp_connect()
    try:
        _sftp_mkdirs(sftp, RESTAREA_REMOTE_DIR)
        out = {"daily": _publish_daily(sftp), "cctv": _publish_cctv(sftp),
               "fast": _publish_fast(sftp), "geometry": _publish_geometry(sftp)}
        now = time.time()
        _PUBLISH_STATE["fast_at"] = now
        _PUBLISH_STATE["daily_at"] = now
        _PUBLISH_STATE["cctv_at"] = now
        return {"ok": True, "result": out}
    finally:
        sftp.close()
        tr.close()


def _publish_loop():
    while True:
        try:
            if _publish_enabled():
                now = time.time()
                do_fast = (now - _PUBLISH_STATE["fast_at"]) >= _PUBLISH_FAST_SEC
                do_daily = (now - _PUBLISH_STATE["daily_at"]) >= _PUBLISH_DAILY_SEC
                do_cctv = (now - _PUBLISH_STATE["cctv_at"]) >= _PUBLISH_CCTV_SEC
                if do_fast or do_daily or do_cctv:
                    tr, sftp = _sftp_connect()
                    try:
                        _sftp_mkdirs(sftp, RESTAREA_REMOTE_DIR)
                        if do_daily:
                            _publish_daily(sftp)
                            _PUBLISH_STATE["daily_at"] = now
                        if do_cctv:
                            _publish_cctv(sftp)
                            _PUBLISH_STATE["cctv_at"] = now
                        if do_fast:
                            _publish_fast(sftp)
                            _PUBLISH_STATE["fast_at"] = now
                        print(f"[restarea publish] ok fast={do_fast} daily={do_daily} cctv={do_cctv}")
                    finally:
                        sftp.close()
                        tr.close()
        except Exception as e:
            # 연결 실패 등은 조용히 스킵 — 다음 주기에 재시도
            print(f"[restarea publish] 실패(다음 주기 재시도): {e}")
        time.sleep(60)


def _start_publish_thread():
    with _PUBLISH_LOCK:
        if _PUBLISH_STATE["thread"]:
            return
        _PUBLISH_STATE["thread"] = True
    threading.Thread(target=_publish_loop, daemon=True).start()


# ── Flask 라우트 등록 (master_server.py에서 register(app) 호출) ─────────────
def register(app):
    from flask import jsonify, request

    @app.route("/api/restarea/list")
    def _restarea_list():
        route = request.args.get("route")
        direction = request.args.get("direction")
        try:
            data = get_all_restareas(route=route, direction=direction)
            return jsonify({"ok": True, "count": len(data), "items": data})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/<std_rest_cd>")
    def _restarea_detail(std_rest_cd):
        try:
            d = get_restarea_detail(std_rest_cd)
            if not d:
                return jsonify({"ok": False, "error": "not found"}), 404
            return jsonify({"ok": True, "item": d})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/nearby")
    def _restarea_nearby():
        try:
            lat = request.args.get("lat")
            lng = request.args.get("lng")
            limit = int(request.args.get("limit", 20))
            if lat is None or lng is None:
                return jsonify({"ok": False, "error": "lat/lng required"}), 400
            data = get_nearby(lat, lng, limit=limit)
            return jsonify({"ok": True, "count": len(data), "items": data})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/refresh")
    def _restarea_refresh():
        try:
            result = refresh_all(force=True)
            _MERGED_CACHE["data"] = None  # merge 캐시 무효화
            return jsonify({"ok": True, "result": result})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/traffic-summary")
    def _restarea_traffic():
        try:
            return jsonify({"ok": True, "summary": get_traffic_summary()})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/route-lines")
    def _restarea_route_lines():
        try:
            lines = get_route_lines()
            return jsonify({"ok": True, "count": len(lines), "routes": list(lines.values())})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/incidents")
    def _restarea_incidents():
        try:
            data = get_active_incidents()
            return jsonify({"ok": True, "count": len(data), "items": data})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/repair-road-loops")
    def _restarea_repair_road_loops():
        try:
            result = repair_road_geometry_loops()
            return jsonify({"ok": True, "result": result})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/build-official-road-geometry")
    def _restarea_build_official_road_geometry():
        try:
            summary = build_official_road_geometry()
            return jsonify({"ok": True, "summary": summary})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/build-nodelink-road-geometry")
    def _restarea_build_nodelink_road_geometry():
        try:
            summary = build_nodelink_road_geometry()
            return jsonify({"ok": True, "summary": summary})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/build-nodelink-name-coords")
    def _restarea_build_nodelink_name_coords():
        try:
            summary = build_nodelink_name_coords()
            return jsonify({"ok": True, "summary": summary})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/route-traffic")
    def _restarea_route_traffic():
        try:
            route = request.args.get("route")
            if not route:
                return jsonify({"ok": False, "error": "route required"}), 400
            segments = get_route_traffic(route)
            return jsonify({"ok": True, "route": route, "count": len(segments), "segments": segments})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/cctv")
    def _restarea_cctv():
        try:
            items = _fetch_its_cctv()
            return jsonify({"ok": True, "count": len(items), "items": items})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/cctv-fresh")
    def _restarea_cctv_fresh():
        try:
            lat = float(request.args.get("lat", ""))
            lng = float(request.args.get("lng", ""))
        except ValueError:
            return jsonify({"ok": False, "error": "lat/lng required"}), 400
        if not (33.0 <= lat <= 39.5 and 124.0 <= lng <= 132.0):
            return jsonify({"ok": False, "error": "out of range"}), 400
        try:
            data = get_cctv_fresh(lat, lng)
            if not data:
                return jsonify({"ok": False, "error": "no cctv nearby"})
            return jsonify({"ok": True, "cctv": data})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/city-times")
    def _restarea_city_times():
        try:
            return jsonify(get_city_times(request.args.get("origin")))
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/link-traffic")
    def _restarea_link_traffic():
        try:
            return jsonify(get_link_traffic())
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/build-link-geometry")
    def _restarea_build_link_geometry():
        try:
            summary = build_link_geometry()
            return jsonify({"ok": True, "summary": summary})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/local-traffic")
    def _restarea_local_traffic():
        try:
            return jsonify(get_local_traffic(
                request.args.get("minX"), request.args.get("maxX"),
                request.args.get("minY"), request.args.get("maxY")))
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/build-local-link-geometry")
    def _restarea_build_local_link_geometry():
        try:
            summary = build_local_link_geometry()
            return jsonify({"ok": True, "summary": summary})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/publish-all")
    def _restarea_publish_all():
        try:
            return jsonify(publish_restarea_all())
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    # refininfo.com 정적 데이터 발행 백그라운드 스레드(10분/일1회 주기)
    _start_publish_thread()

    @app.route("/api/restarea/vms")
    def _restarea_vms():
        try:
            route = request.args.get("route")
            if not route:
                return jsonify({"ok": False, "error": "route required"}), 400
            items = get_vms_messages(route)
            return jsonify({"ok": True, "route": route, "count": len(items), "items": items})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/restarea/vms-alerts")
    def _restarea_vms_alerts():
        try:
            items = get_vms_alerts()
            return jsonify({"ok": True, "count": len(items), "items": items,
                            "updated_at": _VMS_ALERTS_CACHE["at"]})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500
