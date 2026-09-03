# -*- coding: utf-8 -*-
"""GitHub Actions용 휴게소 조회기 데이터 발행 러너.

노트북 5001 발행의 백업 — 15분 크론으로 돌며:
  1) 라이브의 link-traffic.json이 10분 이내로 신선하면(=노트북이 발행 중) 즉시 종료
  2) 아니면 ITS 속도를 동기 취득 후 _publish_fast(교통·돌발·도시시간·VMS·일반도로) 발행
  3) cctv.json이 55분, list.json이 23시간 넘게 오래됐으면 각각 재발행

필요 환경변수(Secrets): EX_OPENAPI_KEY, ITS_OPENAPI_KEY,
  FTP_HOST, FTP_PORT, FTP_USER_REFININFO, FTP_PASS_REFININFO
"""
import json
import sys
import time
import urllib.request

import restarea_helpers as rh

LIVE = "https://refininfo.com/restarea-data/"
FRESH_SKIP_SEC = 10 * 60  # 노트북 발행이 이보다 신선하면 이번 회차 스킵


def remote_age(name):
    """라이브 발행 파일의 (초 단위) 나이. 실패 시 아주 큰 값."""
    try:
        req = urllib.request.Request(
            f"{LIVE}{name}?t={int(time.time())}",
            headers={"User-Agent": "restarea-publisher-ci"})
        with urllib.request.urlopen(req, timeout=20) as r:
            j = json.load(r)
        ts = j.get("published_at") or j.get("updated_at") or 0
        return time.time() - float(ts)
    except Exception as e:
        print(f"[remote_age] {name}: {e}")
        return 10 ** 9


def prime_vms_from_remote():
    """라이브 vms-alerts.json이 40분 이내면 그 값을 메모리 캐시에 심어
    이번 회차의 VMS 전량 스윕(도로공사 API 약 770호출)을 건너뛴다."""
    try:
        req = urllib.request.Request(
            f"{LIVE}vms-alerts.json?t={int(time.time())}",
            headers={"User-Agent": "restarea-publisher-ci"})
        with urllib.request.urlopen(req, timeout=20) as r:
            j = json.load(r)
        ts = float(j.get("updated_at") or 0)
        if time.time() - ts < 40 * 60 and isinstance(j.get("items"), list):
            rh._VMS_ALERTS_CACHE["data"] = j["items"]
            rh._VMS_ALERTS_CACHE["at"] = time.time()
            print(f"VMS: 라이브 값 재사용({len(j['items'])}건, 스윕 생략)")
    except Exception as e:
        print(f"[warn] VMS prime 실패(정상 스윕 진행): {e}")


def main():
    import os
    age = remote_age("link-traffic.json")
    print(f"link-traffic.json age = {age/60:.1f}min")
    if age < FRESH_SKIP_SEC and os.getenv("RESTAREA_FORCE") != "1":
        print("노트북 발행이 신선함 — 이번 회차 스킵 (백업 대기)")
        return 0
    prime_vms_from_remote()

    # ITS 링크 속도 동기 취득 (서버에선 백그라운드 스레드지만 CI는 즉시 필요)
    try:
        data = rh._fetch_its_link_speeds()
        if data:
            rh._save_cache("its_link_speeds", data)
            rh._ITS_SPEED_CACHE["data"] = data
            rh._ITS_SPEED_CACHE["at"] = time.time()
            print(f"ITS 링크 속도 {len(data)}건 취득")
    except Exception as e:
        print(f"[warn] ITS 속도 취득 실패(캐시 폴백): {e}")

    tr, sftp = rh._sftp_connect()
    try:
        rh._sftp_mkdirs(sftp, rh.RESTAREA_REMOTE_DIR)
        sizes = rh._publish_fast(sftp)
        print("fast 발행:", {k: v for k, v in (sizes or {}).items()})
        if remote_age("cctv.json") > 55 * 60:
            print("cctv 재발행:", rh._publish_cctv(sftp))
        if remote_age("list.json") > 23 * 3600:
            print("daily 재발행:", rh._publish_daily(sftp))
    finally:
        sftp.close()
        tr.close()
    print("발행 완료")
    return 0


if __name__ == "__main__":
    sys.exit(main())
