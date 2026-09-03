# restarea-publisher

refininfo.com 고속도로 휴게소·실시간 교통 조회기(https://refininfo.com/rest-stop-finder.html)의
공개 데이터 발행 백업 러너입니다.

- 15분마다 공공데이터(한국도로공사·국가교통정보센터 오픈API)를 수집해
  정적 JSON으로 발행합니다.
- 주 발행기(로컬 서버)가 살아 있으면 아무것도 하지 않고 종료합니다(이중화 백업).
- 인증 정보는 전부 GitHub Secrets — 이 리포에는 키가 없습니다.
- `restarea_cache/`의 데이터는 국토교통부 표준노드링크 등 공공데이터에서
  파생된 지오메트리 캐시입니다.
