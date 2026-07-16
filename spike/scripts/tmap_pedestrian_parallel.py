#!/usr/bin/env python3
"""TMAP 보행자 API 병렬 호출 실측 — 동시 N개를 버스트로 던졌을 때 성공하는가.

한 검색의 도보 leg 재조회를 병렬로 해도 되는지 확인하는 스크립트 (docs §3.6).
✅ 실측 (2026-07-16): 동시 20 버스트까지 전량 200. 429·차단 없음.
   순차 188ms → 동시 10 은 243ms(벽시계 0.36s), 동시 20 부터 개별 지연 3배(747ms).
   → 검색당 재조회는 동시 10 이 스윗스팟. TAGO 와 달리 버스트에 민감하지 않다.
⚠️ 이 테스트는 "한 검색의 버스트"까지만 증명한다. 다수 사용자의 지속 TPS 는 별개.

사용: TMAP_APP_KEY 환경변수 또는 bus-test/.env.local 필요. 총 38콜 (쿼터 1,000/일).
"""
import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
KEY_FILE = os.path.join(HERE, "..", "..", "..", "bus-test", ".env.local")
URL = "https://apis.openapi.sk.com/tmap/routes/pedestrian?version=1&format=json"


def load_key():
    key = os.environ.get("TMAP_APP_KEY")
    if key:
        return key
    try:
        for line in open(KEY_FILE, encoding="utf-8"):
            if line.strip().startswith("TMAP_APP_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    except OSError:
        pass
    raise SystemExit("TMAP_APP_KEY 없음")


KEY = load_key()

# 강남·판교 일대 도보 거리(300m~1km) 좌표쌍 — i 오프셋으로 전부 다른 요청을 만든다
# (같은 좌표를 반복하면 서버 캐시가 결과를 왜곡할 수 있다)
BASES = [
    (127.0276, 37.4979, 127.0252, 37.5045),  # 강남역 → 신논현 방면
    (127.1115, 37.3947, 127.1058, 37.3897),  # 판교역 → 백현동 방면
    (127.0088, 37.4855, 127.0032, 37.4813),  # 사당 방면
]


def call(i):
    sx, sy, ex, ey = BASES[i % len(BASES)]
    j = (i // len(BASES)) * 0.0007  # ~78m 씩 이동 — 요청마다 다른 경로
    body = urllib.parse.urlencode({
        "startX": f"{sx + j:.6f}", "startY": f"{sy + j:.6f}",
        "endX": f"{ex + j:.6f}", "endY": f"{ey + j:.6f}",
        "startName": urllib.parse.quote("A"), "endName": urllib.parse.quote("B"),
        "reqCoordType": "WGS84GEO", "resCoordType": "WGS84GEO",
    }).encode()
    req = urllib.request.Request(URL, data=body, headers={"appKey": KEY})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            json.loads(r.read().decode())
            return r.status, time.time() - t0
    except urllib.error.HTTPError as e:
        return e.code, time.time() - t0
    except Exception as e:
        return type(e).__name__, time.time() - t0


def tier(name, n, workers):
    with ThreadPoolExecutor(max_workers=workers) as ex:
        t0 = time.time()
        rs = list(ex.map(call, range(n)))
        wall = time.time() - t0
    ok = sum(1 for c, _ in rs if c == 200)
    lat = [t for _, t in rs]
    codes = {}
    for c, _ in rs:
        codes[c] = codes.get(c, 0) + 1
    print(f"{name:<14} 성공 {ok}/{n} · 상태 {codes} · "
          f"지연 중앙값 {statistics.median(lat)*1000:.0f}ms / 최대 {max(lat)*1000:.0f}ms · "
          f"벽시계 {wall:.2f}s", flush=True)


if __name__ == "__main__":
    tier("순차 3 (기준)", 3, 1)
    time.sleep(3)
    tier("동시 5 버스트", 5, 5)
    time.sleep(3)
    tier("동시 10 버스트", 10, 10)
    time.sleep(3)
    tier("동시 20 버스트", 20, 20)
    print("총 38콜 사용 (보행자 쿼터 1,000/일)")
