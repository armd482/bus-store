#!/usr/bin/env python3
"""전국횡단보도표준데이터(15028201) 확인 — 판교 커버리지 + 신호시간(cycle/green) 채움률.

✅ 실측 확인 (2026-07, data.go.kr 원문):
  - 스키마에 **`녹색신호시간`·`적색신호시간` 컬럼이 있다** → cycle = 녹색+적색 계산 가능.
  - 그러나 **지자체 자율 업로드**다 (현재 55개 기관만 참여, 갱신 반기, 소관 국토부·경찰청).
  - **성남시는 미참여** → "성남시 횡단보도" 표준데이터셋 검색 0건 → **판교가 15028201에 없다.**
  - 참여 지역도 샘플(무주군)상 녹색/적색시간이 대부분 공란 — 컬럼은 있어도 채움률이 낮다.
  → 결론: 구조적으론 담을 수 있으나 실제론 (a) 판교 미참여 (b) 타이밍 대부분 미기재라
    **판교엔 못 쓴다.** 참여 지자체의 CSV 를 받아 이 스크립트로 채움률을 확인해 판단할 것.

핵심 질문 두 개를 답한다:
  1. 이 데이터에 **신호 타이밍 컬럼**(녹색·적색·주기·현시·보행시간)이 실제로 있나? (→ ✅ 있다)
  2. 그 지역이 레코드에 있고, 그 타이밍이 채워져 있나? (→ 지자체마다 다름)

사용:
  # (권장) data.go.kr 15028201 파일데이터에서 CSV 다운로드 후:
  python3 check_crosswalk_std.py --csv 전국횡단보도표준데이터.csv

  # odcloud API 로 (UDDI 필요 · DATA_GO_KR_KEY 또는 SIGNAL_API_KEY 승인 시):
  python3 check_crosswalk_std.py --api "https://api.odcloud.kr/api/15028201/v1/uddi:...."

판교 bbox: 경도 127.06~127.13 · 위도 37.38~37.43 (docs §7.1 실측 클리핑과 동일)
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
ENV_FILES = (os.path.join(HERE, ".env"), os.path.join(PROJECT_ROOT, ".env"))

PANGYO_BBOX = (127.06, 127.13, 37.38, 37.43)   # lon_min, lon_max, lat_min, lat_max
SEONGNAM_TEXT = ("성남", "분당", "판교", "43113")   # 소재지/시군구 문자열 힌트

# 신호 '타이밍'을 뜻할 수 있는 컬럼명 키워드 — 이게 있어야 cycle/green 을 얻는다.
TIMING_HINTS = ("녹색", "적색", "주기", "현시", "보행시간", "신호시간", "점멸", "green", "cycle")
# 신호 '유무/위치'만 뜻하는 컬럼 — 있어도 타이밍은 아니다.
PRESENCE_HINTS = ("신호등유무", "보행자신호", "신호등", "연동")


def _dotenv(path, name):
    try:
        for raw in open(path, encoding="utf-8"):
            line = raw.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k.strip() == name:
                    return v.strip().strip('"').strip("'") or None
    except OSError:
        return None
    return None


def load_key():
    for name in ("DATA_GO_KR_KEY", "SIGNAL_API_KEY", "GBIS_BUS_KEY"):
        v = os.environ.get(name)
        for p in ENV_FILES:
            v = v or _dotenv(p, name)
        if v:
            return v, name
    return None, None


def read_csv_rows(path):
    """cp949/utf-8 자동 판별해 dict 행을 만든다 (표준데이터는 대개 cp949)."""
    raw = open(path, "rb").read()
    for enc in ("utf-8-sig", "cp949", "utf-8"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        sys.exit("인코딩 판별 실패 — CSV 가 아닌 것 같다.")
    return list(csv.DictReader(io.StringIO(text)))


def fetch_api_rows(uddi, key, max_rows=100000):
    """odcloud 표준데이터 API — 페이지네이션으로 전량 수집."""
    rows, page = [], 1
    while len(rows) < max_rows:
        q = urllib.parse.urlencode({"serviceKey": key, "page": page,
                                    "perPage": 1000, "returnType": "JSON"})
        req = urllib.request.Request(f"{uddi}?{q}")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")[:200]
            sys.exit(f"API HTTP {e.code}: {body}\n"
                     f"→ 이 키가 15028201 에 미승인이거나(403) UDDI 가 틀렸을 수 있다.")
        batch = d.get("data") or []
        rows += batch
        if len(batch) < 1000:
            break
        page += 1
    return rows


def analyze(rows):
    if not rows:
        sys.exit("레코드가 0건이다.")
    cols = list(rows[0].keys())
    print(f"총 레코드: {len(rows):,}건 · 컬럼 {len(cols)}개")
    print(f"컬럼: {', '.join(cols)}\n")

    timing_cols = [c for c in cols if any(h in c.lower() for h in
                   (h.lower() for h in TIMING_HINTS))]
    presence_cols = [c for c in cols if any(h in c for h in PRESENCE_HINTS)
                     and c not in timing_cols]

    print("── 신호 타이밍 컬럼(cycle/green) 존재 여부 ─────────────")
    if timing_cols:
        print(f"  ✅ 타이밍 후보 컬럼: {timing_cols}")
    else:
        print("  ❌ 녹색/적색/주기/현시 등 타이밍 컬럼이 없다 "
              "— 이 데이터로는 cycle/green 을 못 얻는다.")
    if presence_cols:
        print(f"  (참고) 신호등 유무/위치 컬럼: {presence_cols}")

    # 위경도 컬럼 찾기
    lat_c = next((c for c in cols if c in ("위도", "lat", "LAT", "y", "Y")), None)
    lon_c = next((c for c in cols if c in ("경도", "lon", "LON", "x", "X")), None)

    def in_pangyo(row):
        if lat_c and lon_c:
            try:
                lon, lat = float(row[lon_c]), float(row[lat_c])
                return (PANGYO_BBOX[0] <= lon <= PANGYO_BBOX[1]
                        and PANGYO_BBOX[2] <= lat <= PANGYO_BBOX[3])
            except (TypeError, ValueError):
                pass
        blob = " ".join(str(v) for v in row.values())
        return any(t in blob for t in SEONGNAM_TEXT)

    pangyo = [r for r in rows if in_pangyo(r)]
    print(f"\n── 판교/성남 커버리지 ──────────────────────────────")
    print(f"  판교 bbox/성남 레코드: {len(pangyo):,}건 "
          f"({'좌표' if lat_c and lon_c else '주소문자열'} 기준)")

    print(f"\n── 타이밍 채움률 (전국 vs 판교) ────────────────────")
    if not timing_cols:
        print("  타이밍 컬럼이 없어 채움률 계산 불가.")
    else:
        for c in timing_cols:
            def filled(rs):
                return sum(1 for r in rs if str(r.get(c, "")).strip()
                           not in ("", "0", "null", "None"))
            allf = filled(rows)
            pf = filled(pangyo)
            print(f"  {c:<16} 전국 {allf:,}/{len(rows):,} ({allf/len(rows)*100:.1f}%) · "
                  f"판교 {pf}/{len(pangyo)}")

    print("\n── 판정 ────────────────────────────────────────────")
    if timing_cols and pangyo and any(
            str(r.get(timing_cols[0], "")).strip() not in ("", "0") for r in pangyo):
        print("  🟢 15028201 에 판교 + 신호시간이 있다 → 실측 없이 crosswalks.json 매핑 가능.")
    elif not timing_cols:
        print("  🔴 신호 타이밍 컬럼 자체가 없다 → 15028201 로는 cycle/green 못 얻음.")
        print("     이 데이터는 '신호등 유무·위치'용. 타이밍은 경찰청 TOD 또는 현장 실측이 답.")
    elif not pangyo:
        print("  🟡 타이밍 컬럼은 있으나 판교 레코드를 못 찾음 → 좌표/주소 필터 확인 필요.")
    else:
        print("  🟡 판교는 있으나 타이밍이 비어 있음(일부만 채움) → 판교는 실측 필요.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", help="다운로드한 15028201 CSV 경로")
    ap.add_argument("--api", help="odcloud UDDI URL (https://api.odcloud.kr/api/15028201/v1/uddi:...)")
    args = ap.parse_args()
    if args.csv:
        analyze(read_csv_rows(args.csv))
    elif args.api:
        key, name = load_key()
        if not key:
            sys.exit("키 없음 — DATA_GO_KR_KEY/SIGNAL_API_KEY 를 .env 에 넣을 것.")
        print(f"API 조회 (키: {name})\n")
        analyze(fetch_api_rows(args.api, key))
    else:
        sys.exit("--csv 파일 또는 --api UDDI 중 하나가 필요하다.\n"
                 "CSV: data.go.kr 15028201 '파일데이터'에서 다운로드가 가장 확실하다.")


if __name__ == "__main__":
    main()
