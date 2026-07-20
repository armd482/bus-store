#!/usr/bin/env python3
"""
수집 조율 — 커버리지 추적 + 노선 선택 + 진행률 (docs §4.4)

문제: 지금까지는 노선 목록이 고정이라 이미 충분히 모은 구간을 계속 재폴링했다.
      그 콜로 아직 한 번도 안 본 노선을 봐야 한다.

해법: (노선, 구간, 시간대, 요일) 셀별 샘플 수를 세고, 셀이 목표를 채운 노선은
      폴링 목록에서 빼고 미커버 노선을 넣는다. 그러면 총 소요가 달력이 아니라
      커버리지에 묶인다.

⚠️ 커버리지 산수 (✅ 성남 실측 기반):
     구간당 관측 51.5회/일  ÷ 시간대 수 = 셀당/일
     시간대 7개  → 7.4회/일 → 목표 10샘플에 1.4일
     시간대 19개 → 2.7회/일 → 목표 10샘플에 3.7일
   → timebands 가 수집 기간을 지배한다. config.json 참조.

⚠️ 요일 7종 전부 분리(2026-07-17) — 월~일 모두 주 1일씩만 채워지는 동등한
   처지다. 진행률·완주 판정도 전부 7요일 기준이다 (5요일만 세면 노선이
   주말을 남긴 채 '완주'로 빠질 수 있다).

사용:
  python3 orchestrator.py status           진행률
  python3 orchestrator.py routes           다음 사이클에 폴링할 노선
  python3 orchestrator.py holidays         장부에서 빠지는 공휴일 (--refresh 로 재조회)
  python3 orchestrator.py reset --yes      관측 카운트 전체 초기화 (아래 주의)
  python3 orchestrator.py rebuild --yes    장부를 jsonl 데이터에서 재계산

⚠️ 데이터(jsonl)와 장부(cell)는 따로다 — 아무도 jsonl 을 다시 읽지 않으므로
   파일이나 행을 지워도 장부는 모른다. 장부만 남으면 "이미 채웠다"고 믿어
   그 구간을 다시 안 찍는다: 영구 구멍. 데이터를 지웠으면:
     전부 지웠다  → reset --yes  (장부도 0으로)
     일부만 지웠다 → rebuild --yes (남은 jsonl 로 장부 재계산 — 밴드·요일은 t 에서
                     현 config 규칙으로 재분류된다)
   반대로 장부만 지우면 재수집할 뿐이라 안전하다 (중복 데이터, 쿼터 낭비).
"""

import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone

import holidays as H   # 공휴일 자동 조회 (대체·임시공휴일 포함) — 목록을 박지 않는다

# ⚠️ 윈도우 방어. 모든 스크립트가 이 모듈을 import 하므로 여기서 한 번만 한다.
#    윈도우의 기본 stdout 인코딩은 cp949 라 로그의 한글·이모지에서 UnicodeEncodeError 로 죽는다.
#    ✅ 재현: PYTHONIOENCODING=cp949 → "⚠️ 실패" 출력에서 크래시.
#    하필 그 줄은 API 실패 시에만 타는 경로다 → 잘 돌다가 첫 실패에 수집기가 통째로 죽는다.
#    errors="replace" 까지 두는 건, 로그 한 글자 때문에 수집이 멈추는 일은 없어야 하기 때문.
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass  # 파이프로 감싸였거나 3.7 미만

KST = timezone(timedelta(hours=9))
HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
DB = os.path.join(DATA, "coverage.sqlite")
CONFIG = os.path.join(HERE, "config.json")


_CFG = {"at": None, "v": None}


def cfg():
    """config.json — mtime 이 그대로면 파싱을 재사용한다.

    ⚠️ 매번 파싱하던 것이 조용한 낭비였다: is_holiday 는 관측 행마다 불리므로
    rebuild 가 수백만 번 config.json 을 다시 읽었다. 핫 리로드(수집 중 설정을
    고치면 다음 사이클에 반영)는 mtime 비교로 그대로 유지된다.
    """
    try:
        m = os.path.getmtime(CONFIG)
    except OSError:
        m = None
    if _CFG["v"] is None or m != _CFG["at"]:
        _CFG["v"] = json.load(open(CONFIG, encoding="utf-8"))
        _CFG["at"] = m
    return _CFG["v"]


def service_day_of(t):
    """운행일. 04시 기준으로 하루를 가른다 — 01:30 에 잡힌 버스는 전날 운행분이다."""
    return t - timedelta(hours=4)


def day_type(t):
    """⚠️ 벽시계가 아니라 운행일 기준이다.

    토요일 01:30 에 도는 버스는 금요일 막차다. t.weekday() 를 그대로 쓰면
    금요일 막차가 '토요일' 표본으로 잘못 들어간다.

    ⚠️ 요일 7종 전부 분리 (2026-07-17 결정). 비용을 알고 용인했다:
    각 요일은 주 1일씩만 얻으므로 목표 7샘플 = 요일당 7주
    (평일 통합이던 이전 판은 1.4주). 요일별 주행시간 차이를 직접 보기 위함.
    규칙을 되돌리려면 day_type 을 고치고 rebuild — 요일은 t 에서 재계산된다.
    """
    return ("mon", "tue", "wed", "thu", "fri", "sat", "sun")[service_day_of(t).weekday()]


def holiday_set(offline=False):
    """지금 적용할 공휴일 집합 — **자동 조회분 ∪ config 수동 목록**.

    offline=True 면 네트워크를 건드리지 않고 캐시만 본다 — 대시보드(/api)처럼
    5초마다 불리는 경로용. 갱신은 수집 루프가 맡는다.

    자동: holidays.py 가 특일정보(공식) → 구글 iCal 순으로 받아 캐시한다.
    대체공휴일·임시공휴일이 포함되고, 목록을 코드나 config 에 박지 않는다
    (박으면 15주 수집 중에 반드시 어긋난다 — 임시공휴일은 갑자기 지정된다).
    수동: config "holidays" 는 **덮어쓰기**다. 피드 반영이 늦은 임시공휴일을
    즉시 얹는 탈출구이고, 비어 있는 게 정상이다.

    네트워크가 죽어도 캐시로 돌고 캐시도 없으면 빈 집합이다 — 공휴일을 못
    받았다고 수집을 멈추지 않는다 (그 손실이 오염보다 크다. 나중에 rebuild).
    """
    try:
        auto = H.cached() if offline else H.load()
    except Exception as e:                       # 어떤 이유로도 수집을 멈추지 않는다
        print(f"[공휴일] 조회 예외 — 수동 목록만 사용: {type(e).__name__} {e}", flush=True)
        auto = set()
    return auto | set(cfg().get("holidays") or [])


def is_holiday(t):
    """공휴일(운행일 기준)인가.

    평일에 낀 공휴일은 그 요일의 평상 운행이 아니다(대개 휴일 다이어) —
    수요일 추석을 'wed' 표본으로 넣으면 요일별 주행시간이 오염된다.
    장부(cell/subway_cell)에서만 제외하고 jsonl 엔 남긴다 — 목록이 틀렸어도
    고치고 rebuild 하면 재분류된다.

    ⚠️ 루프에서는 holiday_set() 을 한 번 받아 쓸 것 — 행마다 부르면 집합을
    매번 다시 만든다 (rebuild 가 그렇게 느려졌었다).
    """
    return service_day_of(t).strftime("%Y-%m-%d") in holiday_set()


def band_of(t, bands):
    """시각 → 밴드 인덱스. 어느 밴드에도 없으면 None(커버리지에 안 넣음).

    ⚠️ 24를 넘는 끝값은 익일이다: [20, 28] = 20:00~04:00(운행일 경계).
    이걸 처리 안 하면 00:30 관측이 어느 밴드에도 안 걸려 영원히 안 채워진다.
    """
    h = t.hour
    for i, (a, b) in enumerate(bands):
        if b <= 24:
            if a <= h < b:
                return i
        elif h >= a or h < b - 24:   # 자정 넘김
            return i
    return None


def in_window(t, window):
    """운행 창 안인가. window 도 24 초과 = 익일. [4, 27] = 04:00~03:00."""
    a, b = window
    h = t.hour
    return (a <= h < b) if b <= 24 else (h >= a or h < b - 24)


def connect(check_same_thread=True):
    """check_same_thread=False 는 여러 스레드가 한 연결을 공유할 때만 —
    호출부가 락으로 직렬화할 책임을 진다 (server.py 의 읽기 전용 연결)."""
    os.makedirs(DATA, exist_ok=True)
    c = sqlite3.connect(DB, timeout=30, check_same_thread=check_same_thread)
    c.execute("PRAGMA journal_mode=WAL")  # 수집기가 쓰는 중에도 status 가 읽히도록
    c.execute("""
      CREATE TABLE IF NOT EXISTS cell (
        routeid  TEXT NOT NULL,
        from_ord INTEGER NOT NULL,
        to_ord   INTEGER NOT NULL,
        band     INTEGER NOT NULL,
        daytype  TEXT NOT NULL,
        n        INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (routeid, from_ord, to_ord, band, daytype)
      )""")
    c.execute("CREATE INDEX IF NOT EXISTS cell_route ON cell(routeid)")
    # 대시보드가 5초마다 요일·밴드 필터 쿼리를 친다 — 셀이 수백만이 되면
    # daytype 풀스캔이 /api 를 초 단위로 늘린다.
    c.execute("CREATE INDEX IF NOT EXISTS cell_day ON cell(daytype)")
    # 지하철 셀 (B안) — (노선, 열차, 역, 요일)별 관측 일수. trainNo 가 매일 반복이라
    # 요일별 며칠이면 각 열차의 정시성 분포가 나온다 (§8 #1). n = 관측한 서로 다른 날 수.
    c.execute("""
      CREATE TABLE IF NOT EXISTS subway_cell (
        line     TEXT NOT NULL,
        trainNo  TEXT NOT NULL,
        statnId  TEXT NOT NULL,
        daytype  TEXT NOT NULL,
        n        INTEGER NOT NULL DEFAULT 0,
        last_day TEXT,
        PRIMARY KEY (line, trainNo, statnId, daytype)
      )""")
    c.execute("CREATE INDEX IF NOT EXISTS subway_cell_line ON subway_cell(line)")
    # 기존 DB 마이그레이션 — last_day 는 "이 셀을 마지막으로 센 운행일"이고,
    # 하루 1회 제약을 메모리가 아니라 **디스크**에 두기 위한 것이다 (bump_subway 참조).
    if "last_day" not in {r[1] for r in c.execute("PRAGMA table_info(subway_cell)")}:
        c.execute("ALTER TABLE subway_cell ADD COLUMN last_day TEXT")
    # 노선 풀 — fetch_routes.py 가 채운다
    c.execute("""
      CREATE TABLE IF NOT EXISTS route (
        routeid     TEXT PRIMARY KEY,
        cityCode    INTEGER NOT NULL,
        routeno     TEXT,
        routetp     TEXT,
        nstops      INTEGER DEFAULT 0,
        startvt     TEXT,               -- 첫차 'HHMM' — pick_routes 의 운행시간 필터(§2.6)
        endvt       TEXT,               -- 막차 출발 'HHMM'. ⚠️ 도착이 아니다
        emptyStreak INTEGER DEFAULT 0,  -- 연속 0대 반환 횟수 — 관측이 정하는 후순위
        lastSeen    REAL                -- 마지막으로 버스가 보인 시각
      )""")

    # 기존 DB 마이그레이션 — CREATE TABLE IF NOT EXISTS 는 이미 있는 테이블에 컬럼을 안 붙인다.
    # ⚠️ 이걸 빠뜨려 배포본에만 손으로 ALTER 했다가, 새 기계에서 'no such column: startvt' 로 죽었다.
    have = {r[1] for r in c.execute("PRAGMA table_info(route)")}
    for col, decl in (("startvt", "TEXT"), ("endvt", "TEXT"),
                      ("emptyStreak", "INTEGER DEFAULT 0"), ("lastSeen", "REAL")):
        if col not in have:
            c.execute(f"ALTER TABLE route ADD COLUMN {col} {decl}")
    c.commit()
    return c


def _rclone(args):
    """rclone 호출 → (성공?, stdout). rclone 이 없거나 실패하면 (False, "")."""
    import subprocess
    try:
        r = subprocess.run(["rclone"] + args, capture_output=True, text=True, timeout=180)
        return r.returncode == 0, r.stdout
    except Exception as e:
        print(f"[rclone] {' '.join(args)} 실패: {e}", flush=True)
        return False, ""


def _backup_verified(name, local_gz, remote):
    """이 .gz 가 안전하게 백업됐나 — 원본 삭제의 전제.

    remote(예 'gdrive:busdata') 설정 시: 드라이브에 있는지 확인 → 없으면 재업로드
    시도 → 재확인. 그래도 없으면 False (원본을 안 지운다).
    remote 미설정 시(윈도우 OneDrive 등): 로컬 .gz 존재로만 판단 — 동기화앱/크론이
    올린다고 신뢰한다.
    """
    if not remote:
        return os.path.exists(local_gz)
    ok, out = _rclone(["lsf", remote, "--include", name])
    if ok and name in out:
        return True
    _rclone(["copy", local_gz, remote])            # 없으면 재업로드 시도
    ok, out = _rclone(["lsf", remote, "--include", name])
    return ok and name in out


def rotate_due(rotated_day, t=None):
    """지금 로테이션을 돌릴 때인가 — 돌려야 하면 그 운행일 문자열, 아니면 None.

    ⚠️ 운행일 경계(04시)가 아니라 config.rotateHour(기본 6시)에 돌린다:
      - 경계를 걸친 사이클이 아직 전날 파일에 쓰고 있을 수 있다 (day 변수는 사이클
        시작에 정해지므로 04:00:20 에 끝나는 사이클도 전날 파일로 간다)
      - 04시는 버스 첫차·운행일 전환·심야 정리가 겹치는 시각이라 gzip·업로드를
        얹으면 그 사이클만 길어진다
      두 시간 여유를 두면 전날 파일이 확실히 닫힌 뒤 백업된다.

    하루 한 번만 참이 되도록 호출부가 rotated_day 를 갱신한다.
    """
    t = t or datetime.now(KST)
    today = service_day_of(t).strftime("%Y-%m-%d")
    if rotated_day == today or t.hour < cfg().get("rotateHour", 6):
        return None
    return today


def rotate_jsonl(prefix):
    """2단 로테이션 — 백업(어제)과 삭제(그저께 이전)를 분리한다. 버스·지하철 공유.

      오늘        : 손대지 않는다 (수집 중)
      어제        : gzip 백업본을 exportDir 로(+remote 설정 시 드라이브 업로드).
                    원본 jsonl 은 로컬에 유지 — 대시보드 ETA 가 어제 파일 크기를 쓴다
      그저께 이전  : 백업이 **드라이브에 확인되면** 원본 jsonl 삭제. 확인 안 되면
                    재업로드 시도, 그래도 실패하면 그대로 둔다 (데이터를 잃는 경로 없음).
                    → 삭제 전 하루의 유예가 생겨 수동 백업 확인이 가능하다.

    exportDir 미설정이면 no-op. .gz 는 rebuild 가 읽으므로 exportDir 에 계속 남긴다
    (rebuild 는 bus-*.gz 만 읽어 shinbundang-*.gz 와 무관).
    """
    k = cfg()
    export_dir = k.get("exportDir")
    if not export_dir:
        return
    remote = k.get("driveRemote")   # 예 'gdrive:busdata'. null 이면 로컬 .gz 존재로만 판단
    import glob
    import gzip
    import shutil
    base = service_day_of(datetime.now(KST))
    today = base.strftime("%Y-%m-%d")
    yest = (base - timedelta(days=1)).strftime("%Y-%m-%d")
    # ⚠️ exportDir 은 EC2 절대경로(/home/ubuntu/…)라 다른 기계에선 만들지 못할 수 있다.
    #    같은 config 를 맥/윈도우에서 쓰면 rotateHour 마다 이 스레드가 예외로 죽는데,
    #    데몬 스레드라 조용히 사라져 백업이 안 도는 걸 아무도 모른다. 안내하고 건너뛴다
    #    (수집 자체는 계속 돈다 — 로테이션은 부가 기능이다).
    try:
        os.makedirs(export_dir, exist_ok=True)
    except OSError as e:
        print(f"[백업] ⚠️ exportDir 을 못 만든다 — 로테이션 건너뜀: {export_dir} ({e})\n"
              f"        이 기계에서 백업이 필요 없으면 config 의 exportDir 을 null 로 둘 것.",
              flush=True)
        return
    for src in sorted(glob.glob(os.path.join(DATA, f"{prefix}-*.jsonl"))):
        day = os.path.basename(src)[len(prefix) + 1:len(prefix) + 11]  # {prefix}-|YYYY-MM-DD|.jsonl
        if day >= today:
            continue                                   # 오늘/미래 — 손대지 않음
        name = os.path.basename(src) + ".gz"
        dst = os.path.join(export_dir, name)
        # ① 백업 보장 — .gz 가 없으면 만든다 (원본 유지)
        if not os.path.exists(dst):
            try:
                with open(src, "rb") as fi, gzip.open(dst + ".tmp", "wb") as fo:
                    shutil.copyfileobj(fi, fo)
                os.replace(dst + ".tmp", dst)          # 원자적
                print(f"[백업] {name} 생성 ({os.path.getsize(dst)/1e6:.1f}MB)", flush=True)
            except OSError as e:
                print(f"[백업] ⚠️ {name} 실패 (원본 보존): {e}", flush=True)
                continue
        if remote:
            _rclone(["copy", dst, remote])             # 백업 직후 즉시 업로드 시도
        # ② 삭제는 그저께 이전만 — 어제는 원본 유지(ETA)
        if day >= yest:
            continue
        # 유예 보장 — 캐치업(며칠 꺼졌다 재기동)으로 방금 만든 .gz 는 같은 패스에서
        # 바로 지우지 않는다. "삭제 전 하루 유예"(README) 가 정상 운영에서만이 아니라
        # 항상 성립하게. 정상 흐름은 .gz 생성(어제 취급)→삭제(그저께 취급)가 하루
        # 간격이라 이 가드를 그냥 통과한다.
        try:
            if time.time() - os.path.getmtime(dst) < 20 * 3600:
                continue
        except OSError:
            continue
        if _backup_verified(name, dst, remote):
            os.remove(src)
            print(f"[삭제] {os.path.basename(src)} (백업 확인됨)", flush=True)
        else:
            print(f"[삭제 보류] {os.path.basename(src)} — 백업 미확인, 원본 유지 (수동 확인 요망)", flush=True)


def bump(conn, routeid, from_ord, to_ord, band, daytype, k=1):
    conn.execute("""
      INSERT INTO cell(routeid,from_ord,to_ord,band,daytype,n) VALUES(?,?,?,?,?,?)
      ON CONFLICT(routeid,from_ord,to_ord,band,daytype) DO UPDATE SET n=n+?
    """, (routeid, from_ord, to_ord, band, daytype, k, k))


# ── 지하철 관제 시각 (§8.1) ────────────────────────────────────────
# 정시성 판정은 t(우리 폴링 시각)가 아니라 recptnDt(관제 수신 시각)를 쓴다.
# 그런데 원본 API 에 두 가지 함정이 있다 (✅ 2026-07-18 실측 119,151행).
PREDICTIVE_AHEAD = 60      # 이 이상 '미래'면 예측성 피드로 본다 (초)
STALE_MAX = 1800           # |recptnDt − t| 가 이보다 크면 죽은 레코드 — 판정 제외


def recptn_of(r):
    """행의 recptnDt 를 datetime 으로 — 자정 날짜 오류를 보정한다.

    ⚠️ 원본 API 는 자정을 넘긴 뒤에도 recptnDt 의 **날짜만 당일로** 찍는다:
    00:01:38 에 폴링한 행이 recptnDt=<오늘> 23:58:38 로 온다 (실제로는 어제
    23:58:38). 그대로 쓰면 24시간 가까이 어긋난 값이 판정에 섞인다
    (✅ 846행 / 119,151). 1시간 넘게 미래면 하루를 뺀다 — 정상 오차는
    분 단위라 이 임계값에서 오분류가 없다.
    """
    try:
        b = datetime.strptime(r["recptnDt"], "%Y-%m-%d %H:%M:%S")
    except (KeyError, TypeError, ValueError):
        return None
    try:
        t = datetime.fromisoformat(r["t"]).replace(tzinfo=None)
    except (KeyError, ValueError):
        return b
    if (b - t).total_seconds() > 3600:
        b -= timedelta(days=1)
    return b


def stale(r):
    """판정에 못 쓰는 행인가 — API 가 되돌려주는 '죽은' 레코드.

    종착역처럼 지금 열차가 없는 역은 원본이 **어젯밤 막차 기록을 계속 반환한다**
    (✅ 실측: 14:18 폴링에 8호선 암사 recptnDt=전날 23:57 — 14시간 전).
    이걸 정시성에 넣으면 거대한 이상치가 된다.

    분포가 깨끗하게 갈려 임계값이 애매하지 않다 (✅ 119,151행):
    99%가 19분 이내, 그 위는 곧장 16시간대 — 그 사이가 비어 있다.
    """
    b = recptn_of(r)
    if b is None:
        return True
    try:
        t = datetime.fromisoformat(r["t"]).replace(tzinfo=None)
    except (KeyError, ValueError):
        return True
    return abs((b - t).total_seconds()) > STALE_MAX


def audit_subway(paths=None):
    """지하철 원본 감사 — 노선별 recptnDt 성격과 죽은 레코드 비율 (§8.1).

    판정 전에 반드시 볼 것. recptnDt 가 폴링 시각보다 **미래**인 노선은 그 값이
    실측이 아니라 예측이라는 뜻이고, 예측을 시각표와 비교하면 순환 논증이 된다
    (운영사가 시각표로 예측을 만들었다면 편차 0 이 나온다).
    """
    import glob
    import gzip
    import statistics
    if not paths:
        paths = sorted(glob.glob(os.path.join(DATA, "subway-*.jsonl")))
        exp = cfg().get("exportDir")
        if exp:
            paths += sorted(glob.glob(os.path.join(exp, "subway-*.jsonl.gz")))
    if not paths:
        sys.exit("subway jsonl 이 없다.")
    off, n_stale, n = {}, {}, 0
    trains = {}      # line -> trainNo -> {역 집합, 최초/최종 관측}
    stations = {}    # line -> 역 집합
    for p in paths:
        opener = gzip.open if p.endswith(".gz") else open
        for line in opener(p, mode="rt", encoding="utf-8"):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            b = recptn_of(r)
            if b is None:
                continue
            try:
                t = datetime.fromisoformat(r["t"]).replace(tzinfo=None)
            except (KeyError, ValueError):
                continue
            ln = r.get("line")
            n += 1
            if stale(r):
                n_stale[ln] = n_stale.get(ln, 0) + 1
                continue                      # 죽은 레코드는 성격 판정에서도 뺀다
            off.setdefault(ln, []).append((b - t).total_seconds())
            tn, sid = r.get("trainNo"), r.get("statnId")
            if tn and sid:
                stations.setdefault(ln, set()).add(sid)
                v = trains.setdefault(ln, {}).get(tn)
                if v is None:
                    trains[ln][tn] = {"st": {sid}, "min": t, "max": t}
                else:
                    v["st"].add(sid)
                    if t < v["min"]:
                        v["min"] = t
                    if t > v["max"]:
                        v["max"] = t
    print(f"파일 {len(paths)}개 · {n:,}행\n")
    print(f"{'노선':<10}{'중앙값':>8}{'미래%':>7}{'죽은%':>7}{'행':>9}  판정")
    flagged = []
    for ln, d in sorted(off.items(), key=lambda x: -statistics.median(x[1])):
        fut = sum(1 for x in d if x > PREDICTIVE_AHEAD) / len(d)
        dead = n_stale.get(ln, 0) / (len(d) + n_stale.get(ln, 0))
        verdict = "⚠️ 예측성 — 시각표 대조 전 판정 금지" if fut > 0.5 else "실측"
        if fut > 0.5:
            flagged.append(ln)
        print(f"{ln:<10}{statistics.median(d):>8.0f}{fut:>6.0%}{dead:>6.1%}{len(d):>9,}  {verdict}")
    print("\n중앙값 = recptnDt − 폴링시각(초). 음수 = 과거(실측), 양수 = 미래(예측).")

    # ── trainNo 의 의미가 노선마다 다르다 (✅ 2026-07-19 실측) ──────────
    # B안 셀은 trainNo 가 "1회 운행" 단위라고 가정한다. 그 가정이 깨지는 노선이
    # 있고, 깨지면 셀이 하루 만에 포화돼 관측 일수가 정시성과 무관해진다.
    print(f"\n{'노선':<10}{'열차수':>7}{'역/열차':>8}{'커버율':>7}{'시간폭':>8}   trainNo 의미")
    rep = []
    for ln, tr in trains.items():
        ns = len(stations.get(ln, ())) or 1
        med = statistics.median(len(v["st"]) for v in tr.values())
        spans = sorted((v["max"] - v["min"]).total_seconds() / 3600 for v in tr.values())
        sp = spans[len(spans) // 2]
        rep.append((med / ns, ln, len(tr), med, sp))
    for cov, ln, ntr, med, sp in sorted(rep, reverse=True):
        if sp > 6:
            k = "❌ 종일 재사용 — 셀이 하루에 포화"
        elif cov < 0.25:
            k = "⚠️ 운행 일부만 — 셀 파편화"
        else:
            k = "✅ 1회 운행"
        print(f"{ln:<10}{ntr:>7}{med:>8.0f}{cov:>6.0%}{sp:>7.1f}h   {k}")
    print("커버율 = 한 열차가 지나는 역 ÷ 그 노선 역 수. 급행·지선이 있으면 낮게 나온다.")
    print("시간폭 = 한 trainNo 가 관측된 시간 길이. 6시간 초과면 1회 운행이 아니라")
    print("번호가 종일 재사용되는 것이고, 그 노선은 (열차,역,요일) 셀이 성립하지 않는다.")
    if flagged:
        print(f"⚠️ {', '.join(flagged)}: recptnDt 가 미래다. §8.1 — 이 노선은 "
              f"시각표(§9 #1)와 대조해 예측 근거를 확인하기 전까지 정시성 판정에 쓰지 말 것.")


def bump_subway(conn, line, trainNo, statnId, daytype, day):
    """지하철 셀 +1 — n = 관측 일수. **같은 운행일에 두 번 불러도 한 번만 오른다.**

    ⚠️ 하루 1회 제약이 수집기 메모리(bumped 집합)에만 있었을 때 셀이 통째로
    0 이 되는 경로가 있었다 (✅ 재현): jsonl 은 행마다 즉시 append 되지만 bump 는
    사이클 끝에서 commit 된다. 사이클 도중 죽으면 jsonl 은 남고 DB 는 롤백되는데,
    재시작 때 seed_today 가 **그 jsonl 로** dedup 집합을 채워 "이미 셌다"고 판단
    → 그날 셀이 영영 0. 재시작이 잦을수록(install/stop) 확실히 터진다.

    그래서 제약을 디스크로 내린다. day 가 같으면 UPDATE 가 스킵되므로 재시작 후
    다시 시도해도 안전하고, 죽어서 놓친 bump 는 다음 사이클에 저절로 복구된다.
    """
    conn.execute("""
      INSERT INTO subway_cell(line,trainNo,statnId,daytype,n,last_day) VALUES(?,?,?,?,1,?)
      ON CONFLICT(line,trainNo,statnId,daytype) DO UPDATE SET
        n = n + 1, last_day = excluded.last_day
      WHERE subway_cell.last_day IS NOT excluded.last_day
    """, (line, trainNo, statnId, daytype, day))


def route_progress(conn, target, nbands):
    """노선별 (충족 셀, 목표 셀, 관측된 셀). 목표 셀 = 구간수 × 밴드수 × 7요일.

    ⚠️ 7요일 전부 센다 — 평일만 세면(이전 판) 평일이 다 찬 노선이 pct=1.0 으로
    로테이션에서 빠져 토·일 셀이 영영 안 채워지는 구조가 된다. 대시보드(server.py)의
    분모와도 일치해야 한다.
    """
    rows = conn.execute("""
      SELECT r.routeid, r.routeno, r.routetp, r.cityCode, r.nstops,
             COALESCE(SUM(CASE WHEN c.n >= ? THEN 1 ELSE 0 END), 0),
             COUNT(c.n)
      FROM route r LEFT JOIN cell c ON c.routeid = r.routeid
      GROUP BY r.routeid
    """, (target,)).fetchall()
    out = []
    for rid, no, tp, city, nstops, done, seen in rows:
        goal = max(1, (nstops - 1)) * nbands * 7  # 구간수 × 밴드수 × 7요일
        out.append({
            "routeid": rid, "routeno": no, "routetp": tp, "cityCode": city,
            "goal": goal, "done": done, "seen": seen,
            "pct": done / goal if goal else 0.0,
        })
    return out


def _hm(s):
    """'0450' → 290 (자정 이후 분). 값이 없거나 이상하면 None."""
    if not s or not str(s).isdigit() or len(str(s)) < 4:
        return None
    s = str(s)
    return int(s[:2]) * 60 + int(s[2:4])


def tail_fn():
    """config 를 **한 번만** 읽어 nstops → 꼬리(분) 함수를 만든다.

    ⚠️ maybe_running 안에서 cfg() 를 부르면 pick_routes 가 노선마다(경기 2,200개)
    config.json 을 다시 파싱한다. 재선정이 시간당 1회라 치명적이진 않지만
    한 줄로 없앨 수 있는 낭비다 — 호출부가 이걸 만들어 넘긴다.
    """
    k = cfg()
    per = k.get("tailPerStopMin", 1.5)
    lo, hi = k.get("tailMinMin", 90), k.get("tailMaxMin", 300)
    return lambda nstops: (min(hi, max(lo, int((nstops or 0) * per))) if nstops else hi)


def maybe_running(startvt, endvt, t, nstops=None, tail_min=None):
    """지금 이 노선에 버스가 있을 법한가 — **1차 필터일 뿐이다.**

    ⚠️ 확실히 아닌 것만 거른다. 진짜 판정은 관측이 한다(emptyStreak).

    `endvehicletime` 은 막차 **출발** 시각이다. 그 뒤로도 종점까지 달린다.
    ✅ 실측: 36번은 00:50 출발 + 177정류소 → 05:16 도착 추정 = 꼬리 **266분**.

    ⚠️ 고정 90분은 그 실측과 자기모순이었다 — 00:50+90 = 02:20 에 창이 닫혀
    05:16 까지 달리는 36번이 02:20 부터 후보에서 빠졌다. 1단계에서 빠지면
    emptyStreak(2단계 관측)이 볼 기회 자체가 없어, collector-design §2.6 이
    경고한 실패("36번을 버리고 차고의 낮 노선을 찍는다")가 기본값에서 재현됐다.
    하필 심야 장거리 = 광역/직행좌석 = 문서가 "버스 차별점이 성립하는 유일한
    부류"로 지목한 노선들이다.

    → 꼬리를 **정류소 수에 비례**시킨다: nstops × tailPerStopMin (기본 1.5분),
      하한 90분. 36번(177정류소)이면 266분으로 실측과 맞는다.
      nstops 를 모르면 상한(tailMaxMin, 기본 300분)으로 넉넉히 연다 —
      과하게 여는 쪽이 안전하다(빈 응답 몇 콜 vs 데이터 영구 손실).

    운행시간 정보가 없으면 True(폴링해서 확인).
    """
    a, b = _hm(startvt), _hm(endvt)
    if a is None or b is None:
        return True
    if tail_min is None:
        tail_min = tail_fn()(nstops)   # 단독 호출용. 루프에선 호출부가 tail_fn 을 재사용한다
    # ⚠️ 절대 분으로 계산한다. b 에 꼬리를 더한 뒤 % 1440 부터 하면, 꼬리가 창 끝을
    #    시작 시각 너머로 밀 때 자정 넘김 판정이 뒤집힌다 — 36번(a=05:00, b=00:50,
    #    꼬리 265)이면 b→05:15 가 되어 "05:00~05:15 만 운행"으로 읽혔다.
    end = b + (1440 if b < a else 0) + tail_min   # 막차 출발(자정 넘으면 +1일) + 꼬리
    if end - a >= 1440:
        return True                               # 창이 하루를 통째로 덮는다
    end %= 1440
    now = t.hour * 60 + t.minute
    return (a <= now <= end) if a <= end else (now >= a or now <= end)


def pick_routes(conn, n, target, nbands, t=None, max_empty=6):
    """미충족이 큰 노선 우선. 단 **지금 안 도는 노선은 뺀다.**

    ⚠️ 이전 판은 커버리지만 보고 골라서, 심야에 '미커버 순 40개'를 찍었다.
    그게 실제로 도는 노선인지는 안 봤다 — 05:16 까지 달리는 36번을 떨어뜨리고
    이미 차고에 들어간 낮 노선을 찍고 있을 수 있었다. **가장 필요한 데이터를 버리는 동작.**

    이제 두 단계로 거른다:
      1. 운행시간 창 (명백히 아닌 것만)
      2. emptyStreak — 연속으로 0대면 후순위. **추정이 아니라 관측이 정한다.**
    """
    prog = route_progress(conn, target, nbands)
    live = [p for p in prog if p["pct"] < 1.0]
    if t is not None:
        # nstops 도 같이 — 막차 꼬리를 정류소 수에 비례시킨다 (maybe_running 참조).
        # 긴 노선일수록 막차 출발 후 오래 달리므로 고정 꼬리는 장거리를 잘라낸다.
        meta = {r[0]: r[1:] for r in conn.execute(
            "SELECT routeid, startvt, endvt, emptyStreak, nstops FROM route")}
        tail = tail_fn()                       # config 는 여기서 한 번만 읽는다
        out = []
        for p in live:
            m = meta.get(p["routeid"])
            if not m:
                out.append(p)
                continue
            s, e, streak, nstops = m
            if not maybe_running(s, e, t, tail_min=tail(nstops)):
                continue                       # 운행시간 밖
            if (streak or 0) >= max_empty:
                p["cold"] = True               # 계속 비어 있음 — 후순위
            out.append(p)
        live = out
    live.sort(key=lambda p: (p.get("cold", False), p["pct"], -p["goal"]))
    return live[:n]


def mark_empty(conn, routeid, empty):
    """관측 결과로 emptyStreak 갱신. 한 대라도 보이면 리셋."""
    if empty:
        conn.execute("UPDATE route SET emptyStreak = COALESCE(emptyStreak,0)+1 WHERE routeid=?", (routeid,))
    else:
        conn.execute("UPDATE route SET emptyStreak = 0, lastSeen = ? WHERE routeid=?",
                     (time.time(), routeid))


def status():
    c = connect()
    k = cfg()
    nb = len(k["timebands"])
    tgt = k["targetSamples"]
    prog = route_progress(c, tgt, nb)
    if not prog:
        print("노선 풀이 비어 있다. fetch_routes.py 를 먼저 돌릴 것.")
        return

    done_routes = [p for p in prog if p["pct"] >= 1.0]
    goal = sum(p["goal"] for p in prog)
    done = sum(p["done"] for p in prog)
    seen = sum(p["seen"] for p in prog)
    total_obs = c.execute("SELECT COALESCE(SUM(n),0) FROM cell").fetchone()[0]

    print(f"=== 커버리지 (요일 7종 분리 · 목표 {tgt}샘플 · 시간대 {nb}개) ===")
    print(f"  노선      {len(prog):,}개 중 완주 {len(done_routes):,}")
    print(f"  셀        {done:,} / {goal:,} 충족 ({done/goal*100 if goal else 0:.1f}%)")
    print(f"  관측된 셀  {seen:,} ({seen/goal*100 if goal else 0:.1f}%)  ← 한 번이라도 본 것")
    print(f"  총 관측    {total_obs:,}건")

    # 남은 기간 추정 — 셀당 하루 관측률로 역산
    if seen:
        per_cell_day = 51.5 / nb  # ✅ 성남 실측 51.5회/구간/일
        remain_cells = goal - done
        rate = min(k["maxRoutes"], len(prog) - len(done_routes))
        if rate:
            rounds = max(1, (len(prog) - len(done_routes)) / rate)
            weeks_per_round = tgt / per_cell_day  # 각 요일은 주 1회씩만 발생 → 주 단위
            print(f"\n  추정: 라운드 {rounds:.1f}회 × {weeks_per_round:.1f}주 = 약 {rounds*weeks_per_round:.0f}주")
            print(f"        (각 요일은 주 1일씩만 오지만 셀당 관측이 ~{per_cell_day:.1f}건/일이라 "
                  f"{tgt}샘플은 대개 그 요일 하루 안에 찬다 — 기간은 로테이션 라운드 수가 지배)")

    print(f"\n=== 진행 중 상위/하위 ===")
    live = sorted([p for p in prog if p["pct"] < 1.0], key=lambda p: -p["pct"])
    for p in live[:3]:
        print(f"  {p['routeno']:<6} {p['routetp'][:4]:<4} {p['pct']*100:5.1f}%  ({p['done']:,}/{p['goal']:,})")
    if len(live) > 6:
        print("  …")
    for p in live[-3:]:
        print(f"  {p['routeno']:<6} {p['routetp'][:4]:<4} {p['pct']*100:5.1f}%  ({p['done']:,}/{p['goal']:,})")


def reset(force):
    """관측 카운트(cell) + 관측 파생 상태(emptyStreak/lastSeen) 초기화.

    노선 풀(route)과 일 콜 카운터(.buscalls-*)는 **유지한다** —
    풀은 다시 받으려면 4,400콜이고, 콜 카운터는 오늘 실제로 쓴 쿼터라
    지우면 50만 상한을 넘겨 그날 수집이 죽을 수 있다.
    """
    c = connect()
    n = c.execute("SELECT COALESCE(SUM(n),0) FROM cell").fetchone()[0]
    if not force:
        sys.exit(f"관측 {n:,}건이 지워진다. jsonl 데이터도 같이 지울 것(rm data/bus-*.jsonl).\n"
                 f"정말이면: python3 orchestrator.py reset --yes")
    c.execute("DELETE FROM cell")
    c.execute("UPDATE route SET emptyStreak = 0, lastSeen = NULL")
    c.commit()
    print(f"관측 {n:,}건 초기화. 노선 풀·일 콜 카운터는 유지 (쿼터는 실사용이라 지우면 안 된다).")
    print("수집기가 돌고 있었다면 재시작할 것 — 메모리의 누적 카운터(written)는 별개다.")


def rebuild(force, shrink_ok=False):
    """장부(cell)를 jsonl 데이터에서 재계산 — 데이터 일부를 지웠거나 장부가 의심될 때.

    밴드·요일 **둘 다 t 에서 현 config 규칙으로 재계산**한다 — 분류 규칙이 바뀌면
    (weekday 통합→7종 분리, 밴드 [20,27]→[20,28] 등) 옛 행도 새 규칙으로 재분류되게.
    행에 저장된 band/daytype 은 행 자체를 읽을 때의 참고용이다. 그래서 밴드 경계를
    넓힌 뒤 rebuild 하면 예전에 밴드 밖이던 03시대 관측도 장부로 회수된다.
    emptyStreak/lastSeen 은 jsonl 에 이력이 없으므로 건드리지 않는다.
    ⚠️ 수집기를 멈추고 돌릴 것 — 재계산 중 들어온 관측은 교체 때 유실된다.

    메모리: 전체를 dict 로 들지 않는다 — 15주 뒤 셀 ~700만 개면 1GB+ 라
    t4g.micro 에서 OOM 이다. 파일(하루) 단위로만 모아 스테이징 테이블에 흘린다.

    축소 가드: 재계산 결과가 기존 장부의 절반 미만이면 중단한다 — 내보낸 .gz 를
    클라우드로 move 해버려 로컬에 과거가 없는 상태에서 돌리면 장부가 이틀치로
    쪼그라드는 사고를 막는다 (그래서 rclone 은 copy 를 쓴다 — README).
    """
    import glob
    import gzip
    files = sorted(glob.glob(os.path.join(DATA, "bus-*.jsonl")))
    # exportDir 로 내보낸 .gz 도 진실의 일부다 — 같이 재계산한다.
    # ⚠️ 단 **같은 날짜의 로컬 원본이 있으면 .gz 는 건너뛴다.** 로테이션 정책상
    #    어제 파일은 .gz 백업을 만든 뒤에도 원본을 로컬에 남기므로(ETA 용), 둘 다
    #    읽으면 그날 관측이 **두 번 세어져** 셀 n 이 부풀고 실제보다 일찍 '충족'된다.
    #    (삭제 보류된 그저께도 같은 상태가 된다.) ✅ 재현: 1건이 n=2 로 들어감.
    have = {os.path.basename(p) for p in files}
    exp = cfg().get("exportDir")
    if exp:
        files += [p for p in sorted(glob.glob(os.path.join(exp, "bus-*.jsonl.gz")))
                  if os.path.basename(p)[:-3] not in have]   # .gz 떼면 원본 이름
    if not force:
        sys.exit(f"jsonl {len(files)}개 파일에서 장부를 다시 계산해 cell 을 통째로 교체한다.\n"
                 f"수집기를 먼저 멈출 것. 정말이면: python3 orchestrator.py rebuild --yes")
    c = connect()
    old = c.execute("SELECT COALESCE(SUM(n),0) FROM cell").fetchone()[0]
    # 스테이징에 먼저 쌓고 마지막에 원자적으로 교체한다 — 도중 실패해도 기존 장부가 남는다
    c.execute("DROP TABLE IF EXISTS cell_stage")
    c.execute("""CREATE TABLE cell_stage (
        routeid TEXT NOT NULL, from_ord INTEGER NOT NULL, to_ord INTEGER NOT NULL,
        band INTEGER NOT NULL, daytype TEXT NOT NULL, n INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (routeid, from_ord, to_ord, band, daytype))""")
    bad = total = 0
    k = cfg()
    hols = holiday_set()   # 자동 조회 ∪ 수동 — 수집기와 같은 기준. 루프 밖에서 한 번만
    bands = k["timebands"]
    for p in files:
        opener = gzip.open if p.endswith(".gz") else open
        day_counts = {}   # 이 파일(하루)에서만 — 메모리 상한이 하루치로 묶인다
        for line in opener(p, mode="rt", encoding="utf-8"):
            try:
                r = json.loads(line)
            except ValueError:
                bad += 1  # 강제종료로 잘린 마지막 줄 등 — 한 줄 손상은 한 줄만 버린다
                continue
            try:
                fo, to = int(r["from_ord"]), int(r["to_ord"])
            except (KeyError, TypeError, ValueError):
                continue
            if to <= fo:
                continue  # 회차 아티팩트(168→1 등) — 수집기와 같은 기준으로 거른다
            # 밴드·요일은 저장값이 아니라 t 에서 다시 계산한다 — 분류 규칙이 바뀌어도
            # (weekday 통합 → 월~일 분리, 밴드 [20,27] → [20,28]) 옛 행이 재분류되도록.
            try:
                dtm = datetime.fromisoformat(r["t"])
            except (KeyError, ValueError):
                continue
            b = band_of(dtm, bands)
            if b is None:
                continue  # 현 밴드 밖 — 장부 대상 아님
            if service_day_of(dtm).strftime("%Y-%m-%d") in hols:
                continue  # 공휴일 — 수집기와 같은 기준으로 장부에서 제외 (jsonl 은 진실로 유지)
            dt = day_type(dtm)
            # 수집기와 같은 인접 구간 분해 — 2칸 이상 건너뛴 전이를 (fo,to) 셀로
            # 넣으면 분모(인접 구간수)와 어긋난다. bus_collector 의 bump 로직 참조.
            for o in range(fo, to):
                key = (r["routeid"], o, o + 1, b, dt)
                day_counts[key] = day_counts.get(key, 0) + 1
        c.executemany("""INSERT INTO cell_stage(routeid,from_ord,to_ord,band,daytype,n)
                         VALUES(?,?,?,?,?,?)
                         ON CONFLICT(routeid,from_ord,to_ord,band,daytype)
                         DO UPDATE SET n = n + excluded.n""",
                      [k + (v,) for k, v in day_counts.items()])
        total += sum(day_counts.values())
    if old and total < old * 0.5 and not shrink_ok:
        c.execute("DROP TABLE cell_stage")
        c.commit()
        sys.exit(f"⚠️ 중단 — 재계산 결과({total:,}건)가 기존 장부({old:,}건)의 절반 미만이다.\n"
                 "내보낸 .gz 가 전부 로컬(exportDir)에 있는지 확인할 것 — 클라우드로 move 했다면 먼저 내려받기.\n"
                 "그래도 맞다면: python3 orchestrator.py rebuild --yes --shrink-ok")
    ncell = c.execute("SELECT COUNT(*) FROM cell_stage").fetchone()[0]
    c.execute("DELETE FROM cell")
    c.execute("INSERT INTO cell SELECT * FROM cell_stage")
    c.execute("DROP TABLE cell_stage")
    c.commit()
    print(f"재계산 완료: 파일 {len(files)}개 → 관측 {total:,}건 · 셀 {ncell:,}개 "
          f"(이전 장부 {old:,}건" + (f" · 깨진 줄 {bad}" if bad else "") + ")")
    print("수집기가 돌고 있었다면 재시작할 것.")


def rebuild_subway(force):
    """지하철 셀(subway_cell)을 subway-*.jsonl 에서 재계산 — 요일 규칙이 바뀌었을 때.

    셀 n = **관측한 서로 다른 날 수**이므로, 파일에서도 (노선,열차,역,요일)별로
    **날짜를 집합으로 모아 그 크기**를 센다 (행 수가 아니다 — 같은 날 진입·도착·출발
    3행이 나와도 1일).
    요일은 저장값이 아니라 t 에서 다시 계산한다 → day_type 을 3종↔7종으로 바꿔도
    이 명령 한 번이면 재분류된다. 공휴일(config.holidays)은 수집기와 같게 제외한다.
    ⚠️ 수집기를 멈추고 돌릴 것.
    """
    import glob
    import gzip
    # 옛 노선별 파일(shinbundang-*/suinbundang-* — realtimePosition 시절)도 읽는다.
    # 형식은 달라도 line·trainNo·statnId·t 는 동일해 관측 일수 계산엔 그대로 유효하다 —
    # 일괄(ALL) 전환 이전에 쌓인 날들을 버리면 그만큼 수렴이 늦어진다.
    files = []
    for pre in ("subway", "shinbundang", "suinbundang"):
        files += sorted(glob.glob(os.path.join(DATA, f"{pre}-*.jsonl")))
    # 버스와 같은 이유로 같은 날짜의 로컬 원본이 있으면 .gz 는 건너뛴다.
    # (여기선 날짜를 집합으로 세어 n 이 안 부풀지만, 같은 파일을 두 번 읽는 낭비는 없앤다.)
    have = {os.path.basename(p) for p in files}
    exp = cfg().get("exportDir")
    if exp:
        for pre in ("subway", "shinbundang", "suinbundang"):
            files += [p for p in sorted(glob.glob(os.path.join(exp, f"{pre}-*.jsonl.gz")))
                      if os.path.basename(p)[:-3] not in have]
    if not force:
        sys.exit(f"subway jsonl {len(files)}개에서 subway_cell 을 재계산해 교체한다.\n"
                 f"수집기를 먼저 멈출 것. 정말이면: python3 orchestrator.py rebuild-subway --yes")
    days = {}   # (line, trainNo, statnId, daytype) -> {운행일...}
    bad = 0
    hols = holiday_set()   # ⚠️ 루프 밖에서 한 번만 — 행마다 부르면 집합을 매번 새로 만든다
    for p in files:
        opener = gzip.open if p.endswith(".gz") else open
        for line in opener(p, mode="rt", encoding="utf-8"):
            try:
                r = json.loads(line)
            except ValueError:
                bad += 1
                continue
            ln, tn, sid = r.get("line"), r.get("trainNo"), r.get("statnId")
            if not (ln and tn and sid):
                continue
            try:
                t = datetime.fromisoformat(r["t"])
            except (KeyError, ValueError):
                continue
            if service_day_of(t).strftime("%Y-%m-%d") in hols:
                continue                      # 수집기와 같은 기준 — 평일 다이어가 아니다
            key = (ln, tn, sid, day_type(t))
            days.setdefault(key, set()).add(service_day_of(t).strftime("%Y-%m-%d"))
    c = connect()
    old = c.execute("SELECT COALESCE(SUM(n),0) FROM subway_cell").fetchone()[0]
    c.execute("DELETE FROM subway_cell")
    # last_day = 그 셀에서 관측한 **마지막 운행일**. 이걸 안 채우면 재집계 직후
    # 수집기가 같은 날을 한 번 더 세서 n 이 1 부풀어 오른다.
    c.executemany(
        "INSERT INTO subway_cell(line,trainNo,statnId,daytype,n,last_day) VALUES(?,?,?,?,?,?)",
        [k + (len(v), max(v)) for k, v in days.items()])
    c.commit()
    print(f"지하철 셀 재계산: 파일 {len(files)}개 → 셀 {len(days):,}개 "
          f"(관측일 합 {sum(len(v) for v in days.values()):,} · 이전 {old:,})"
          + (f" · 깨진 줄 {bad}" if bad else ""))
    print("수집기가 돌고 있었다면 재시작할 것.")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "status":
        status()
    elif cmd == "routes":
        c = connect()
        k = cfg()
        for p in pick_routes(c, k["maxRoutes"], k["targetSamples"], len(k["timebands"])):
            print(f"{p['routeid']}\t{p['cityCode']}\t{p['routeno']}\t{p['pct']*100:.1f}%")
    elif cmd == "reset":
        reset("--yes" in sys.argv[2:])
    elif cmd == "rebuild":
        rebuild("--yes" in sys.argv[2:], "--shrink-ok" in sys.argv[2:])
    elif cmd == "rebuild-subway":
        rebuild_subway("--yes" in sys.argv[2:])
    elif cmd == "audit-subway":
        audit_subway([p for p in sys.argv[2:] if not p.startswith("-")])
    elif cmd == "holidays":
        # 지금 장부에서 빠지는 날들. --refresh 로 캐시를 무시하고 다시 받는다.
        if "--refresh" in sys.argv[2:]:
            H.refresh(key=H.data_go_kr_key())
        auto, manual = H.load(), set(cfg().get("holidays") or [])
        d = H.info()
        print(f"공휴일 {len(auto | manual)}일 (자동 {len(auto)} · 수동 {len(manual)}) · "
              f"출처 {d['source'] or '없음'} · 갱신 {d['updated'] or '—'}")
        today = datetime.now(KST).strftime("%Y-%m-%d")
        for x in sorted(auto | manual):
            if x >= today:
                tags = ("수동" if x in manual else "") + (" 오늘" if x == today else "")
                print(f"  {x} {tags}".rstrip())
    else:
        sys.exit(f"모르는 명령: {cmd}  "
                 "(status | routes | holidays | reset | rebuild | rebuild-subway)")


if __name__ == "__main__":
    main()
