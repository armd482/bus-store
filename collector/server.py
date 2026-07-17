#!/usr/bin/env python3
"""
수집 서버 — 수집기 + 대시보드를 한 프로세스로 (docs/collector-design.md)

다른 컴퓨터에서 24시간 무인으로 돌리고, 브라우저로 상태를 본다.
표준 라이브러리만 쓴다 (설치할 것 없음).

  python3 server.py                 # 수집 + 대시보드 (기본 877 포트)
  python3 server.py --port 9000
  python3 server.py --no-collect    # 대시보드만 (조회용)

⚠️ 대시보드는 인증이 없다. 같은 기계에 API 키(.env)가 있으므로
   외부에 열려면 그 앞에 인증을 두거나 방화벽으로 막을 것.
"""

import argparse
import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import orchestrator as O

try:
    import bus_collector
except Exception:
    bus_collector = None

# 수집기가 갱신하는 최근 상태 (대시보드가 읽는다)
STATE = {
    "started": None, "cycles": 0, "lastObs": None, "lastCycleSec": None,
    "picked": 0, "moving": 0, "errors": {}, "written": 0, "night": False,
    "fetching": False,  # 지금 API 요청 사이클이 도는 중인가 (대시보드 표시용)
    "errLog": [],       # 최근 잔여 실패 이력 (대시보드 오류 리스트)
}
LOCK = threading.Lock()


AVG_ROW_BYTES = 222  # 슬림 형식 행 평균 (✅ 실측) — 행 수 추정용


def _obs_rate_per_day():
    """(하루 관측률 행/일, 측정 창 일수) — 최근 1~2 운행일 jsonl 크기 ÷ 경과 시간.

    창 일수도 돌려준다 — ETA 가 측정 창이 얇을 때(방금 시작) 요동치므로,
    호출부가 이 값으로 '측정 중'을 판단한다. 행 수를 직접 세지 않고
    크기 ÷ 평균 행 크기로 추정한다 (수백 MB 를 5초마다 세지 않기 위함).
    """
    now = datetime.now(O.KST)
    sd = O.service_day_of(now)
    anchor = now.replace(hour=4, minute=0, second=0, microsecond=0)
    if now.hour < 4:
        anchor -= timedelta(days=1)          # 운행일 시작(04시) 앵커
    elapsed = (now - anchor).total_seconds()

    window, size = 0.0, 0
    p_today = os.path.join(O.DATA, f"bus-{sd:%Y-%m-%d}.jsonl")
    p_prev = os.path.join(O.DATA, f"bus-{sd - timedelta(days=1):%Y-%m-%d}.jsonl")
    if os.path.exists(p_today) and elapsed > 3600:
        size += os.path.getsize(p_today)
        window += elapsed
    if os.path.exists(p_prev):
        size += os.path.getsize(p_prev)
        window += 86400
    if window < 3600 or size == 0:
        return None, 0.0                     # 표본 부족
    return (size / AVG_ROW_BYTES) / (window / 86400), window / 86400


# ── 커버리지 집계 ──────────────────────────────────────────────────
def snapshot():
    k = O.cfg()
    c = O.connect()
    bands = k["timebands"]
    nb, tgt = len(bands), k["targetSamples"]

    routes = c.execute("SELECT COUNT(*), COALESCE(SUM(nstops),0) FROM route").fetchone()
    nroute, nstops = routes
    nseg = max(0, nstops - nroute)
    # 요일 7종을 전부 분리한 뒤로 토·일은 더 이상 '병목'이 아니다 — 월~일 모두
    # 주 1회씩만 채워지는 동등한 처지다. 그래서 완성률 분모도 7요일 전체로 본다.
    ndays = 7
    day_goal = nseg * nb           # 요일 하나 기준 — 요일별 진행률의 분모
    goal = day_goal * ndays        # 전체 목표 셀 (구간 × 밴드 × 7요일)

    # 한 번의 스캔으로 — 5초마다 오는 /api 가 셀 수백만 규모에서도 버티도록
    seen, total, done = c.execute(
        "SELECT COUNT(*), COALESCE(SUM(n),0), COALESCE(SUM(n >= ?),0) FROM cell", (tgt,)).fetchone()

    # 밴드별 — 오늘(운행일 기준) 요일만 본다. 자정~04시엔 전날 요일이 '오늘'이다
    # (01:30 관측은 전날 막차 — day_type 이 운행일 경계로 처리한다).
    now = datetime.now(O.KST)
    today = O.day_type(now)
    nowband = O.band_of(now, bands)
    byband = {b: (n, cells) for b, n, cells in c.execute(
        "SELECT band, SUM(n), COUNT(*) FROM cell WHERE daytype=? GROUP BY band", (today,))}
    band_rows = []
    band_need = nseg * tgt  # 이 요일·밴드에서 채워야 할 관측 수 = 구간수 × 목표샘플
    for i, (a, b) in enumerate(bands):
        n, cells = byband.get(i, (0, 0))
        band_rows.append({
            "i": i, "from": a, "to": b if b <= 24 else b - 24, "wrap": b > 24,
            "obs": n, "cells": cells, "need": band_need,
            "pct": n / band_need if band_need else 0,
            "peak": (a, b) in ((7, 9), (17, 20)),
        })

    # 요일별 — 분모는 요일 하나 기준(day_goal). 단일 GROUP BY 로 한 번에
    # (요일마다 별도 COUNT 쿼리를 치면 스캔 7회 — 셀이 커지면 /api 가 느려진다).
    byday = {}
    for d, n, cells, full in c.execute(
            "SELECT daytype, SUM(n), COUNT(*), SUM(n >= ?) FROM cell GROUP BY daytype", (tgt,)):
        byday[d] = {"obs": n, "cells": cells, "done": full, "pct": full / day_goal if day_goal else 0}

    # 쿼터는 달력일 키다 (data.go.kr 자정 리셋) — 운행일(service_day)이 아니다
    calls = bus_collector.read_calls(bus_collector.quota_day(datetime.now(O.KST))) if bus_collector else 0

    with LOCK:
        st = dict(STATE)
        st["errors"] = dict(STATE["errors"])
        st["errLog"] = list(STATE.get("errLog", []))  # 락 밖 직렬화 중 수집기 append 와 경합 방지

    # 남은 기간 — 밴드별 천장 도달 시점의 [최소, 최대] 범위.
    #
    # 속도: 최근 1~2 운행일 jsonl 크기 기반 (⚠️ 이전 판 버그 — total(역대 전체) ÷
    # elapsed(재시작 후)라서 재시작마다 뻥튀기, 실제 ~100일이 24.9일로 표시됐다).
    #
    # 구조: (밴드, 요일) 셀은 그 요일(주 1회)에만 채워지고, 운행 구조상 천장이 있다
    # (새벽·심야 ~82%, 낮 ~90% — 운행 안 하는 시간대의 셀은 영원히 빈다).
    # 밴드마다 "천장까지 몇 주"를 계산해 범위를 만든다. 단 안정화 두 겹:
    #  ① 측정 창이 1일 미만이면(방금 시작) '측정 중' — per_day 가 몇 시간치라 요동친다.
    #  ② 상한은 중앙값의 3배 이내 밴드의 최대치 — 데이터가 거의 없는 밴드 하나가
    #     max 를 305일처럼 튀게 하던 것을 자른다 (밴드 7개라 퍼센타일은 부정확).
    eta = eta_hi = None
    eta_measuring = False
    per_day, win_days = _obs_rate_per_day()
    if per_day and win_days < 1.0:
        eta_measuring = True                     # 표본은 있으나 창이 얇다
    elif per_day:
        band_tot = dict(c.execute("SELECT band, SUM(n) FROM cell GROUP BY band"))
        tot_all = sum(band_tot.values())
        need_bd = nseg * tgt                     # (밴드, 요일) 하나의 필요 관측 수
        CEIL = {0: 0.82, nb - 1: 0.82}           # 04-07·20-03시 천장 — 나머지 0.90
        weeks = []
        for i in range(nb):
            share = band_tot.get(i, 0) / tot_all if tot_all else 0  # 하루 관측 중 이 밴드 비중(실측 근사)
            avg_pct = band_tot.get(i, 0) / (need_bd * 7)            # 7요일 평균 진행률
            ceil = CEIL.get(i, 0.90)
            if share > 0.02 and avg_pct < ceil:  # share≤2% 밴드는 표본 부족 — 제외
                weekly_gain = per_day * share / need_bd             # 요일 하나 기준 주당 증가
                weeks.append((ceil - avg_pct) / weekly_gain)
        if weeks:
            weeks.sort()
            med = weeks[len(weeks) // 2]
            cap = med * 3                                          # 이상치 컷 — 중앙값 3배
            hi = max([w for w in weeks if w <= cap], default=weeks[0])
            eta, eta_hi = weeks[0] * 7, hi * 7                     # 일 단위

    return {
        "routes": nroute, "segments": nseg, "goal": goal, "dayGoal": day_goal,
        "dayNeed": nseg * nb * tgt,  # 요일 하나의 필요 관측 건수 = 밴드별 need × 밴드 수
        "target": tgt,
        "done": done, "seen": seen, "total": total,
        "pct": done / goal if goal else 0,
        "bands": band_rows, "days": byday, "today": today, "nowBand": nowband,
        "calls": calls, "quota": k["dailyQuota"],
        "state": st, "etaDays": eta, "etaDaysHi": eta_hi, "etaMeasuring": eta_measuring,
        "cfg": {kk: k[kk] for kk in
                ("targetSamples", "maxRoutes", "dispatchRate",
                 "intervalSec", "serviceWindow", "dailyQuota")},
    }


# ── 대시보드 ──────────────────────────────────────────────────────
PAGE = """<!doctype html><meta charset="utf-8"><title>수집 현황</title>
<meta name=viewport content="width=device-width,initial-scale=1">
<style>
 :root{color-scheme:light dark}
 body{font:14px/1.6 -apple-system,BlinkMacSystemFont,"Apple SD Gothic Neo",sans-serif;
      margin:0;padding:24px;max-width:900px;margin:0 auto}
 h1{font-size:18px;margin:0 0 4px} h2{font-size:14px;margin:28px 0 10px;opacity:.7}
 .sub{opacity:.55;font-size:12px;margin-bottom:20px}
 .big{font-size:34px;font-weight:700;letter-spacing:-.02em}
 .row{display:flex;align-items:center;gap:10px;margin:5px 0}
 .lbl{width:88px;font-variant-numeric:tabular-nums;opacity:.75;font-size:13px}
 .bar{flex:1;height:14px;background:#8883;border-radius:3px;overflow:hidden}
 .fill{height:100%;background:#3b82f6;transition:width .4s}
 .fill.ok{background:#22c55e} .fill.warn{background:#f59e0b} .fill.bad{background:#ef4444}
 .val{width:200px;text-align:right;font-variant-numeric:tabular-nums;font-size:12px;opacity:.7;white-space:nowrap}
 .tag{font-size:10px;padding:1px 5px;border-radius:3px;background:#8882;margin-left:4px}
 .warn{color:#f59e0b} .bad{color:#ef4444} .ok{color:#22c55e}
 table{border-collapse:collapse;width:100%;font-size:13px}
 td{padding:4px 8px 4px 0} td.n{text-align:right;font-variant-numeric:tabular-nums}
 .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px;margin:14px 0}
 .card{background:#8881;border-radius:8px;padding:12px}
 .card .k{font-size:11px;opacity:.6} .card .v{font-size:20px;font-weight:600;margin-top:2px}
 code{background:#8882;padding:1px 4px;border-radius:3px;font-size:12px}
</style>
<h1>버스 위치 수집 현황</h1>
<div class=sub id=sub>…</div>
<div id=app>불러오는 중…</div>
<script>
const pct = x => (x*100).toFixed(1)+'%';
const num = x => (x||0).toLocaleString();
function bar(p, cls){ return `<div class=bar><div class="fill ${cls||''}" style="width:${Math.min(100,p*100)}%"></div></div>`; }

let S = null;  // 최근 /api 응답 — 1초 타이머(paintObs)가 참조한다
async function tick(){
  const d = await (await fetch('/api')).json();
  S = d;
  const s = d.state;
  const alive = s.lastObs && (Date.now()/1000 - s.lastObs) < 180;

  document.getElementById('sub').innerHTML =
    `경기 ${num(d.routes)}노선 · ${num(d.segments)}구간 · 목표 ${num(d.goal)}셀 × ${d.target}샘플`
    + (alive ? ' · <span class=ok>●</span> 수집 중' : ' · <span class=bad>●</span> 멈춤');

  let h = '';
  // 완성률
  h += `<div class=big>${pct(d.pct)}</div>`;
  const etaTxt = d.etaMeasuring ? ' · 남은 기간 <b>측정 중</b> (관측 하루치 쌓이면 표시)'
    : d.etaDays!=null ? ` · 남은 기간 약 <b>${Math.round(d.etaDays)}~${Math.round(d.etaDaysHi)}일</b>`
       + ` (${(d.etaDays/7).toFixed(0)}~${(d.etaDaysHi/7).toFixed(0)}주 · 밴드별 천장 도달 기준)` : '';
  h += `<div class=sub>${num(d.done)} / ${num(d.goal)} 셀 충족${etaTxt}</div>`;
  h += bar(d.pct);

  // 건강
  const q = d.calls/d.quota;
  const errN = Object.values(s.errors).reduce((a,b)=>a+b,0);
  const errRate = s.picked ? errN/s.picked : 0;
  h += '<h2>건강 상태</h2><div class=grid>';
  h += `<div class=card><div class=k>쿼터</div><div class="v ${q>.95?'bad':q>.85?'warn':''}">${num(d.calls)}</div>
        <div class=k>/ ${num(d.quota)} (${pct(q)})</div></div>`;
  h += `<div class=card><div class=k>실패율</div><div class="v ${errRate>.05?'bad':errRate>.02?'warn':'ok'}">${pct(errRate)}</div>
        <div class=k>${Object.entries(s.errors).map(([k,v])=>k+'×'+v).join(' ')||'없음'}</div></div>`;
  h += `<div class=card><div class=k>마지막 관측</div><div class=v id=lastobs>—</div>
        <div class=k>사이클 ${s.lastCycleSec?s.lastCycleSec.toFixed(0)+'s':'—'} · ${s.picked}노선${s.night?' (심야)':''}</div></div>`;
  h += `<div class=card><div class=k>총 관측</div><div class=v>${num(d.total)}</div>
        <div class=k>운행 ${num(s.moving)}대</div></div>`;
  h += '</div>';

  // 밴드 — 지금 채워지는 요일의 **누적** (모든 주의 해당 요일 합).
  // 장부에 날짜가 없고 (밴드, 요일) 카운트만 쌓이므로 자연히 누적이다.
  // 04시(운행일 경계)에 다음 요일 뷰로 넘어간다 — 자정 넘김은 전날 요일에 계속.
  const KO = {mon:'월',tue:'화',wed:'수',thu:'목',fri:'금',sat:'토',sun:'일'};
  h += `<h2>밴드별 — <b>${KO[d.today]||'?'}요일</b> 누적 (모든 주 합산 · 04시에 다음 요일로 전환)</h2>`;
  for(const b of d.bands){
    const label = `${String(b.from).padStart(2,'0')}-${String(b.to).padStart(2,'0')}시`;
    const cur = d.nowBand!=null && b.i===d.nowBand;
    h += `<div class=row><div class=lbl>${label}${b.peak?'<span class=tag>첨두</span>':''}${b.wrap?'<span class=tag>익일</span>':''}${cur?'<span class=tag style="color:#3b82f6">진행 중</span>':''}</div>`
       + bar(b.pct, b.pct>=1?'ok':'')
       + `<div class=val>${num(b.obs)} / ${num(b.need)} · <b>${pct(b.pct)}</b></div></div>`;
  }

  // 요일 — 7종 전부 분리. 모든 요일이 주 1일씩만 얻는다 (목표 7샘플 = 요일당 7주)
  h += `<h2>요일별 완성률 (전부 분리) — 각 요일은 주 1일씩만 얻는다: 목표 ${d.target}샘플이면 요일당 ${d.target}주</h2><table>`;
  for(const [k,label] of [['mon','월'],['tue','화'],['wed','수'],['thu','목'],['fri','금'],['sat','토'],['sun','일']]){
    const v = d.days[k] || {obs:0,done:0,cells:0,pct:0};
    h += `<tr><td width=40>${label}</td>
          <td class=n width=64><b>${pct(v.pct)}</b></td>
          <td width=250>${bar(v.pct, v.pct>=1?'ok':'')}</td>
          <td class=n style="white-space:nowrap">충족 셀 ${num(v.done)} / ${num(d.dayGoal)}</td>
          <td class=n style="white-space:nowrap;opacity:.6">관측 ${num(v.obs)} / ${num(d.dayNeed)}건</td></tr>`;
  }
  h += '</table>';

  // 오류 로그 — 재시도 후에도 실패한 사이클만 (회복된 재시도는 제외). 최근 순.
  const log = (s.errLog||[]);
  h += `<h2>오류 로그 — 재시도 후 잔여 실패 (최근 ${log.length}건)</h2>`;
  if(!log.length){
    h += '<div class=sub style="color:#22c55e">최근 실패 없음</div>';
  } else {
    h += '<table>';
    for(const e of log.slice().reverse()){
      const t = new Date(e.t*1000).toLocaleTimeString('ko-KR',{hour12:false});
      const rate = e.picked ? (e.n/e.picked*100).toFixed(1) : '0';
      h += `<tr><td width=80 style="opacity:.6">${t}</td>`
         + `<td width=90 class=bad>실패 ${e.n}/${e.picked}</td>`
         + `<td width=44 class=n>${rate}%</td>`
         + `<td style="opacity:.8">${e.detail}</td></tr>`;
    }
    h += '</table>';
  }

  // 설정
  h += '<h2>설정 (읽기 전용 — config.json)</h2><table>';
  for(const [k,v] of Object.entries(d.cfg))
    h += `<tr><td width=140><code>${k}</code></td><td>${JSON.stringify(v)}</td></tr>`;
  h += '</table>';

  document.getElementById('app').innerHTML = h;
  paintObs();
}

// 마지막 관측 타이머 — /api 폴링(5초)과 별개로 1초마다 다시 그린다.
// 수집기가 요청 사이클 중이면 '데이터 요청 중' 태그를 함께 띄운다.
// (사이클의 대부분이 요청 시간이라, 타이머를 통째로 바꾸면 거의 항상 가려진다)
function paintObs(){
  const el = document.getElementById('lastobs');
  if(!el || !S) return;
  const s = S.state;
  const secs = s.lastObs ? Math.round(Date.now()/1000 - s.lastObs) : null;
  const alive = secs != null && secs < 180;
  el.className = 'v ' + (alive ? 'ok' : 'bad');
  el.innerHTML = (secs != null ? secs + '초 전' : '—')
    + (s.fetching ? ' <span class=tag style="background:#3b82f622;color:#3b82f6">데이터 요청 중</span>' : '');
}
tick(); setInterval(tick, 5000); setInterval(paintObs, 1000);
</script>
"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/api"):
            body = json.dumps(snapshot(), ensure_ascii=False).encode()
            ct = "application/json; charset=utf-8"
        elif self.path == "/":
            body = PAGE.encode()
            ct = "text/html; charset=utf-8"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass  # 접근 로그 끔 — 수집 로그만 본다


class Tee:
    """콘솔에는 전부, 파일에는 중요한 줄만 — 윈도우 bat 용 (--log).

    셸 리다이렉트(>>)로 하면 스케줄러가 띄우는 cmd 창이 텅 비어 버린다.
    창에는 그대로 흐르고, 파일은 **사망 사인 확인용**이므로 오류·경고와
    시작/종료 표식만 남긴다 — 사이클 로그로 채우면 정작 필요한 줄이 묻힌다.
    stderr 는 전체 보존한다 (트레이스백은 여러 줄이고 표식이 없다).
    """
    KEEP = ("⚠️", "❌", "Traceback", "Error", "수집 시작", "대시보드", "종료 감지", "일 상한")

    def __init__(self, stream, f, keep_all=False):
        self.s, self.f, self.keep_all, self.buf = stream, f, keep_all, ""

    def write(self, x):
        self.s.write(x)
        self.buf += x
        while "\n" in self.buf:
            line, self.buf = self.buf.split("\n", 1)
            if self.keep_all or any(k in line for k in self.KEEP):
                self.f.write(line + "\n")

    def flush(self):
        self.s.flush()
        self.f.flush()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=877)
    ap.add_argument("--no-collect", action="store_true", help="대시보드만")
    ap.add_argument("--log", help="출력을 이 파일에도 복사 (콘솔에는 그대로)")
    args = ap.parse_args()

    if args.log:
        os.makedirs(os.path.dirname(args.log) or ".", exist_ok=True)
        _f = open(args.log, "a", encoding="utf-8", buffering=1, errors="replace")
        sys.stdout = Tee(sys.stdout, _f)                 # 오류·경고·표식만
        sys.stderr = Tee(sys.stderr, _f, keep_all=True)  # 트레이스백은 전체

    if not args.no_collect:
        if bus_collector is None:
            raise SystemExit("bus_collector 를 못 불러왔다")
        bus_collector.STATE = STATE
        bus_collector.LOCK = LOCK
        t = threading.Thread(target=bus_collector.main, daemon=True)
        t.start()
        with LOCK:
            STATE["started"] = time.time()

        # ⚠️ 수집 스레드 감시 — 스레드만 죽으면 대시보드는 멀쩡히 떠 있어서
        #    launchd/systemd/배치 루프의 자동 재시작이 발동하지 않는 좀비가 된다
        #    (✅ 실전: 마지막 관측만 하염없이 늘어나는 채로 발견됨).
        #    스레드가 죽으면 프로세스째 내려서 재시작 장치가 되살리게 한다.
        def watchdog():
            t.join()  # 수집 스레드가 죽어야 리턴한다
            print("수집 스레드 종료 감지 — 프로세스를 내린다 (자동 재시작 장치가 되살린다)", flush=True)
            os._exit(1)
        threading.Thread(target=watchdog, daemon=True).start()

    srv = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print(f"대시보드 http://localhost:{args.port}  (수집 {'끔' if args.no_collect else '켬'})", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        # Ctrl-C 정상 종료. 수집 스레드는 daemon 이라 함께 죽는다 — 사이클 중간이어도
        # 안전하다: sqlite 는 WAL + 사이클 단위 commit, jsonl 은 줄 단위 append,
        # 콜 카운터는 os.replace 원자 쓰기.
        # ⚠️ os._exit 인 이유: daemon 스레드가 print 중(stdout 락 보유)에 인터프리터가
        #    finalize 되면 "_enter_buffered_busy ... could not acquire lock" 으로 abort 한다.
        #    finalize 를 건너뛰면 그 경합 자체가 없다.
        print("\n종료", flush=True)
        os._exit(0)


if __name__ == "__main__":
    main()
