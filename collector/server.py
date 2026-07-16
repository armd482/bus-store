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
}
LOCK = threading.Lock()


# ── 커버리지 집계 ──────────────────────────────────────────────────
def snapshot():
    k = O.cfg()
    c = O.connect()
    bands = k["timebands"]
    nb, tgt = len(bands), k["targetSamples"]

    routes = c.execute("SELECT COUNT(*), COALESCE(SUM(nstops),0) FROM route").fetchone()
    nroute, nstops = routes
    nseg = max(0, nstops - nroute)
    goal = nseg * nb  # 평일 기준 목표 셀

    done = c.execute("SELECT COUNT(*) FROM cell WHERE daytype='weekday' AND n >= ?", (tgt,)).fetchone()[0]
    seen = c.execute("SELECT COUNT(*) FROM cell WHERE daytype='weekday'").fetchone()[0]
    total = c.execute("SELECT COALESCE(SUM(n),0) FROM cell").fetchone()[0]

    # 밴드별 (평일)
    byband = {b: (n, cells) for b, n, cells in c.execute(
        "SELECT band, SUM(n), COUNT(*) FROM cell WHERE daytype='weekday' GROUP BY band")}
    band_rows = []
    seg_per_band = nseg or 1
    for i, (a, b) in enumerate(bands):
        n, cells = byband.get(i, (0, 0))
        band_rows.append({
            "i": i, "from": a, "to": b if b <= 24 else b - 24, "wrap": b > 24,
            "obs": n, "cells": cells, "pct": cells / seg_per_band if seg_per_band else 0,
            "peak": (a, b) in ((7, 9), (17, 20)),
        })

    # 요일별
    byday = {}
    for d, n, cells in c.execute("SELECT daytype, SUM(n), COUNT(*) FROM cell GROUP BY daytype"):
        full = c.execute("SELECT COUNT(*) FROM cell WHERE daytype=? AND n>=?", (d, tgt)).fetchone()[0]
        byday[d] = {"obs": n, "cells": cells, "done": full, "pct": full / goal if goal else 0}

    day = bus_collector.service_day(datetime.now(O.KST)) if bus_collector else None
    calls = bus_collector.read_calls(day) if bus_collector and day else 0

    with LOCK:
        st = dict(STATE)
        st["errors"] = dict(STATE["errors"])

    # 남은 기간 — 가정이 아니라 최근 실측 관측률로 역산
    eta = None
    if st["lastObs"] and total > 0 and st["started"]:
        elapsed = (time.time() - st["started"]) / 86400.0
        if elapsed > 0.002:  # 3분 이상 돌았을 때만
            per_day = total / elapsed
            remain = max(0, goal * tgt - total)
            if per_day > 0:
                eta = remain / per_day

    return {
        "routes": nroute, "segments": nseg, "goal": goal, "target": tgt,
        "done": done, "seen": seen, "total": total,
        "pct": done / goal if goal else 0,
        "bands": band_rows, "days": byday,
        "calls": calls, "quota": k["dailyQuota"],
        "state": st, "etaDays": eta,
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
 .val{width:120px;text-align:right;font-variant-numeric:tabular-nums;font-size:12px;opacity:.7}
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

async function tick(){
  const d = await (await fetch('/api')).json();
  const s = d.state;
  const alive = s.lastObs && (Date.now()/1000 - s.lastObs) < 180;

  document.getElementById('sub').innerHTML =
    `경기 ${num(d.routes)}노선 · ${num(d.segments)}구간 · 목표 ${num(d.goal)}셀 × ${d.target}샘플`
    + (alive ? ' · <span class=ok>●</span> 수집 중' : ' · <span class=bad>●</span> 멈춤');

  let h = '';
  // 완성률
  h += `<div class=big>${pct(d.pct)}</div>`;
  h += `<div class=sub>${num(d.done)} / ${num(d.goal)} 셀 충족`
     + (d.etaDays!=null ? ` · 남은 기간 약 <b>${d.etaDays.toFixed(1)}일</b> (현재 관측률 기준)` : '') + '</div>';
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
  h += `<div class=card><div class=k>마지막 관측</div><div class="v ${alive?'ok':'bad'}">${
        s.lastObs? Math.round(Date.now()/1000-s.lastObs)+'초 전':'—'}</div>
        <div class=k>사이클 ${s.lastCycleSec?s.lastCycleSec.toFixed(0)+'s':'—'} · ${s.picked}노선${s.night?' (심야)':''}</div></div>`;
  h += `<div class=card><div class=k>총 관측</div><div class=v>${num(d.total)}</div>
        <div class=k>운행 ${num(s.moving)}대</div></div>`;
  h += '</div>';

  // 밴드
  h += '<h2>밴드별 (평일) — 컴퓨터가 꺼진 시간대는 영원히 0이다</h2>';
  for(const b of d.bands){
    const label = `${String(b.from).padStart(2,'0')}-${String(b.to).padStart(2,'0')}시`;
    const stale = b.obs===0;
    h += `<div class=row><div class=lbl>${label}${b.peak?'<span class=tag>첨두</span>':''}${b.wrap?'<span class=tag>익일</span>':''}</div>`
       + bar(b.pct, stale?'bad':b.pct>=1?'ok':'')
       + `<div class=val>${num(b.obs)} ${stale?'<span class=bad>⚠ 관측 없음</span>':''}</div></div>`;
  }

  // 요일
  h += '<h2>요일별 — 토·일은 주 1일씩만 얻는다</h2><table>';
  for(const [k,label] of [['weekday','평일'],['sat','토요일'],['sun','일요일']]){
    const v = d.days[k] || {obs:0,done:0,pct:0};
    const note = k!=='weekday' ? `<span class=tag>목표 ${d.target}샘플이면 ${d.target}주</span>` : '';
    h += `<tr><td width=70>${label}${note}</td><td width=280>${bar(v.pct, k==='weekday'?'':'warn')}</td>
          <td class=n>${num(v.obs)}건</td></tr>`;
  }
  h += '</table>';

  // 설정
  h += '<h2>설정 (읽기 전용 — config.json)</h2><table>';
  for(const [k,v] of Object.entries(d.cfg))
    h += `<tr><td width=140><code>${k}</code></td><td>${JSON.stringify(v)}</td></tr>`;
  h += '</table>';

  document.getElementById('app').innerHTML = h;
}
tick(); setInterval(tick, 5000);
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=877)
    ap.add_argument("--no-collect", action="store_true", help="대시보드만")
    args = ap.parse_args()

    if not args.no_collect:
        if bus_collector is None:
            raise SystemExit("bus_collector 를 못 불러왔다")
        bus_collector.STATE = STATE
        bus_collector.LOCK = LOCK
        t = threading.Thread(target=bus_collector.main, daemon=True)
        t.start()
        with LOCK:
            STATE["started"] = time.time()

    srv = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print(f"대시보드 http://localhost:{args.port}  (수집 {'끔' if args.no_collect else '켬'})", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
