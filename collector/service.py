#!/usr/bin/env python3
"""수집기 서비스 관리 — 설치 / 시작 / 완전 종료 / 제거를 OS 공통 명령으로 (macOS·리눅스·윈도우).

  python3 service.py install     상시 실행 등록 + 시작 (크래시·재부팅 자동 재시작)
  python3 service.py stop        ★ 완전 종료 — 프로세스 중지 + 자동 재시작·부팅 실행 해제.
                                 start 하기 전까지 재부팅해도 안 살아난다.
  python3 service.py start       다시 켜기 (등록은 유지돼 있던 것)
  python3 service.py logs        로그 실시간 보기
  python3 service.py uninstall   등록 자체를 제거
  python3 service.py status      상태 확인

install/start 는 등록·시작만 하고 **바로 프롬프트를 돌려준다**. 수집은 systemd·
launchd 가 백그라운드로 돌린다. 로그를 이어서 보려면 `--logs` 를 붙이거나
따로 `service.py logs` (Ctrl-C 는 보기만 끝낸다 — 수집은 계속 돈다).

install 은 시작 전에 이미 돌고 있는 수집기를 전부 내린다 (등록분 + 수동 실행분).
그래서 코드 배포 후엔 stop 없이 install 만 다시 돌리면 된다.

수동 실행(python3 server.py)과 병행하지 말 것 — 세션 30 을 나눠 쓰면 서로 실패한다.
"""
import os
import shutil
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LABEL = "com.findpath.collector"
PY = sys.executable or "/usr/bin/python3"


def run(cmd, ok_fail=False):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0 and not ok_fail:
        sys.exit(f"실패: {' '.join(cmd)}\n{r.stderr.strip()}")
    return r


def follow(path):
    """로그 파일을 tail -f 처럼 따라간다 — 윈도우 포함 순수 파이썬.
    Ctrl-C 는 보기만 끝낸다. 수집은 백그라운드에서 계속 돈다."""
    print(f"로그 스트리밍 (Ctrl-C = 보기만 종료, 수집은 계속): {path}", flush=True)
    try:
        while not os.path.exists(path):
            time.sleep(0.5)
        with open(path, encoding="utf-8", errors="replace") as f:
            # 최근 내용부터: 끝에서 4KB 앞으로 가서 마지막 몇 줄을 먼저 보여준다
            f.seek(0, 2)
            back = min(f.tell(), 4096)
            f.seek(f.tell() - back)
            if back:
                tail = f.read().splitlines()[-10:]
                for l in tail:
                    print(l, flush=True)
            while True:
                line = f.readline()
                if line:
                    print(line, end="", flush=True)
                else:
                    time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n보기 종료 — 수집은 계속 돈다. 다시 보려면: python3 service.py logs", flush=True)


def ensure_pool(workdir):
    """노선 풀이 비어 있으면 fetch_routes 를 먼저 돌린다 — install 전용.

    순서가 핵심이다: 수집기를 시작하기 **전**이라 세션 30 충돌이 없다.
    풀 없이 수집기를 올리면 '노선 풀이 비어 있다'만 반복하며 공회전하고,
    그 상태에서 fetch_routes 를 돌리면 세션을 나눠 써 서로 실패한다 (✅ 실측).
    """
    import sqlite3
    db = os.path.join(workdir, "data", "coverage.sqlite")
    try:
        n = sqlite3.connect(db).execute("SELECT COUNT(*) FROM route").fetchone()[0]
    except sqlite3.Error:
        n = 0
    if n:
        print(f"노선 풀 {n:,}개 확인 — fetch_routes 생략")
        return
    print("노선 풀이 비어 있다 — fetch_routes.py 를 먼저 돌린다 (~10분, 4,400여 콜)")
    r = subprocess.run([PY, "fetch_routes.py"], cwd=workdir)  # 출력 그대로 보여준다
    if r.returncode != 0:
        sys.exit("fetch_routes 실패 — .env 의 GBIS_BUS_KEY 를 확인하고 install 을 다시 돌릴 것")


# install/start 는 **기본으로 콘솔을 잡지 않는다**. 수집은 systemd/launchd 가
# 백그라운드로 돌리므로 스트리밍은 '보기'일 뿐인데, 그게 마지막 줄이라 셸을
# 되찾으려면 Ctrl-C 를 쳐야 했다 — 배포 스크립트에 넣을 수도 없다.
# 옛 동작이 필요하면 `install --logs`.
FOLLOW = False


def maybe_follow(streamer):
    if FOLLOW:
        streamer()
        return
    print("백그라운드에서 돈다 — 셸은 그대로 쓸 수 있다.")
    print("로그: python3 service.py logs   (install --logs 로 바로 이어 볼 수도 있다)")


PROC_PAT = r"python[^ ]* (server|subway_collector|seoul_collector)\.py"


def kill_manual(why="정리"):
    """수동으로 띄운 수집기도 내린다 — 모드와 무관하게 '완전 종료'가 되도록.

    ⚠️ **버스·지하철·서울 세 수집기 모두** 잡는다.
    server.py 만 죽이던 것이 stop 메시지("완전 종료 (버스+지하철)")와 어긋났다 —
    수동으로 띄운 지하철 수집기가 살아남아 다음 install 때 세션·쿼터를 나눠 쓴다.

    죽인 PID 를 **찍는다**. 조용히 죽이면 install 후 중복이 남았는지 알 길이
    없어서, 실제로 nohup 으로 띄운 지하철 수집기가 유닛과 나란히 돌며 jsonl 을
    이중 기록하고 쿼터를 두 배로 태웠다 (✅ 실전).

    SIGTERM → 3초 → SIGKILL. 지금 수집기들엔 TERM 핸들러가 없어 둘 다 즉사하지만
    (jsonl 이 진리라 사이클 단위 commit 로 손실은 그 사이클 하나뿐), TERM 을 먼저
    보내는 건 나중에 핸들러가 생겨도 그대로 맞기 때문이다. 3초 안에 안 죽으면
    승격한다 — TERM 을 무시하는 프로세스에 매달려 install 이 멈추면 안 된다.
    """
    if os.name == "nt":
        # 스케줄러 트리는 schtasks /End 가 내리지만 수동 `py server.py` 는 남는다.
        # 커맨드라인으로 골라 죽인다 (service.py 자신은 이 패턴에 안 걸린다).
        run(["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process | Where-Object "
             "{$_.CommandLine -match '(server|subway_collector|seoul_collector)\\.py'} "
             "| ForEach-Object {Stop-Process -Id $_.ProcessId -Force}"], ok_fail=True)
        return

    def alive():
        # ⚠️ -i 필수 — macOS 프레임워크 파이썬은 프로세스명이 대문자 'Python' 이라
        #    소문자 패턴이 빗나간다 (✅ 실측: stop 이 수동 실행분을 못 내렸다).
        p = subprocess.run(["pgrep", "-if", PROC_PAT], capture_output=True, text=True)
        me = {os.getpid(), os.getppid()}
        return [int(x) for x in p.stdout.split() if x.isdigit() and int(x) not in me]

    pids = alive()
    if not pids:
        return
    print(f"[{why}] 이미 돌고 있는 수집기 {len(pids)}개를 내린다: {' '.join(map(str, pids))}")
    for sig in (signal.SIGTERM, signal.SIGKILL):
        for pid in pids:
            try:
                os.kill(pid, sig)
            except OSError:
                pass
        for _ in range(30):                  # 최대 3초 — commit 할 시간을 준다
            pids = alive()
            if not pids:
                return
            time.sleep(0.1)
    if pids:
        print(f"⚠️ 안 죽는 PID: {' '.join(map(str, pids))} — 수동 확인 필요")


# ── macOS — launchd ──────────────────────────────────────────────────
def mac_workdir():
    """launchd 가 실제로 도는 위치. TCC 보호 폴더(Desktop/Documents) 밑이면 ~/findpath."""
    home = os.path.expanduser("~")
    if HERE.startswith((os.path.join(home, "Desktop"), os.path.join(home, "Documents"))):
        return os.path.join(home, "findpath", "collector")
    return HERE


def mac_run_dir():
    """TCC 보호 폴더 밑이면 ~/findpath 로 코드를 복사해 그쪽을 돌린다.
    launchd 는 보호 폴더를 못 읽는다 (README). data/logs 는 실행본 쪽에 새로 쌓인다."""
    dst = mac_workdir()
    if dst != HERE:
        shutil.copytree(HERE, dst, dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns("data", "logs", "__pycache__", "*.pyc"))
        print(f"TCC 보호 밖으로 복사: {dst}  (코드 수정 후엔 install 재실행으로 재배포)")
    return dst


# 리눅스와 같은 이유로 세 에이전트를 함께 관리한다 (버스 + 지하철 + 서울)
MAC_AGENTS = (
    (LABEL, "server.py", "server"),
    (LABEL + ".subway", "subway_collector.py", "subway"),
    (LABEL + ".seoul", "seoul_collector.py", "seoul"),
)


def mac_plist_path(label=LABEL):
    return os.path.expanduser(f"~/Library/LaunchAgents/{label}.plist")


def mac_target(label=LABEL):
    return f"gui/{os.getuid()}/{label}"


def mac_install():
    d = mac_run_dir()
    os.makedirs(os.path.join(d, "logs"), exist_ok=True)
    for label, _, _ in MAC_AGENTS:
        run(["launchctl", "bootout", mac_target(label)], ok_fail=True)  # 돌던 게 있으면 먼저
    kill_manual("install")   # nohup 등으로 띄운 것도 — 안 그러면 에이전트와 중복으로 돈다
    ensure_pool(d)                                                      # 풀이 비었으면 fetch_routes 부터
    for label, script, logname in MAC_AGENTS:
        with open(mac_plist_path(label), "w") as f:
            f.write(f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>{label}</string>
  <key>ProgramArguments</key><array><string>{PY}</string><string>{script}</string></array>
  <key>WorkingDirectory</key><string>{d}</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>StandardOutPath</key><string>{d}/logs/{logname}.log</string>
  <key>StandardErrorPath</key><string>{d}/logs/{logname}.err</string>
</dict></plist>
""")
        run(["launchctl", "enable", mac_target(label)], ok_fail=True)
        run(["launchctl", "bootstrap", f"gui/{os.getuid()}", mac_plist_path(label)], ok_fail=True)
    print("설치·시작됨 (버스+지하철+서울). 대시보드: http://localhost:877")
    print("⚠️ 지하철은 config.subwayKeys에 등록된 .env 키를 사용한다 "
          "(현재 SEOUL_SUBWAY_KEY~KEY5)")
    maybe_follow(lambda: follow(os.path.join(d, "logs", "server.log")))


def mac_stop():
    for label, _, _ in MAC_AGENTS:
        run(["launchctl", "bootout", mac_target(label)], ok_fail=True)   # 지금 내리고
        run(["launchctl", "disable", mac_target(label)], ok_fail=True)   # 재부팅에도 안 뜨게
    kill_manual()
    print("완전 종료 (버스+지하철+서울). 재부팅해도 안 살아난다 — 다시 켜려면: python3 service.py start")


def mac_start():
    if not os.path.exists(mac_plist_path()):
        sys.exit("등록이 없다 — 먼저: python3 service.py install")
    for label, _, _ in MAC_AGENTS:
        if not os.path.exists(mac_plist_path(label)):
            continue
        run(["launchctl", "enable", mac_target(label)], ok_fail=True)
        run(["launchctl", "bootstrap", f"gui/{os.getuid()}", mac_plist_path(label)], ok_fail=True)
        run(["launchctl", "kickstart", mac_target(label)], ok_fail=True)
    print("시작됨 (버스+지하철+서울).")
    maybe_follow(lambda: follow(os.path.join(mac_workdir(), "logs", "server.log")))


def mac_logs():
    follow(os.path.join(mac_workdir(), "logs", "server.log"))


def mac_uninstall():
    mac_stop()
    for label, _, _ in MAC_AGENTS:
        try:
            os.remove(mac_plist_path(label))
        except OSError:
            pass
    print("등록 제거됨 (버스+지하철).")


def mac_status():
    for label, _, _ in MAC_AGENTS:
        r = run(["launchctl", "print", mac_target(label)], ok_fail=True)
        if r.returncode != 0:
            print(f"{label:<30} 안 떠 있음 (등록 없음이거나 stop 상태)")
        else:
            state = next((l.strip() for l in r.stdout.splitlines() if "state =" in l), "?")
            print(f"{label:<30} {state}")


# ── 리눅스 — systemd (user) ──────────────────────────────────────────
# 유닛 두 개를 한 명령으로 관리한다 — 지하철(§8 #1 존폐 항목)이 수동 설치에
# 의존하면 "대시보드는 안내하는데 install 은 안 만드는" 간극이 생긴다.
#   findpath         = server.py (버스 수집 + 대시보드)
#   findpath-subway  = subway_collector.py (전 노선 도착정보)
LX_UNITS = (
    ("findpath", "server.py --port 8080", "findpath collector (bus + dashboard)"),
    ("findpath-subway", "subway_collector.py", "findpath subway collector"),
    ("findpath-seoul", "seoul_collector.py", "findpath seoul bus arrival snapshot"),
)


def lx_unit_path(name="findpath"):
    return os.path.expanduser(f"~/.config/systemd/user/{name}.service")


def lx_timer_path(name="findpath-routes"):
    return os.path.expanduser(f"~/.config/systemd/user/{name}.timer")


def lx_install():
    os.makedirs(os.path.dirname(lx_unit_path()), exist_ok=True)
    # ⚠️ --port 8080 필수 — 기본 877 은 특권 포트(<1024)라 리눅스 일반 유저는 bind 못 한다
    #    (✅ 실전: EC2 에서 PermissionError 크래시 루프. 손으로 고쳐도 install 재실행이
    #    유닛을 재생성하며 되돌리므로 여기 박아둔다).
    for name, script, desc in LX_UNITS:
        with open(lx_unit_path(name), "w") as f:
            f.write(f"""[Unit]
Description={desc}
[Service]
WorkingDirectory={HERE}
ExecStart={PY} {script}
Restart=always
RestartSec=10
# ⚠️ t4g.micro 는 RAM 903MB 뿐이다. 2026-07-18 에 수집기가 541MB 까지 부풀어
#    커널 OOM 킬러가 두 번 돌았다 (23:15, 23:51). 커널이 고르면 누가 죽을지
#    모르고 — 지하철 수집기나 sshd 가 희생될 수 있다 — 로그도 안 남는다.
#    유닛에 상한을 걸면 그 유닛만 죽고 Restart=always 가 즉시 되살린다.
#    MemoryHigh 에서 먼저 회수 압력을 받으므로 대개 Max 까지 안 간다.
MemoryHigh=320M
MemoryMax=420M
[Install]
WantedBy=default.target
""")
        run(["systemctl", "--user", "stop", name], ok_fail=True)  # 돌던 게 있으면 먼저 내리고
    # 매주 노선·정류장 구조를 새 버전으로 스냅샷한다. 같은 TAGO 세션 풀을 쓰는
    # 경기버스 수집기는 배타적으로 내리고, 성공/실패와 무관하게 다시 올린다.
    with open(lx_unit_path("findpath-routes"), "w") as f:
        f.write(f"""[Unit]
Description=findpath weekly route metadata refresh
[Service]
Type=oneshot
WorkingDirectory={HERE}
ExecStartPre=systemctl --user stop findpath.service
ExecStart={PY} fetch_routes.py
ExecStopPost=systemctl --user start findpath.service
""")
    with open(lx_timer_path(), "w") as f:
        f.write("""[Unit]
Description=findpath weekly route metadata refresh timer
[Timer]
OnCalendar=Sun *-*-* 03:30:00
Persistent=true
RandomizedDelaySec=900
[Install]
WantedBy=timers.target
""")
    kill_manual("install")   # nohup/screen 으로 띄운 것도 — 안 그러면 유닛과 중복으로 돈다
    ensure_pool(HERE)                                             # 풀이 비었으면 fetch_routes 부터
    user = os.environ.get("USER", "")
    run(["loginctl", "enable-linger", user], ok_fail=True)  # 부팅 자동 실행 (로그인 불요)
    run(["systemctl", "--user", "daemon-reload"])
    for name, _, _ in LX_UNITS:
        run(["systemctl", "--user", "enable", "--now", name])
    run(["systemctl", "--user", "enable", "--now", "findpath-routes.timer"])
    print("설치·시작됨 (버스+지하철+서울). 대시보드: http://localhost:8080 (리눅스는 877이 특권 포트라 8080)")
    print("⚠️ 지하철은 config.subwayKeys에 등록된 .env 키를 사용한다 "
          "(현재 SEOUL_SUBWAY_KEY~KEY5) — 없는 키는 로그에 표시하고 건너뛴다")
    maybe_follow(lx_logs)


def lx_stop():
    for name, _, _ in LX_UNITS:
        run(["systemctl", "--user", "disable", "--now", name], ok_fail=True)
    run(["systemctl", "--user", "disable", "--now", "findpath-routes.timer"], ok_fail=True)
    kill_manual()
    print("완전 종료 (버스+지하철+서울). 재부팅해도 안 살아난다 — 다시 켜려면: python3 service.py start")


def lx_start():
    for name, _, _ in LX_UNITS:
        run(["systemctl", "--user", "enable", "--now", name], ok_fail=True)
    print("시작됨 (버스+지하철+서울).")
    maybe_follow(lx_logs)


def lx_logs():
    print("로그 스트리밍 — 버스+지하철+서울 (Ctrl-C = 보기만 종료, 수집은 계속)", flush=True)
    try:
        args = ["journalctl", "--user", "-f", "-n", "10"]
        for name, _, _ in LX_UNITS:
            args += ["-u", name]
        subprocess.run(args)
    except KeyboardInterrupt:
        print("\n보기 종료 — 수집은 계속 돈다. 다시 보려면: python3 service.py logs", flush=True)


def lx_uninstall():
    lx_stop()
    for name, _, _ in LX_UNITS:
        try:
            os.remove(lx_unit_path(name))
        except OSError:
            pass
    run(["systemctl", "--user", "daemon-reload"], ok_fail=True)
    print("등록 제거됨 (버스+지하철).")


def lx_status():
    for name, _, _ in LX_UNITS:
        r = run(["systemctl", "--user", "is-active", name], ok_fail=True)
        print(f"{name:<18} {r.stdout.strip() or '없음'}")


# ── 윈도우 — 작업 스케줄러 + 감시 루프 배치 ─────────────────────────
TASK = "findpath-collector"


def win_bat_path():
    return os.path.join(HERE, "run_forever.bat")


def win_install():
    # ⚠️ --log 로 파일에도 남긴다 (서버 내부 tee) — 죽은 뒤 사인을 보기 위함.
    #    셸 리다이렉트(>>)로 하면 스케줄러가 띄우는 cmd 창이 텅 비어 버린다 (✅ 실전):
    #    창에는 그대로 흐르고 파일에는 복사본이 남아야 한다.
    # 지하철은 별도 창에서 같이 띄운다 — 리눅스의 두 유닛과 같은 구성.
    # start "" 로 새 창을 열어 각자 감시 루프를 돌린다 (한쪽이 죽어도 다른 쪽 유지).
    with open(os.path.join(HERE, "run_subway.bat"), "w") as f:
        f.write(f"""@echo off
cd /d "{HERE}"
:loop
"{PY}" subway_collector.py
timeout /t 30
goto loop
""")
    with open(win_bat_path(), "w") as f:
        f.write(f"""@echo off
cd /d "{HERE}"
start "findpath-subway" cmd /c run_subway.bat
:loop
"{PY}" server.py --log logs\\server.log
timeout /t 10
goto loop
""")
    run(["schtasks", "/End", "/TN", TASK], ok_fail=True)  # 돌던 게 있으면 먼저 내리고
    kill_manual("install")   # 직접 띄운 창도 — 안 그러면 스케줄러 분과 중복으로 돈다
    ensure_pool(HERE)                                     # 풀이 비었으면 fetch_routes 부터
    # ONSTART 등록은 관리자 권한 필요 — 실패하면 안내
    r = run(["schtasks", "/Create", "/F", "/TN", TASK, "/TR", win_bat_path(),
             "/SC", "ONSTART", "/RL", "HIGHEST"], ok_fail=True)
    if r.returncode != 0:
        sys.exit("schtasks 등록 실패 — 관리자 PowerShell 에서 다시 실행할 것.\n" + r.stderr.strip())
    run(["schtasks", "/Run", "/TN", TASK])
    print("설치·시작됨 — 로그는 새로 뜬 cmd 창에 흐른다 (파일 복사본: logs\\server.log)")
    print("창을 닫았거나 안 보이면: py service.py logs")


def win_stop():
    run(["schtasks", "/End", "/TN", TASK], ok_fail=True)               # 지금 내리고
    run(["schtasks", "/Change", "/TN", TASK, "/Disable"], ok_fail=True)  # 재부팅에도 안 뜨게
    kill_manual()                                                        # 수동 실행분도
    print("완전 종료. 재부팅해도 안 살아난다 — 다시 켜려면: python3 service.py start")


def win_start():
    run(["schtasks", "/Change", "/TN", TASK, "/Enable"], ok_fail=True)
    run(["schtasks", "/Run", "/TN", TASK])
    print("시작됨 — 로그는 새로 뜬 cmd 창에 흐른다 (파일 복사본: logs\\server.log)")
    print("창을 닫았거나 안 보이면: py service.py logs")


def win_logs():
    follow(os.path.join(HERE, "logs", "server.log"))


def win_uninstall():
    run(["schtasks", "/End", "/TN", TASK], ok_fail=True)
    run(["schtasks", "/Delete", "/F", "/TN", TASK], ok_fail=True)
    print("등록 제거됨.")


def win_status():
    r = run(["schtasks", "/Query", "/TN", TASK], ok_fail=True)
    print(r.stdout.strip() or "등록 없음")


# ── 진입점 ───────────────────────────────────────────────────────────
def main():
    global FOLLOW
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    FOLLOW = any(a in ("--logs", "-f", "--follow") for a in sys.argv[1:])
    cmd = args[0] if args else "status"
    osname = ("win" if os.name == "nt"
              else "mac" if sys.platform == "darwin" else "lx")
    table = {
        ("mac", "install"): mac_install, ("mac", "stop"): mac_stop,
        ("mac", "start"): mac_start, ("mac", "uninstall"): mac_uninstall,
        ("mac", "status"): mac_status, ("mac", "logs"): mac_logs,
        ("lx", "install"): lx_install, ("lx", "stop"): lx_stop,
        ("lx", "start"): lx_start, ("lx", "uninstall"): lx_uninstall,
        ("lx", "status"): lx_status, ("lx", "logs"): lx_logs,
        ("win", "install"): win_install, ("win", "stop"): win_stop,
        ("win", "start"): win_start, ("win", "uninstall"): win_uninstall,
        ("win", "status"): win_status, ("win", "logs"): win_logs,
    }
    fn = table.get((osname, cmd))
    if not fn:
        sys.exit(f"모르는 명령: {cmd}  (install | start | stop | logs | uninstall | status)")
    fn()


if __name__ == "__main__":
    main()
