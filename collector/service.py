#!/usr/bin/env python3
"""수집기 서비스 관리 — 설치 / 시작 / 완전 종료 / 제거를 OS 공통 명령으로 (macOS·리눅스·윈도우).

  python3 service.py install     상시 실행 등록 + 시작 (크래시·재부팅 자동 재시작)
  python3 service.py stop        ★ 완전 종료 — 프로세스 중지 + 자동 재시작·부팅 실행 해제.
                                 start 하기 전까지 재부팅해도 안 살아난다.
  python3 service.py start       다시 켜기 (등록은 유지돼 있던 것)
  python3 service.py uninstall   등록 자체를 제거
  python3 service.py status      상태 확인

수동 실행(python3 server.py + Ctrl-C)과 병행하지 말 것 — 세션 30 을 나눠 쓰면 서로 실패한다.
stop 은 수동으로 띄운 server.py 도 함께 내린다 (어느 모드든 '완전 종료' 한 명령).
"""
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
LABEL = "com.findpath.collector"
PY = sys.executable or "/usr/bin/python3"


def run(cmd, ok_fail=False):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0 and not ok_fail:
        sys.exit(f"실패: {' '.join(cmd)}\n{r.stderr.strip()}")
    return r


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


def kill_manual():
    """수동으로 띄운 server.py 도 내린다 — 모드와 무관하게 '완전 종료'가 되도록."""
    if os.name == "nt":
        return  # 윈도우는 스케줄러 /End 가 배치 트리를 내린다
    # ⚠️ -i 필수 — macOS 프레임워크 파이썬은 프로세스명이 대문자 'Python' 이라
    #    소문자 패턴이 빗나간다 (✅ 실측: stop 이 수동 실행분을 못 내렸다).
    run(["pkill", "-if", r"python[^ ]* server\.py"], ok_fail=True)


# ── macOS — launchd ──────────────────────────────────────────────────
def mac_run_dir():
    """TCC 보호 폴더(Desktop/Documents) 밑이면 ~/findpath 로 코드를 복사해 그쪽을 돌린다.
    launchd 는 보호 폴더를 못 읽는다 (README). data/logs 는 실행본 쪽에 새로 쌓인다."""
    home = os.path.expanduser("~")
    if not HERE.startswith((os.path.join(home, "Desktop"), os.path.join(home, "Documents"))):
        return HERE
    dst = os.path.join(home, "findpath", "collector")
    shutil.copytree(HERE, dst, dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("data", "logs", "__pycache__", "*.pyc"))
    print(f"TCC 보호 밖으로 복사: {dst}  (코드 수정 후엔 install 재실행으로 재배포)")
    return dst


def mac_plist_path():
    return os.path.expanduser(f"~/Library/LaunchAgents/{LABEL}.plist")


def mac_target():
    return f"gui/{os.getuid()}/{LABEL}"


def mac_install():
    d = mac_run_dir()
    os.makedirs(os.path.join(d, "logs"), exist_ok=True)
    run(["launchctl", "bootout", mac_target()], ok_fail=True)  # 돌던 게 있으면 먼저 내리고
    ensure_pool(d)                                             # 풀이 비었으면 fetch_routes 부터
    plist = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>{LABEL}</string>
  <key>ProgramArguments</key><array><string>{PY}</string><string>server.py</string></array>
  <key>WorkingDirectory</key><string>{d}</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>StandardOutPath</key><string>{d}/logs/server.log</string>
  <key>StandardErrorPath</key><string>{d}/logs/server.err</string>
</dict></plist>
"""
    with open(mac_plist_path(), "w") as f:
        f.write(plist)
    run(["launchctl", "enable", mac_target()], ok_fail=True)
    run(["launchctl", "bootout", mac_target()], ok_fail=True)  # 이미 떠 있으면 교체
    run(["launchctl", "bootstrap", f"gui/{os.getuid()}", mac_plist_path()])
    print(f"설치·시작됨. 로그: {d}/logs/server.log · 대시보드: http://localhost:877")


def mac_stop():
    run(["launchctl", "bootout", mac_target()], ok_fail=True)   # 지금 내리고
    run(["launchctl", "disable", mac_target()], ok_fail=True)   # 재부팅에도 안 뜨게
    kill_manual()
    print("완전 종료. 재부팅해도 안 살아난다 — 다시 켜려면: python3 service.py start")


def mac_start():
    if not os.path.exists(mac_plist_path()):
        sys.exit("등록이 없다 — 먼저: python3 service.py install")
    run(["launchctl", "enable", mac_target()], ok_fail=True)
    run(["launchctl", "bootstrap", f"gui/{os.getuid()}", mac_plist_path()], ok_fail=True)
    run(["launchctl", "kickstart", mac_target()], ok_fail=True)
    print("시작됨.")


def mac_uninstall():
    mac_stop()
    try:
        os.remove(mac_plist_path())
    except OSError:
        pass
    print("등록 제거됨.")


def mac_status():
    r = run(["launchctl", "print", mac_target()], ok_fail=True)
    if r.returncode != 0:
        print("에이전트: 안 떠 있음 (등록 없음이거나 stop 상태)")
    else:
        state = next((l.strip() for l in r.stdout.splitlines() if "state =" in l), "?")
        print(f"에이전트: {state}")


# ── 리눅스 — systemd (user) ──────────────────────────────────────────
def lx_unit_path():
    return os.path.expanduser("~/.config/systemd/user/findpath.service")


def lx_install():
    os.makedirs(os.path.dirname(lx_unit_path()), exist_ok=True)
    with open(lx_unit_path(), "w") as f:
        f.write(f"""[Unit]
Description=findpath collector
[Service]
WorkingDirectory={HERE}
ExecStart={PY} server.py
Restart=always
RestartSec=10
[Install]
WantedBy=default.target
""")
    run(["systemctl", "--user", "stop", "findpath"], ok_fail=True)  # 돌던 게 있으면 먼저 내리고
    ensure_pool(HERE)                                               # 풀이 비었으면 fetch_routes 부터
    user = os.environ.get("USER", "")
    run(["loginctl", "enable-linger", user], ok_fail=True)  # 부팅 자동 실행 (로그인 불요)
    run(["systemctl", "--user", "daemon-reload"])
    run(["systemctl", "--user", "enable", "--now", "findpath"])
    print("설치·시작됨. 로그: journalctl --user -u findpath -f")


def lx_stop():
    run(["systemctl", "--user", "disable", "--now", "findpath"], ok_fail=True)
    kill_manual()
    print("완전 종료. 재부팅해도 안 살아난다 — 다시 켜려면: python3 service.py start")


def lx_start():
    run(["systemctl", "--user", "enable", "--now", "findpath"])
    print("시작됨.")


def lx_uninstall():
    lx_stop()
    try:
        os.remove(lx_unit_path())
    except OSError:
        pass
    run(["systemctl", "--user", "daemon-reload"], ok_fail=True)
    print("등록 제거됨.")


def lx_status():
    r = run(["systemctl", "--user", "is-active", "findpath"], ok_fail=True)
    print(f"서비스: {r.stdout.strip() or '없음'}")


# ── 윈도우 — 작업 스케줄러 + 감시 루프 배치 ─────────────────────────
TASK = "findpath-collector"


def win_bat_path():
    return os.path.join(HERE, "run_forever.bat")


def win_install():
    # ⚠️ 로그 리다이렉트 필수 — 스케줄러로 뜬 배치는 콘솔이 없어서, 리다이렉트가
    #    없으면 수집기가 죽으며 남긴 트레이스백을 볼 방법이 없다 (✅ 실전에서 겪음).
    with open(win_bat_path(), "w") as f:
        f.write(f"""@echo off
cd /d "{HERE}"
if not exist logs mkdir logs
:loop
"{PY}" server.py >> logs\\server.log 2>&1
timeout /t 10
goto loop
""")
    run(["schtasks", "/End", "/TN", TASK], ok_fail=True)  # 돌던 게 있으면 먼저 내리고
    ensure_pool(HERE)                                     # 풀이 비었으면 fetch_routes 부터
    # ONSTART 등록은 관리자 권한 필요 — 실패하면 안내
    r = run(["schtasks", "/Create", "/F", "/TN", TASK, "/TR", win_bat_path(),
             "/SC", "ONSTART", "/RL", "HIGHEST"], ok_fail=True)
    if r.returncode != 0:
        sys.exit("schtasks 등록 실패 — 관리자 PowerShell 에서 다시 실행할 것.\n" + r.stderr.strip())
    run(["schtasks", "/Run", "/TN", TASK])
    print("설치·시작됨.")


def win_stop():
    run(["schtasks", "/End", "/TN", TASK], ok_fail=True)               # 지금 내리고
    run(["schtasks", "/Change", "/TN", TASK, "/Disable"], ok_fail=True)  # 재부팅에도 안 뜨게
    print("완전 종료. 재부팅해도 안 살아난다 — 다시 켜려면: python3 service.py start")


def win_start():
    run(["schtasks", "/Change", "/TN", TASK, "/Enable"], ok_fail=True)
    run(["schtasks", "/Run", "/TN", TASK])
    print("시작됨.")


def win_uninstall():
    run(["schtasks", "/End", "/TN", TASK], ok_fail=True)
    run(["schtasks", "/Delete", "/F", "/TN", TASK], ok_fail=True)
    print("등록 제거됨.")


def win_status():
    r = run(["schtasks", "/Query", "/TN", TASK], ok_fail=True)
    print(r.stdout.strip() or "등록 없음")


# ── 진입점 ───────────────────────────────────────────────────────────
def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    osname = ("win" if os.name == "nt"
              else "mac" if sys.platform == "darwin" else "lx")
    table = {
        ("mac", "install"): mac_install, ("mac", "stop"): mac_stop,
        ("mac", "start"): mac_start, ("mac", "uninstall"): mac_uninstall,
        ("mac", "status"): mac_status,
        ("lx", "install"): lx_install, ("lx", "stop"): lx_stop,
        ("lx", "start"): lx_start, ("lx", "uninstall"): lx_uninstall,
        ("lx", "status"): lx_status,
        ("win", "install"): win_install, ("win", "stop"): win_stop,
        ("win", "start"): win_start, ("win", "uninstall"): win_uninstall,
        ("win", "status"): win_status,
    }
    fn = table.get((osname, cmd))
    if not fn:
        sys.exit(f"모르는 명령: {cmd}  (install | start | stop | uninstall | status)")
    fn()


if __name__ == "__main__":
    main()
