# 수집기 운영

버스 위치를 폴링해 **정류장 통과 시각**을 쌓는다. 무엇을 왜 모으는지는 [`../docs/collector-design.md`](../docs/collector-design.md), 설계 전체는 [`../docs/transit-routing-gtfs.md`](../docs/transit-routing-gtfs.md).

## 실행

**파이썬 3.7+ 만 있으면 된다.** 표준 라이브러리만 쓰므로 `pip install` 이 없고, SQLite 는 파이썬에 내장이다.

```bash
cp .env.example .env      # 키를 채운다. 발급처는 .env.example 안에.
python3 fetch_routes.py   # 노선 풀 — 경기 2,200노선(정류소 수 + 운행시간), 약 10분. 처음 한 번만.
python3 server.py         # 수집 + 대시보드 → http://localhost:877
```

`fetch_routes.py` 를 **먼저** 돌려야 한다. 노선 풀이 커버리지의 분모라, 비어 있으면 폴링할 대상이 없다.

⚠️ **`fetch_routes.py` 를 돌릴 땐 수집기를 멈출 것.** 동시 세션 30 을 둘이 나눠 쓰면 서로 실패한다.

| 옵션 | |
|---|---|
| `--port 8080` | 포트 변경 |
| `--no-collect` | 대시보드만 — 수집 없이 상태만 볼 때 |

## OS 별

코드는 셋 다 돈다. 다른 건 **항상 켜두는 방법**이다.

**세 OS 공통 관리 명령** — 아래 OS 별 절차를 직접 밟는 대신 이걸 쓰면 된다:

```bash
python3 service.py install     # 상시 실행 등록 + 시작 (크래시·재부팅 자동 재시작)
python3 service.py stop        # ★ 완전 종료 — 중지 + 자동 재시작·부팅 실행 해제
python3 service.py start       # 다시 켜기
python3 service.py uninstall   # 등록 제거
python3 service.py status
```

`stop` 이 **완전 종료**다: 지금 내려가고, 크래시 감시도 풀리고, 재부팅해도 안 살아난다 — `start` 하기 전까지. 수동으로 띄운 `server.py` 가 있어도 함께 내린다. (macOS 에서 `install` 은 TCC 보호 폴더 밖 `~/findpath/` 로 코드를 알아서 복사한다 — 아래 macOS 절 참조.)

`install` 은 **노선 풀이 비어 있으면 `fetch_routes.py` 를 알아서 먼저 돌린다** (~10분) — 수집기 시작 전이라 세션 충돌이 없다. 즉 새 기계에선 `.env` 만 채우고 `install` 한 방이면 된다.

아래는 `service.py` 가 하는 일의 수동 절차다 — 직접 제어하고 싶을 때만.

### 리눅스 / EC2 — 권장

가장 단순하다. TCC 도 절전도 강제 재부팅도 없다.

```bash
python3 server.py
```

상시 실행은 systemd — 크래시 자동 재시작 + 부팅 자동 시작. **사용자가 `stop` 한 경우에만** 꺼진 채로 있는다. `~/.config/systemd/user/findpath.service`:

```ini
[Unit]
Description=findpath collector
[Service]
WorkingDirectory=/home/ubuntu/bus-store/collector
ExecStart=/usr/bin/python3 server.py --port 8080
Restart=always
RestartSec=10
[Install]
WantedBy=default.target
```

⚠️ **`--port 8080` 필수** — 기본 877 은 특권 포트(<1024)라 리눅스 일반 유저는 bind 못 한다 (✅ 실전: `PermissionError` 크래시 루프). `service.py install` 이 유닛을 만들 때도 8080 을 박는다. 대시보드는 `http://localhost:8080`.

```bash
loginctl enable-linger $(whoami)    # ⚠️ 필수 — 없으면 SSH 로그아웃 때 유저 서비스가 같이 죽는다
systemctl --user enable --now findpath
systemctl --user status findpath
journalctl --user -u findpath -f    # 로그

systemctl --user stop findpath      # 수동 종료 — 이때만 자동 재시작이 멈춘다 (Ctrl-C 에 해당)
systemctl --user restart findpath   # 코드 고친 뒤 반영
```

**백업 (EC2 + 구글드라이브):** `config.json` 에 두 값을 넣으면 **수집기가 직접** 업로드·검증한다 (크론 불필요):

```json
"exportDir":  "/home/ubuntu/bus-store/collector/findpath-export",
"driveRemote": "gdrive:busdata"
```

2단 로테이션 (매일 04시):
- **어제** → gzip 백업본을 exportDir 로 만들고 드라이브에 즉시 업로드. 원본 jsonl 은 로컬 유지 (ETA 계산용)
- **그저께 이전** → 원본 삭제 **단 드라이브에 백업이 있는지 rclone 으로 확인 후**. 없으면 재업로드 시도, 그래도 실패하면 원본을 그대로 둔다(로그 `[삭제 보류]`). → 삭제 전 하루의 유예로 수동 백업 확인이 가능하다.

`driveRemote` 를 비우면(윈도우 OneDrive 등) 드라이브 확인 대신 **로컬 .gz 존재로만** 판단하고, 업로드는 동기화앱/크론에 맡긴다. rclone 크론을 쓰고 싶으면:

```
30 4 * * * rclone copy /home/ubuntu/bus-store/collector/findpath-export gdrive:busdata --include "*.jsonl.gz" --log-file /home/ubuntu/rclone.log
```

⚠️ 크론은 반드시 **`copy`** (move 아님) — 로컬 .gz 를 지우면 `rebuild` 가 과거를 못 본다. .gz 전체가 15주 ~1.6GB 라 로컬 보관 부담이 없다.

### macOS

```bash
python3 server.py        # 개발 — 터미널에서 직접. Ctrl-C 로 종료
```

**상시 실행 (크래시·재부팅 자동 재시작)** — `launchd` 를 쓴다. 크래시하든 재부팅하든 알아서 다시 뜨고, **사용자가 명시적으로 내린 경우에만**(아래 bootout — 터미널의 Ctrl-C 에 해당) 꺼진 채로 있는다:

```bash
# 1. TCC 보호 밖으로 복사본을 둔다 (아래 ⚠️ 참조)
rsync -a --exclude data --exclude logs ~/Desktop/assignment/find-path/collector/ ~/findpath/collector/
mkdir -p ~/findpath/collector/logs

# 2. plist 의 USERNAME 치환 후 설치
sed "s/USERNAME/$(whoami)/g" ~/findpath/collector/com.findpath.collector.plist \
  > ~/Library/LaunchAgents/com.findpath.collector.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.findpath.collector.plist

# 상태 확인 (2번째 열이 '-' 면 실행 중)
launchctl list | grep findpath
tail -f ~/findpath/collector/logs/server.log

# 수동 종료 — 이때만 자동 재시작이 멈춘다 (Ctrl-C 에 해당)
launchctl bootout gui/$(id -u)/com.findpath.collector

# 다시 켜기
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.findpath.collector.plist
```

죽었다 되살아나도 안전하다 — 일 콜 카운터는 디스크(`.buscalls-*`)라 쿼터를 다시 세지 않고, 관측 장부는 사이클 단위 커밋이다.

⚠️ **`~/Desktop` · `~/Documents` 아래를 plist 가 가리키면 안 된다.** 이 둘은 TCC 보호 폴더라 터미널에서 직접 실행은 되지만 **`launchd` 로 뜬 프로세스는 권한을 물려받지 못한다:**

```
python3: can't open file '.../Desktop/.../server.py': [Errno 1] Operation not permitted
```

`python3` 에 전체 디스크 접근 권한을 주는 방법도 있으나 범위가 너무 넓다. **홈 밑 보호되지 않는 폴더(`~/findpath/`)로 복사하는 쪽이 권한 부여가 전혀 필요 없다** (위 rsync). 코드를 고치면 rsync 를 다시 돌리고 `launchctl kickstart -k gui/$(id -u)/com.findpath.collector`.

그리고 **잠들면 멈춘다.** launchd 도 잠은 못 깨운다 — 노트북을 덮으면 그 시간대는 0 이 된다 (아래 "한계"). 덮어도 계속 돌리려면 전원 연결 + `sudo pmset -c sleep 0` 또는 상시 기계(EC2)로.

### 윈도우

```powershell
py fetch_routes.py
py server.py
```

상시 실행은 **작업 스케줄러 + 감시 루프 배치**. 작업 스케줄러의 자체 "실패 시 다시 시작"은 재시도 횟수에 상한이 있어서, 크래시 자동 재시작은 배치 루프가 맡는 쪽이 확실하다. `run_forever.bat`:

```bat
:loop
py server.py
timeout /t 10
goto loop
```

- **부팅 자동 시작**: 작업 스케줄러에서 이 배치를 등록 — 트리거 "시스템 시작 시", "사용자 로그온 여부에 관계없이 실행"
- **크래시**: 배치 루프가 10초 뒤 재시작 (쿼터 카운터는 디스크라 이어서 센다)
- **수동 종료** (Ctrl-C 에 해당): 콘솔에서 Ctrl-C → "배치 작업을 종료하시겠습니까?" 에 Y. 스케줄러로 띄운 경우엔 작업 스케줄러에서 "끝내기"
- 전원 옵션에서 절전을 꺼야 한다 (잠들면 그 시간대는 0)

✅ **인코딩은 처리돼 있다.** 윈도우 콘솔 기본이 cp949 라 로그의 한글·이모지에서 `UnicodeEncodeError` 가 나는데 — 하필 `⚠️ 실패` 줄은 API 가 실패할 때만 타는 경로라 **잘 돌다가 첫 실패에 통째로 죽는다.** `orchestrator.py` 가 import 시점에 stdout 을 UTF-8 로 고정한다. 모든 스크립트가 그 모듈을 import 하므로 별도 조치가 필요 없다.

## 상태 보기

`http://localhost:877` (리눅스 서비스 설치 시 **8080**)

| | |
|---|---|
| **완성률** | 목표 셀(구간 × 밴드 7 × 요일 7) 대비 충족 셀. ETA 는 최근 1~2 운행일 실측 관측률로 **밴드별 천장 도달 범위**를 계산한다 |
| **밴드별** | 지금 채워지는 요일의 **누적** (모든 주 합산 · 04시에 다음 요일로 전환) · 현재 밴드 `진행 중` 표시 |
| **요일별** | **7종 전부 분리** — 요일마다 완성률·충족 셀·관측 수. 각 요일은 주 1일씩만 채워진다 |
| **건강** | 쿼터 · 실패율(종류별) · 마지막 관측(1초 타이머 + `데이터 요청 중`) |
| **오류 로그** | 재시도 후 잔여 실패만 — 시각·규모·원인(HTTP429/code99/…) 최근 50건 |

JSON 이 필요하면 `curl -s localhost:8080/api`. 터미널만 있으면 `python3 orchestrator.py status`.

## 제어

```bash
# 백그라운드로 띄우기 (터미널을 닫아도 유지)
nohup python3 server.py > /tmp/run.log 2>&1 &

# 멈추기
pkill -f server.py

# 로그
tail -f /tmp/run.log
```

## 쿼터 — 초과하면 그날 수집이 죽는다

일 호출수는 `data/.buscalls-{운행일}` 에 **파일로** 남는다. 프로세스가 재시작돼도 카운터가 리셋되지 않게 하기 위함이다.

| | 한도 | 현재 |
|---|---|---|
| 버스위치정보 (TAGO 운영) | **50만/일** | 170노선 폴링 ≈ **44~46만/일** (✅ 실측 — 재시도 포함) |
| 서울 지하철 실시간 | **키당 1,000/일** | 전 노선 일괄(ALL) 1회 = 3콜 — 키 3개면 76초 간격 (키가 적으면 간격 자동 연장) |

카운터는 **달력일** 키다 (data.go.kr 자정 리셋과 일치). 데이터 파일은 운행일(04시) — 둘은 일부러 다르다: 돈은 자정에, 데이터는 04시에 넘어간다. ⚠️ `fetch_routes.py` 의 ~4,400콜은 카운터에 안 잡히므로 사용량이 상한 근처인 날엔 자정 직후로 미룰 것.

⚠️ **한도는 두 개고 서로 다르다** (docs §3.1):

- **일 50만** — 위 카운터가 지킨다.
- **동시 세션 30** — `rate × 응답시간` 이다. 응답이 중앙값 2.5s 인데 **꼬리가 5s 로 튀므로**, 평균으로 맞추면 꼬리에서 넘긴다. 넘기면 **HTTP 200 + `resultCode 99`** 로 오므로 `resultCode` 를 안 보면 조용히 버린다.

지하철은 노선별 위치 폴링이면 19노선 45,600콜/일이라 불가능했지만, **도착 일괄(`realtimeStationArrival/ALL`)은 1콜에 전 노선(19개·555역, 경기 포함)이 온다** — 키 2~3개 라운드로빈이면 전 노선 상시 수집이 된다 (config `subwayKeys`). 더 촘촘히 가려면 갤러리 등록(무제한)이고, 그건 배포된 실물이 있어야 한다 (docs §3.3.2).

## 한계 — 기계가 깨어 있어야 한다

공백은 무작위 손실이 아니라 **구조적 구멍**이고, 하필 **출근·퇴근 첨두**가 노트북이 덮여 있는 시간이다 ([collector-design.md §1.2](../docs/collector-design.md)).

- **버스**: 공백이 그대로 손실이다. 배차간격 운영이라 "그 차"가 매일 다르다.
- **지하철**: 덜 아프다. `trainNo` 가 매일 같은 시각표로 돌아 **다른 날 관측을 겹치면 메워진다** (docs §3.3.3).

## 이 데이터는 무엇을 위한 것인가

- **버스** (`bus-{운행일}.jsonl`) — `stop_times` 재료 (docs §4.4, §5). 채택된 B+ 의 **동작에는 필요 없다.** 버스 차별점(중간 환승 절벽)과 C 를 되살릴 선택지를 위한 수집이다 (docs §6.4.1 판단 이력).
  - ⚠️ **분석 규칙**: 요일 셀의 7샘플은 대개 그 요일 **하루** 안에 차는 단일 날짜 표본이다 — **요일 간 비교엔 같은 요일 날짜 ≥ 2 인 셀만** 쓸 것 (날짜 수는 jsonl 의 `t` 로 센다). 밴드 평균(주행시간 모델)엔 그대로 써도 된다. 근거·규칙 전체는 docs §4.4 "분석 규칙".
- **지하철** (`subway-{운행일}.jsonl` — 전 노선 일괄) — **정시성 검증**용 (docs §8 #1). 시각표 복원용이 아니다 — 신분당선 계획 시각표는 운영사 PDF 로 이미 있다 (docs §3.3.1).

파일은 **운행일 기준으로 나뉜다** (04시 경계). 01:30 버스는 전날 파일로 간다 — 벽시계로 나누면 막차가 두 파일로 찢어진다.

## 기록 형식

정류장을 **넘은 순간만** 기록한다. 통과 시각은 `(t_prev, t]` 안에 있다 — API 에 타임스탬프 필드가 없어 폴링 시각으로만 좁힐 수 있다 (docs §3.1). ✅ 30초 폴링이면 이동의 94.7% 를 1칸으로 잡는다.

```json
{"t":"...","t_prev":"...","routeid":"GGB...","vehicleno":"경기77바3714",
 "from_ord":31,"to_ord":32,"nodeid":"...","band":2,"daytype":"fri"}
```

**최소 필드만 저장한다** — `vehicleno` 는 같은 차의 통과를 이어 붙여 구간 소요시간을 만드는 체인 키라 필수고, `band`/`daytype` 은 장부 재계산(`rebuild`)용이다. 노선번호·유형·정류장명·좌표는 `coverage.sqlite` 의 `route` 테이블에서 `routeid` 로 조인한다 — 행마다 반복 저장하지 않는다 (행 381B → ~220B).

⚠️ API 의 `gpslati`/`gpslong` 은 **버스 GPS 가 아니라 현재 정류장의 좌표**다 (docs §3.1). 통과 시각만 필요하므로 저장하지 않는다.

**장부 계상 규칙 두 가지** (jsonl 은 전이 그대로, 장부만 다르게 센다):

- **인접 구간 분해** — 2칸 이상 건너뛴 전이(5.2%)는 (31,33) 같은 비인접 셀이 아니라 사이의 **인접 구간 각각에 1관측**으로 센다. 비인접 셀로 넣으면 분모(인접 구간수)와 어긋나고, 정류장 간격이 짧아 늘 건너뛰어지는 구간은 영영 미충족으로 남는다.
- **공휴일 제외** — config `holidays`(운행일 기준)의 날짜는 평상 다이어가 아니라 요일 표본을 오염시키므로 장부에 안 넣는다. jsonl 엔 남으니 목록을 고치고 `rebuild` 하면 재분류된다.
