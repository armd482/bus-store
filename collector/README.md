# 수집기 운영

버스 위치를 폴링해 **정류장 통과 시각**을 쌓는다. 무엇을 왜 모으는지는 [`../docs/collector-design.md`](../docs/collector-design.md), 설계 전체는 [`../docs/transit-routing-gtfs.md`](../docs/transit-routing-gtfs.md).

## 실행

**파이썬 3.7+ 만 있으면 된다.** 표준 라이브러리만 쓰므로 `pip install` 이 없고, SQLite 는 파이썬에 내장이다.

```bash
cp .env.example .env      # 키를 채운다. 발급처는 .env.example 안에.
python3 fetch_routes.py   # 노선 풀 — 경기 2,200노선, 약 6분. 처음 한 번만.
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

### 리눅스 / EC2 — 권장

가장 단순하다. TCC 도 절전도 강제 재부팅도 없다.

```bash
python3 server.py
```

상시 실행은 systemd. `~/.config/systemd/user/findpath.service`:

```ini
[Unit]
Description=findpath collector
[Service]
WorkingDirectory=/home/ubuntu/find-path/collector
ExecStart=/usr/bin/python3 server.py
Restart=always
[Install]
WantedBy=default.target
```

```bash
systemctl --user enable --now findpath
systemctl --user status findpath
journalctl --user -u findpath -f    # 로그
```

### macOS

```bash
python3 server.py
```

⚠️ **`~/Desktop` · `~/Documents` 아래에 두면 상시 실행이 안 된다.** 이 둘은 TCC 보호 폴더라 터미널에서 직접 실행은 되지만 **`launchd` 로 뜬 프로세스는 권한을 물려받지 못한다:**

```
python3: can't open file '.../Desktop/.../server.py': [Errno 1] Operation not permitted
```

`python3` 에 전체 디스크 접근 권한을 주는 방법도 있으나 범위가 너무 넓다. **홈 밑 보호되지 않는 폴더(`~/findpath/` 등)로 옮기는 쪽이 권한 부여가 전혀 필요 없다.**

그리고 **잠들면 멈춘다.** 노트북을 덮으면 그 시간대는 0 이 된다 (아래 "한계").

### 윈도우

```powershell
py fetch_routes.py
py server.py
```

상시 실행은 **작업 스케줄러** — "사용자 로그온 여부에 관계없이 실행", 트리거 "시스템 시작 시". 전원 옵션에서 절전을 꺼야 한다.

✅ **인코딩은 처리돼 있다.** 윈도우 콘솔 기본이 cp949 라 로그의 한글·이모지에서 `UnicodeEncodeError` 가 나는데 — 하필 `⚠️ 실패` 줄은 API 가 실패할 때만 타는 경로라 **잘 돌다가 첫 실패에 통째로 죽는다.** `orchestrator.py` 가 import 시점에 stdout 을 UTF-8 로 고정한다. 모든 스크립트가 그 모듈을 import 하므로 별도 조치가 필요 없다.

## 상태 보기

`http://localhost:877`

| | |
|---|---|
| **완성률** | 목표 셀 대비 채운 셀. ETA 는 가정이 아니라 **실측 관측률로 역산**한다 |
| **밴드별** | 7개 시간대. **`⚠ 관측 없음` 이 그 시간에 기계가 꺼져 있었다는 유일한 신호다** |
| **요일별** | 토·일 분리 |
| **건강** | 쿼터 · **실패를 종류별로** · 마지막 관측 시각(= 살아 있는지) |

JSON 이 필요하면 `curl -s localhost:877/api`. 터미널만 있으면 `python3 orchestrator.py status`.

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
| 버스위치정보 (TAGO 운영) | **50만/일** | 170노선 폴링 = 약 40만/일 |
| 서울 지하철 실시간 | **1,000/일** | 신분당선 1노선 72초 = 딱 한계 |

⚠️ **한도는 두 개고 서로 다르다** (docs §3.1):

- **일 50만** — 위 카운터가 지킨다.
- **동시 세션 30** — `rate × 응답시간` 이다. 응답이 중앙값 2.5s 인데 **꼬리가 5s 로 튀므로**, 평균으로 맞추면 꼬리에서 넘긴다. 넘기면 **HTTP 200 + `resultCode 99`** 로 오므로 `resultCode` 를 안 보면 조용히 버린다.

지하철은 1,000/일이라 **19개 노선 상시 폴링(45,600/일)이 불가능**하다. 갤러리 등록이 필요하고, 그건 배포된 실물이 있어야 한다 (docs §3.3.2).

## 한계 — 기계가 깨어 있어야 한다

공백은 무작위 손실이 아니라 **구조적 구멍**이고, 하필 **출근·퇴근 첨두**가 노트북이 덮여 있는 시간이다 ([collector-design.md §1.2](../docs/collector-design.md)).

- **버스**: 공백이 그대로 손실이다. 배차간격 운영이라 "그 차"가 매일 다르다.
- **지하철**: 덜 아프다. `trainNo` 가 매일 같은 시각표로 돌아 **다른 날 관측을 겹치면 메워진다** (docs §3.3.3).

## 이 데이터는 무엇을 위한 것인가

- **버스** (`bus-{운행일}.jsonl`) — `stop_times` 재료 (docs §4.4, §5). 채택된 B+ 의 **동작에는 필요 없다.** 버스 차별점(중간 환승 절벽)과 C 를 되살릴 선택지를 위한 수집이다 (docs §6.4.1 판단 이력).
- **지하철** (`shinbundang-{운행일}.jsonl`) — **정시성 검증**용 (docs §8 #1). 시각표 복원용이 아니다 — 신분당선 계획 시각표는 운영사 PDF 로 이미 있다 (docs §3.3.1).

파일은 **운행일 기준으로 나뉜다** (04시 경계). 01:30 버스는 전날 파일로 간다 — 벽시계로 나누면 막차가 두 파일로 찢어진다.

## 기록 형식

정류장을 **넘은 순간만** 기록한다. 통과 시각은 `(t_prev, t]` 안에 있다 — API 에 타임스탬프 필드가 없어 폴링 시각으로만 좁힐 수 있다 (docs §3.1). ✅ 30초 폴링이면 이동의 94.7% 를 1칸으로 잡는다.

```json
{"t":"...","t_prev":"...","routeid":"GGB...","routeno":"9000","routetp":"직행좌석버스",
 "cityCode":31020,"vehicleno":"경기77바3714","from_ord":31,"to_ord":32,
 "nodeid":"...","nodenm":"...","gpslati":37.4,"gpslong":127.1,
 "band":2,"daytype":"weekday"}
```

⚠️ `gpslati`/`gpslong` 은 **버스 GPS 가 아니라 현재 정류장의 좌표**다. 정류장을 넘을 때만 바뀐다 (docs §3.1). 우리가 필요한 건 통과 시각이라 이걸로 충분하다.
