# 수집기 — ⚠️ 여기 있는 건 원본이고, 도는 건 다른 곳이다

**실제로 실행되는 위치: `~/findpath-collector/`**

```
~/findpath-collector/
  bus_collector.py       ← launchd 가 이걸 돌린다
  subway_collector.py
  .env                   (600, GBIS_BUS_KEY · SEOUL_SUBWAY_KEY)
  data/                  ← 수집 결과가 여기 쌓인다
  logs/
```

## 왜 Desktop 밖에 있나

`~/Desktop` 은 **macOS TCC 보호 폴더**다. 터미널에서 직접 실행하면 되지만, **`launchd` 로 뜬 프로세스는 접근 권한을 물려받지 못한다:**

```
python3: can't open file '.../Desktop/.../bus_collector.py': [Errno 1] Operation not permitted
```

`python3` 에 전체 디스크 접근 권한을 주는 방법도 있으나 범위가 너무 넓어 택하지 않았다. **Desktop 밖으로 옮기는 쪽이 권한 부여가 전혀 필요 없다.**

## ⚠️ 여기서 고치면 반영되지 않는다

이 폴더는 **원본(편집용)**이고 `~/findpath-collector/` 는 **복사본(실행용)**이다. 수정했으면 배포해야 한다:

```bash
cp bus_collector.py subway_collector.py ~/findpath-collector/
launchctl kickstart -k gui/$(id -u)/com.findpath.bus-collector
```

## 제어

```bash
# 상태 (2번째 열이 exit code — '-' 면 실행 중)
launchctl list | grep findpath

# 로그
tail -f ~/findpath-collector/logs/bus.log
cat ~/findpath-collector/logs/bus.err

# 끄기
launchctl unload ~/Library/LaunchAgents/com.findpath.bus-collector.plist

# 켜기
launchctl load ~/Library/LaunchAgents/com.findpath.bus-collector.plist

# 아주 지우기
launchctl unload ~/Library/LaunchAgents/com.findpath.bus-collector.plist
rm ~/Library/LaunchAgents/com.findpath.bus-collector.plist
```

## 한계 — 맥이 깨어 있어야 한다

`launchd` 가 재부팅·로그아웃·크래시에는 살아남지만, **맥이 잠들거나 네트워크가 끊기면 그동안은 수집되지 않는다.** 노트북을 덮으면 멈춘다.

- **버스**: 공백이 그대로 손실이다. 배차간격 운영이라 "그 차"가 매일 다르기 때문.
- **지하철**: 덜 아프다. `trainNo` 가 매일 같은 시각표로 돌아 **다른 날 관측을 겹치면 메워진다** (docs §3.3.3).

## 쿼터 (초과하면 그날 수집이 죽는다)

일 호출수는 `data/.buscalls-{운행일}` · `data/.calls-{운행일}` 에 **파일로** 남는다. `KeepAlive` 로 재시작돼도 카운터가 리셋되지 않게 하기 위함이다.

| | 한도 | 현재 사용 |
|---|---|---|
| 버스위치정보 (운영계정) | 50만/일 · 30 TPS | 성남 80노선 = **약 13만/일 (27%)** |
| 서울 지하철 실시간 | **1,000/일** | 신분당선 1노선 72초 = 딱 한계 |

지하철은 1,000/일이라 **19개 노선 상시 폴링(45,600/일)이 불가능**하다. 갤러리 등록이 필요하고, 그건 배포된 실물이 있어야 한다 (docs §3.3.2).

## 이 데이터는 무엇을 위한 것인가

- **버스** (`bus-31020-*.jsonl`) — `stop_times` 재료 (docs §4.4, §5). **C 아키텍처용이다.** 현재 채택된 B+ 는 TMAP `sectionTime` 을 쓰므로 필요 없다. C 를 되살릴 선택지를 살려두기 위한 수집.
- **지하철** (`shinbundang-*.jsonl`) — **정시성 검증**용 (docs §8 #1). 시각표 복원용이 아니다 — 신분당선 계획 시각표는 운영사 PDF 로 이미 있다 (docs §3.3.1).

## 기록 형식

**버스** — 정류장을 넘은 순간만 기록한다. 통과 시각은 `(t_prev, t]` 안에 있다 (타임스탬프 필드가 API 에 없어서 폴링 시각으로만 좁힐 수 있음, docs §3.1):

```json
{"t":"...","t_prev":"...","routeid":"GGB...","routeno":"9000","routetp":"직행좌석버스",
 "vehicleno":"경기77바3714","from_ord":31,"to_ord":32,"nodeid":"...","nodenm":"순천향대학병원"}
```

**지하철** — 열차 상태가 바뀔 때만 기록.
