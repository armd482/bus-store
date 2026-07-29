"""표준 라이브러리만 사용하는 키별 HTTPS Keep-Alive 연결 풀."""

import gzip
import http.client
import queue
import threading


_STALE_CONNECTION_ERRORS = (
    http.client.RemoteDisconnected,
    http.client.CannotSendRequest,
    http.client.ResponseNotReady,
    BrokenPipeError,
    ConnectionResetError,
    ConnectionAbortedError,
)


class HTTPSPoolTimeout(TimeoutError):
    """정해진 시간 안에 사용 가능한 연결을 얻지 못했다."""


class _Bucket:
    def __init__(self, maximum):
        self.maximum = maximum
        self.idle = queue.LifoQueue(maximum)
        self.created = 0


class KeyedHTTPSPool:
    """인증키별로 연결 수를 제한하면서 HTTP/1.1 소켓을 재사용한다.

    응답 본문을 모두 읽은 뒤 ``HTTPResponse.will_close``가 거짓일 때만 연결을
    풀에 돌려놓는다. 서버가 Keep-Alive를 지원하지 않거나 네트워크 예외가 나면
    해당 연결만 폐기하고 다음 요청에서 새로 만든다.
    """

    def __init__(self, host, max_per_key, timeout=15, connection_factory=None,
                 retry_reserver=None):
        self.host = host
        self.max_per_key = max(1, int(max_per_key))
        self.timeout = float(timeout)
        self._factory = connection_factory or (
            lambda host, timeout: http.client.HTTPSConnection(
                host, timeout=timeout))
        self._retry_reserver = retry_reserver
        self._buckets = {}
        self._lock = threading.Lock()
        self._stats = {
            "requests": 0, "created": 0, "reused": 0, "closed": 0,
            "stale_retries": 0,
        }

    def _bucket(self, key):
        with self._lock:
            return self._buckets.setdefault(key, _Bucket(self.max_per_key))

    def _acquire(self, key):
        bucket = self._bucket(key)
        try:
            conn = bucket.idle.get_nowait()
        except queue.Empty:
            with self._lock:
                if bucket.created < bucket.maximum:
                    bucket.created += 1
                    self._stats["created"] += 1
                    try:
                        conn = self._factory(self.host, self.timeout)
                    except Exception:
                        bucket.created -= 1
                        self._stats["closed"] += 1
                        raise
                    return bucket, conn, False
            try:
                conn = bucket.idle.get(timeout=self.timeout)
            except queue.Empty as exc:
                raise HTTPSPoolTimeout("HTTPS connection pool exhausted") from exc
        with self._lock:
            self._stats["reused"] += 1
        return bucket, conn, True

    def _release(self, bucket, conn, reusable):
        if reusable:
            try:
                bucket.idle.put_nowait(conn)
                return
            except queue.Full:
                pass
        try:
            conn.close()
        finally:
            with self._lock:
                bucket.created -= 1
                self._stats["closed"] += 1

    def get(self, key, path, headers=None):
        request_headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "Connection": "keep-alive",
            "User-Agent": "findpath-collector/1",
        }
        request_headers.update(headers or {})
        for attempt in range(2):
            bucket, conn, reused = self._acquire(key)
            response_started = False
            reusable = False
            try:
                conn.request("GET", path, headers=request_headers)
                response = conn.getresponse()
                response_started = True
                data = response.read()
                reusable = not response.will_close
                if response.getheader("Content-Encoding", "").lower() == "gzip":
                    data = gzip.decompress(data)
                with self._lock:
                    self._stats["requests"] += 1
                return response.status, data
            except _STALE_CONNECTION_ERRORS:
                # 서버가 idle Keep-Alive를 이미 닫은 경우다. GET이고 아직 응답
                # 첫 바이트도 받지 않았으므로 새 소켓으로 한 번만 즉시 재전송한다.
                if reused and not response_started and attempt == 0:
                    # 호출 쿼터 장부도 물리 재전송 1회를 먼저 예약한다. 예약자가
                    # 상한을 거부하면 원래 transport 오류를 그대로 반환한다.
                    if (self._retry_reserver is not None
                            and not self._retry_reserver(key)):
                        raise
                    with self._lock:
                        self._stats["stale_retries"] += 1
                    continue
                raise
            finally:
                self._release(bucket, conn, reusable)

    def stats(self):
        with self._lock:
            return dict(self._stats)

    def close(self):
        with self._lock:
            buckets = list(self._buckets.values())
        for bucket in buckets:
            while True:
                try:
                    conn = bucket.idle.get_nowait()
                except queue.Empty:
                    break
                self._release(bucket, conn, False)
