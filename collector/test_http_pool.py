import gzip
import unittest

from http_pool import KeyedHTTPSPool


class FakeResponse:
    def __init__(self, body=b"{}", status=200, will_close=False,
                 encoding=""):
        self.body = body
        self.status = status
        self.will_close = will_close
        self.encoding = encoding

    def read(self):
        return self.body

    def getheader(self, name, default=None):
        if name.lower() == "content-encoding":
            return self.encoding
        return default


class FakeConnection:
    def __init__(self, responses):
        self.responses = responses
        self.requests = 0
        self.closed = False

    def request(self, method, path, headers=None):
        self.requests += 1
        self.last = (method, path, headers)

    def getresponse(self):
        return self.responses.pop(0)

    def close(self):
        self.closed = True


class HTTPSPoolTests(unittest.TestCase):
    def make_pool(self, responses, maximum=1):
        made = []

        def factory(_host, _timeout):
            conn = FakeConnection(responses)
            made.append(conn)
            return conn

        pool = KeyedHTTPSPool(
            "example.test", maximum, timeout=0.1,
            connection_factory=factory)
        self.addCleanup(pool.close)
        return pool, made

    def test_reuses_keepalive_connection_for_same_key(self):
        pool, made = self.make_pool([
            FakeResponse(b"one"), FakeResponse(b"two")])
        self.assertEqual(pool.get("K1", "/one"), (200, b"one"))
        self.assertEqual(pool.get("K1", "/two"), (200, b"two"))
        self.assertEqual(len(made), 1)
        self.assertEqual(made[0].requests, 2)
        self.assertEqual(
            pool.stats(),
            {"requests": 2, "created": 1, "reused": 1, "closed": 0})

    def test_server_close_discards_connection(self):
        pool, made = self.make_pool([
            FakeResponse(b"one", will_close=True),
            FakeResponse(b"two", will_close=True)])
        pool.get("K1", "/one")
        pool.get("K1", "/two")
        self.assertEqual(len(made), 2)
        self.assertTrue(all(conn.closed for conn in made))
        self.assertEqual(pool.stats()["closed"], 2)

    def test_keys_have_separate_connection_limits(self):
        pool, made = self.make_pool([
            FakeResponse(b"one"), FakeResponse(b"two")])
        pool.get("K1", "/one")
        pool.get("K2", "/two")
        self.assertEqual(len(made), 2)

    def test_gzip_response_is_decoded(self):
        body = gzip.compress(b'{"ok":true}')
        pool, _made = self.make_pool([
            FakeResponse(body, encoding="gzip")])
        self.assertEqual(pool.get("K1", "/gzip")[1], b'{"ok":true}')
