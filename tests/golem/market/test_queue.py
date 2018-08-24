import uuid
from unittest import TestCase, mock

from twisted.internet.task import Clock

from golem.core.common import each
from golem.market.queue import MarketQueue


class TestMarketQueue(TestCase):

    @mock.patch('twisted.internet.reactor', create=True)
    def test_magic_methods(self, _):
        q = MarketQueue()
        k = str(uuid.uuid4())

        assert k not in q
        assert q[k]
        assert k in q
        del q[k]
        assert k not in q

    @mock.patch('twisted.internet.reactor', create=True)
    def test_fill_and_drain(self, _):
        q = MarketQueue()
        k = str(uuid.uuid4())
        data = list(range(1, 11))

        q.fill(k, data)
        assert q.drain(k) == data
        assert not q.drain(k)

    @mock.patch('twisted.internet.reactor', new=Clock(), create=True)
    def test_drain_after(self):
        q = MarketQueue()
        k = str(uuid.uuid4())
        data = list(range(1, 11))
        result = []

        def callback(r):
            each(result.append, r)

        q.fill(k, data)
        deferred = q.drain_after(k, 3.50)
        deferred.addCallback(callback)

        q._reactor.advance(10)

        assert deferred.called
        assert result == data
