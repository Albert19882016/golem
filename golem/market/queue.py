import logging
import queue
from typing import List, Dict, Any

from twisted.internet import task, defer
from twisted.internet.defer import Deferred

from golem.core.common import each

logger = logging.getLogger(__name__)


class MarketQueue:

    __instance = None

    def __new__(cls):
        if not cls.__instance:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    def __init__(self):
        logger.debug('Creating new market queue')
        from twisted.internet import reactor

        self._reactor = reactor
        self._queues: Dict[str, queue.Queue] = dict()

    def __getitem__(self, key: str) -> queue.Queue:
        if key not in self:
            self._queues[key] = queue.Queue()
        return self._queues[key]

    def __delitem__(self, key: str):
        del self._queues[key]

    def __contains__(self, key: str):
        return key in self._queues

    def drain_after(self, key: str, timeout: float) -> Deferred:
        if key not in self._queues:
            return defer.fail(KeyError(key))
        return task.deferLater(self._reactor, timeout, self.drain, key)

    def drain(self, key: str) -> List[Any]:
        elements: List[Any] = []
        q = self[key]

        try:
            while True:
                elements.append(q.get(block=False))
        except queue.Empty:
            pass
        return elements

    def fill(self, key: str, items: List[Any]):
        each(self[key].put, items)
