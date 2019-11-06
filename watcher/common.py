import sys

from redislite import Redis


from ..simple_redis_queue import SimpleRedisQueue


__version__ = "0.0.1"

DEFAULT = dict({
    "HOST": "localhost",
    "PORT": 8080,
    "RHOST": "localhost",
    "RPORT": 6379,
    "RDB": 0,
    "POLL_INTERVALL":1
})

DEFAULT.update({
    "REDIS" : Redis(DEFAULT.get("RHOST"), DEFAULT.get("RPORT"), db=DEFAULT.get("RDB"))
})



class QueueWatcher:

    def __init__(self, name, redis=DEFAULT['REDIS']):
        self.name = name
        self._queue = SimpleRedisQueue(name=name, redis_instance=redis)

    def __len__(self):
        return sum(self._queue.n_in_progress())


def signal_handler(sig, frame):
    print("Detected interrup, EXITING ... ", file=sys.stderr)
    sys.exit(0)