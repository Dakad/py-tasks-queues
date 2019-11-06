import signal
import sys
import time

from itertools import zip_longest

from redislite import Redis

try:
    from tqdm import tqdm
except ImportError:
    raise ImportError("tqdm not importable; CLI watcher not available : pip install tqdm")

try:
    import click
except ImportError:
    raise ImportError("click not importable; CLI watcher not available : pip install click")


from .common import __version__
from .common import DEFAULT, QueueWatcher, signal_handler


class TqdmQueueWatcher:

    def __init__(self, watcher, total=None):
        self._watcher = watcher
        current_len = len(self._watcher)
        self.total = total or current_len
        self.previous_done = self.total - current_len

        self.progres_bar = tqdm(desc=watcher.name, total=self.total)
        self.progres_bar.update(self.previous_done)

    def n_done(self):
        return self.total - len(self._watcher)

    def update(self):
        done = self.n_done()
        diff = done - self.previous_done
        self.previous_done = done
        self.progres_bar.update(diff)

    def close(self):
        self.progres_bar.close()

    def __enter__(self):
        return self

    def __exit__(self, _type, value, traceback):
        self.close()


class MultiTqdmQueueWatcher:

    def __init__(self,redis, totals_by_name):
        self._tqdm_watchers = []
        for (name, total) in totals_by_name.items():
            self._tqdm_watchers.append(
                TqdmQueueWatcher(QueueWatcher(name, redis), total)
            )

    def __iter__(self):
        yield from self._tqdm_watchers

    def update(self):
        for tqw in self:
            tqw.update()


    def close(self):
        for tqw in self:
            tqw.close()

    def __enter__(self):
        return self

    def __exit__(self, _type, value, traceback):
        self.close()


def main(redis, names_totals, interval=1):
    signal.signal(signal.SIGINT, signal_handler)
    print("Press Ctrl+C to exit", file=sys.stderr)

    with MultiTqdmQueueWatcher(redis, names_totals) as multitQWatcher:
        while True:
            multitQWatcher.update()
            time.sleep(interval)


@click.command(
    help="Watch the progress of a number of redis-backed queues, on the command line."
)
@click.version_option(version=__version__)
@click.help_option()
@click.option(
    "--name",
    "-n",
    multiple=True,
    help="Name of redis lists to watch (accepts multiple)",
)
@click.option(
    "--total",
    "-t",
    multiple=True,
    type=int,
    help="Total items added to the queue (accepts multiple, same order as --name)",
)
@click.option(
    "--interval",
    "-i",
    default=DEFAULT['INTERVAL'],
    type=float,
    help="Polling interval (seconds)",
    show_default=True,
)
@click.option(
    "--host",
    default=DEFAULT['RHOST'],
    help="Hostname for the Redis instance",
    show_default=True,
)
@click.option(
    "--port",
    default=DEFAULT['RPORT'],
    type=int,
    help="Port for the Redis instance",
    show_default=True,
)
@click.option(
    "--db",
    default=DEFAULT['DB'],
    type=int,
    help="DB ID for the Redis instance",
    show_default=True,
)
@click.option("--password", help="Password for the Redis instance", show_default=True)
def q_watcher(name, total, interval, host, port, db, password):
    redis = Redis(host, port, db, password)
    names_totals = dict(zip_longest(name, total))
    main(redis, names_totals, interval)


if __name__ == "__main__":
    if not click:
        raise ImportError("click is not installed; q_watcher unavailable")
    if not tqdm:
        raise ImportError("tqdm is not installed; q_watcher unavailable")
    q_watcher()