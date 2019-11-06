# -*- coding: utf-8 -*-

"""It is a Python library that allows you to use Redis as a message queue
within your Python programs.
Heavily influenced by :
    - HotQueue : https://github.com/richardhenry/hotqueue
    - Yarqueue : https://github.com/clbarnes/yarqueue
"""
from functools import wraps
from datetime import datetime
import logging
from queue import Empty
import time
import uuid


class QueueTimeoutError(Exception):
    pass

class QueueEmptyError(Empty):
    pass


def _ensure_redis(redis):
    if redis:
        return redis

    try:
        import redislite
        return redislite.Redis('./tmp/redis.db')
    except ImportError:
        raise ValueError(
            "Redis instance not given and redislite not importable. Run at least\n"
            "pip install redislite"
        )


def setup_logger_lever(name, level):
    """[summary]
    
    Arguments:
        name {[type]} -- [description]
        level {[type]} -- [description]
    """
    name = name.upper()
    method = name.lower()

    def log_for_level(self, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            self.__logger(level, msg, *args, **kwargs)

    def log_to_root(msg, *args, **kwargs):
        logging.log(level, msg, *args, **kwargs)

    logging.addLevelName(level, name)
    setattr(logging, name, level)
    setattr(logging.getLoggerClass(), method, log_for_level)
    setattr(logging, method, log_to_root)


setup_logger_lever("HIGHEST_IN_THE_ROOM", 5)



class SimpleRedisQueue(object):
    """Simple FIFO Queue with Redis List as backend message brocker

    Example :
    >>> from simple-redis-queue import SimpleRedisQueue as RedQueue
    >>> queue = RedQueue("my-task-queue", host="localhost", port=6379, db=1)
    
    :param name: Name to add assigned to the queue. By defautl, it will be an UUID4
    :param namespace: The name to use as prefix for the queue. By default, it's "QUEUE"
    :param serializer: The class or module to use to serialize task msg. 
    It MUST HAVE the following methods or functions :
        - ``dumps``
        - ``loads``
    See the following library class for more details : `pickle <http://docs.python.org/library/pickle.html>`
    :param redis_kwargs: Extra kwargs to pass to :class `Redis`, mostly 
        :attr:`host`, :attr:`port`, :attr:`db`

    Additionally, contains an ``n_tasks()`` method exposing the number of items put onto
    the queue without ``task_done()`` being called for them, and an ``n_in_progress()``
    method to count how many have been fetched from the queue with ``task_done()`` being
    called.
    """

    def __init__(self, name=None, namespace="QUEUE", serializer=None, redis_instance=None, **redis_kwargs):
        """The default connection parameters are: host='localhost', port=6379, db=0"""
        self.__name = name or str(uuid.uuid4())
        self.__serializer = serializer
        self.__redis = _ensure_redis(redis_instance)
        self.__logger = logging.getLogger("{}.{}".format(__name__, type(self).__name__))
        self.__task_counter_key =  self.__name + '__task-counter'
        self.key = "{}:{}".format(namespace.upper(), name)

    def __len__(self):
        return self.qsize()


    @property
    def redis_instance(self):
        return self.__redis

    def qsize(self):
        """Return the approximate size of the queue.
        """
        return self.__redis.llen(self.key)

    def is_empty(self):
        """Return ``True`` if the queue is empty, ``False`` otherwise."""
        return self.qsize() == 0

    def clear(self):
        """Clear the queue items in the queue by deleting the Redis key
        """
        self.__redis.delete(self.key)
        self.__redis.delete(self.__task_counter_key)


    def put(self, *items):
        """Put one or many item into the queue.
        Example:

        >>> queue.put({msg:"Another queue item", created_at:1572972413, status:WAITING})
        >>> queue.put("My queue item")

        It is also to bulk post items into the queue, which can be significantly faster if you have a large number of items :
        
        >>> queue.put("MY first bulk item", ["my", "second", "bulk", "item"], {msg:"Another queue bulk item", created_at:1572972413, status:WAITING})

        """
        if self.__serializer is not None:
            items = list(map(self.__serializer.dumps, items))
        self.__redis.rpush(self.key, *items)
        self.__redis.incrby(self.__task_counter_key, len(items))

    def consume(self, limit=None, **kwargs):
        """Return a generator that yields whenever an item is waiting in the queue. 
        Otherwise, will bock

        Example:
        >>> for item in queue.consume(timeout=1):
                print(item)
        my first item to DO
        ... 
        The third remaining todo

        :param limi: Maximum number of items to retrieve. By default, set to ``None``, i.e. infinite)
        :param kwargs: any acceptable args for :meth:`simple-redis-queue.SimpleRedisQueue.get`
        """
        limit = limit or float("inf")
        count = 0
        while count < limit:
            try:
                item = self.get(**kwargs)
                if item is None:
                    break
            except Empty:
                break
            except KeyboardInterrupt:
                print()
                return
            yield item
            count += 1


    def get(self, block=False, timeout=None):
        """Retrieve an item from the queue and remove it. Example:

        >>> queue.get()
        "My Queue item"
        >>> queue.get()
        {msg:"Another queue item", created_at:1572972413, status:WAITING}

        :param block: Whether or not to wait until an item is available in the queue before returning, set by default to ``False``.
            Will raise ``queue.Empty`` exception if no item is available
        :param timeout: When using :attr:`block`, if no item is availble for :attr:`timeout`in seconds, raise ``queue.Empty`` exception.

        If :attr:`block` is ``True`` and timeout is None (the default), block
        if necessary until an item is available.
        """
        if block:
            timeout = timeout or 0
            item = self.__redis.blpop(self.key, timeout=timeout)
            if item is None:
                raise QueueEmptyError("Redis queue {} was empty after {}sec".format(self.key, timeout))
            else:
                item = item[1]
        else:
            item = self.__redis.lpop(self.key)
            if item is None:
                raise QueueEmptyError("Redis queue {} was empty after {}sec".format(self.key, timeout))
        if item is not None and self.__serializer is not None:
            item = self.__serializer.loads(item)
        if isinstance(item, bytes):
            item = item.decode()
        return item

    def get_wait(self):
        """Equivalent to get(block=True).
        """
        return self.get(True)


    def n_tasks(self):
        """How many items have been put into the queue without a respective amount of ``task_done()`` call
        """
        return int(self.__redis.get(self.__task_counter_key))


    def n_in_progress(self):
        """How many items have been retrieved from the queue without a respective amount of ``task_done()`` being called for them
        """
        return self.n_tasks() - self.qsize()

    
    def task_done(self):
        """Indicates hat formely enqueued task is completed

        Used by queue consummers.
        For each ``get()``used to fetch a task, a subsquent call to ``task_done()``tells the queue that the processing on the task is completed.

        If a ``join``is currently blocking, it will resume when all items have been processesd (meaning that a ``task_done()``call was received for every time that had been ``put``into the queue).

        Raises a ``ValueError``if called more times than there were items placed in the queue. 
        """
        if(self.qsize() == 0):
            raise ValueError("No more item available in the queue")
        return self.__redis.decr(self.__task_counter_key)



    def worker(self, *args, **kwargs):
        """Decorator for using a function as a queue worker. Example:
        
        >>> @queue.worker(timeout=1)
        ... def printer(item):
        ...     print item
        >>> printer()
        my item
        another item
        
        You can also use it without passing any keyword arguments:
        
        >>> @queue.worker
        ... def printer(item):
        ...     print item
        >>> printer()
        my message
        another message
        
        :param kwargs: any arguments that :meth:`~simple-redis-queue.SimpleRedisQueue.get` can
            accept (:attr:`block` will default to ``True`` if not given)
        """

        def decorator(worker):
            @wraps(worker)
            def wrapper(*args):
                for item in self.consume(**kwargs):
                    worker(*args + (item,))

            return wrapper

        if args:
            return decorator(*args)
        return decorator



    def wait(self, timeout= None):
        """
        Block until all items in the queue have been retrived adnd processed.

        The count of unfinished tasks increases whenever an item is added into the queue.
        The count decreases whenever a consummer calls ``task_done()``to indicate that
        the item was retrieved and all work on it is complete.
        When the count of unfinished tasks drops to 0, ``wait``unblocks. 
        
        Arguments:
            timeout {Float} -- timeout in seconds. 
                Raise ``QueueTimeoutError`` when the timeout is reached.
        """
        timeout = timeout or float("inf")
        started_at = datetime.utcnow()
        n_tasks = self.n_tasks()
        while self.n_tasks() > 0:
            sleep_time = "now"
            if timeout is not float("inf"):
                sleep_time = "for {} sec".format(timeout)
            self.__logger.info("{} tasks remaining, sleeping {} ... ".format(n_tasks, sleep_time))

            time.sleep(0.5)

            elapsed = datetime.utcnow() - started_at
            if elapsed.total_seconds() > timeout:
                raise QueueTimeoutError("Queue waiting timed out")

            n_tasks = self.n_tasks()

        self.__logger.debug("Waited successfully")
    def join(self):
        """Like ``wait``, but without a timeout
        """
        return self.wait()


    def __enter__(self):
        return self
    
    def __exit__(self, _type, value, traceback):
        self.wait(3) # Wait for 3 seconds before exit
        self.clear()



if __name__ == "__main__":
    import json

    q = SimpleRedisQueue("quick-test", serializer=json)
    q.put("Yello mock World")
    q.put("Another yello item")
    print(q.get(), q.qsize())
