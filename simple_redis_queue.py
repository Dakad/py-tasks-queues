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
import time
import uuid

import redis


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


def key_for_name(name, namespace="QUEUE"):
    """Get an specific name used to store for the given queue name in Redis.
    
    Arguments:
        name {[type]} -- [description]
    """
    name = name or str(uuid.uuid4())
    return "%s:%s" % (namespace, name)


class SimpleRedisQueue(object):
    """Simple FIFO Queue with Redis list as backend message broker
    
    """

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
    """

    def __init__(self, name=None, namespace="QUEUE", serializer=None, **redis_kwargs):
        """The default connection parameters are: host='localhost', port=6379, db=0"""
        self.__name = name
        self.__serializer = serializer
        self.__db = redis.Redis(**redis_kwargs)
        self.__logger = logging.getLogger("{}.{}".format(__name__, type(self).__name__))
        self.key = key_for_name(name, namespace)

    def __len__(self):
        return self.qsize()

    def qsize(self):
        """Return the approximate size of the queue.
        """
        return self.__db.llen(self.key)

    def empty(self):
        """Return ``True`` if the queue is empty, ``False`` otherwise."""
        return self.qsize() == 0

    def clear(self):
        """Clear the queue items in the queue by deleting the Redis key
        """
        return self.__db.delete(self.key)

    def put(self, *items):
        """Put one or many item into the queue.
        Example:

        >>> queue.put({msg:"Another queue item", created_at:1572972413, status:WAITING})
        >>> queue.put("My queue item")

        It is also to bulk post items into the queue, which can be significantly faster if you have a large number of items :
        
        >>> queue.put("MY first bulk item", ["my", "second", "bulk", "item"], {msg:"Another queue bulk item", created_at:1572972413, status:WAITING})

        """
        if self.__serializer is not None:
            items = map(self.__serializer.dumps, items)
        self.__db.rpush(self.key, *items)

    def consume(self, **kwargs):
        """Return a generator that yields whenever an item is waiting in the queue. 
        Otherwise, will bock

        Example:
        >>> for item in queue.consume(timeout=1):
                print(item)
        my first item to DO
        ... 
        The third remaining todo

        :param kwargs: any acceptable args for :meth:`simple-redis-queue.SimpleRedisQueue.get`
        """
        try:
            while True:
                item = self.get(**kwargs)
                if item is None:
                    break
                yield item
        except KeyboardInterrupt:
            print
            return


    def get(self, block=False, timeout=None):
        """Retrieve an item from the queue and remove it. Example:

        >>> queue.get()
        "My Queue item"
        >>> queue.get()
        {msg:"Another queue item", created_at:1572972413, status:WAITING}

        :param block: Whether or not to wait until an item is available in the queue before returning, set by default to ``False``
        :param timeout: When using :attr:`block`, if no item is availble for :attr:`timeout`in seconds, give up and return ``None``

        If :attr:`block` is ``True`` and timeout is None (the default), block
        if necessary until an item is available.
        """
        if block:
            timeout = timeout or 0

            item = self.__db.blpop(self.key, timeout=timeout)
            if item is not None:
                item = item[1]
        else:
            item = self.__db.lpop(self.key)
        if item is not None and self.__serializer is not None:
            item = self.__serializer.loads(item)
        return item

    def get_wait(self):
        """Equivalent to get(block=True).
        """
        return self.get(True)

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


if __name__ == "__main__":
    import json

    q = SimpleRedisQueue("quick-test", serializer=json)
    q.put("Yello mock World")
    q.put("Another yello item")
    print(q.get(), q.qsize())
