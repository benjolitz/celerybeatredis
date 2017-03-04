from __future__ import absolute_import

from .task import PeriodicTask, Crontab, Interval
from .schedulers import RedisScheduler, RedisScheduleEntry


def patch_redlock():
    from redlock import Redlock

    def touch(self, lock, ttl):
        key = lock.resource
        for server in self.servers:
            try:
                server.expire(key, int(ttl/1000))
            except:
                pass

    Redlock.touch = touch

patch_redlock()

__all__ = [
    'PeriodicTask',
    'Crontab',
    'Interval'
    'RedisScheduler',
    'RedisScheduleEntry'
]
