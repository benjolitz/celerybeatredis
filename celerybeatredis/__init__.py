from __future__ import absolute_import

from .task import PeriodicTask, Crontab, Interval
from .schedulers import RedisScheduler, RedisScheduleEntry


def patch_redlock():
    from redlock import Redlock

    def touch(self, lock, ttl):
        if not isinstance(ttl, float):
            ttl = float(ttl)
        new_ttle = int(ttl/1000) or 10
        key = lock.resource
        for server in self.servers:
            try:
                server.expire(key, new_ttle)
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
