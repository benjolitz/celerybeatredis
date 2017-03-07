# -*- coding: utf-8 -*-
# Copyright 2014 Kong Luoxing

# Licensed under the Apache License, Version 2.0 (the 'License'); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at http://www.apache.org/licenses/LICENSE-2.0
import contextlib
import datetime
import functools
import logging
import threading
import time

import celery.schedules
import kombu.utils
import redis.exceptions
from celery import current_app
from celery.beat import Scheduler
from celery.result import ResultBase
from redis import StrictRedis
from redlock import Redlock, MultipleRedlockException, Lock

from .exceptions import TaskTypeError
from .task import PeriodicTask, catch_errors


logger = logging.getLogger(__name__)

# we don't need simplejson, builtin json module is good enough


class EmptyResult(ResultBase):
    '''
    Done to convince celery to not freak out
    '''
    task = 'Not a task'
    id = 'faked id'


@catch_errors
def lock_task_until(name, dlm, lock, t_s, db, result):
    logger.info('Starting singleton task thread tracking {!r} using a redis lock {} ({})'.format(
        name, lock.resource, lock.key))
    try:
        while not result.ready():
            logger.info('Refresh token {!r}'.format(lock.resource))
            dlm.touch(lock, 4*60*1000)
            time.sleep(4*60)

    except Exception:
        logger.exception('Error in {}'.format(name))
    finally:
        logger.info('Callback on {}. Took {:.2f}s'.format(name, time.time() - t_s))
        try:
            logger.info('Releasing Task {} lock'.format(name))
            time.sleep(300)
            dlm.unlock(lock)
        except Exception:
            logger.exception('Unable to release lock on behalf of {!r}'.format(name))

    try:
        result.get()
    except Exception:
        logger.exception('Error in {}'.format(name))


class RedisScheduleEntry(object):
    """
    The Schedule Entry class is mainly here to handle the celery dependency injection
     and delegates everything to a PeriodicTask instance
    It follows the Adapter Design pattern, trivially implemented in python with __getattr__
        and __setattr__
    This is similar to https://github.com/celery/django-celery/blob/master/djcelery/schedulers.py
     that uses SQLAlchemy DBModels as delegates.
    """
    rdb = None

    def __init__(self, name=None, task=None, enabled=True, last_run_at=None,
                 total_run_count=None, schedule=None, args=(), kwargs=None,
                 options=None, app=None, **extrakwargs):

        # defaults (MUST NOT call self here - or loop __getattr__ for ever)
        app = app or current_app
        # Setting a default time a bit before now to not miss a task that was just added.
        last_run_at = last_run_at or app.now() - datetime.timedelta(
                seconds=app.conf.CELERYBEAT_MAX_LOOP_INTERVAL)

        # using periodic task as delegate
        self._task = PeriodicTask(
                # Note : for compatibiilty with celery methods, the name of the task is
                # actually the key in redis DB.
                # For extra fancy fields (like a human readable name, you can leverage extrakwargs)
                name=name,
                task=task,
                enabled=enabled,
                schedule=schedule,  # TODO : sensible default here ?
                args=args,
                kwargs=kwargs or {},
                options=options or {},
                last_run_at=last_run_at,
                total_run_count=total_run_count or 0,
                **extrakwargs
        )

        self.app = app
        super(RedisScheduleEntry, self).__init__()

    # automatic delegation to PeriodicTask (easy delegate)
    def __getattr__(self, attr):
        return getattr(self._task, attr)

    def __setattr__(self, attr, value):
        if attr in ('app', '_task', 'rdb', 'singleton'):
            return super(RedisScheduleEntry, self).__setattr__(attr, value)
        # We set the attribute in the task delegate if available
        if self._task and hasattr(self._task, attr):
            return setattr(self._task, attr, value)

        # else we raise
        raise AttributeError(
                "Attribute {attr} not found in {tasktype}".format(attr=attr,
                                                                  tasktype=type(self._task)))

    #
    # Overrides schedule accessors in PeriodicTask to store dict in json but
    # retrieve proper celery schedules
    #
    def get_schedule(self):
        if {'every', 'period'}.issubset(self._task.schedule.keys()):
            return celery.schedules.schedule(
                    datetime.timedelta(
                        **{self._task.schedule['period']: self._task.schedule['every']}),
                    self.app)
        elif {'minute', 'hour', 'day_of_week', 'day_of_month', 'month_of_year'}.issubset(
                self._task.schedule.keys()):
            return celery.schedules.crontab(minute=self._task.schedule['minute'],
                                            hour=self._task.schedule['hour'],
                                            day_of_week=self._task.schedule['day_of_week'],
                                            day_of_month=self._task.schedule['day_of_month'],
                                            month_of_year=self._task.schedule['month_of_year'],
                                            app=self.app)
        else:
            raise TaskTypeError('Existing Task schedule type not recognized')

    def set_schedule(self, schedule):
        if isinstance(schedule, celery.schedules.schedule):
            # TODO : unify this with Interval in PeriodicTask
            self._task.schedule = {
                'every': max(schedule.run_every.total_seconds(), 0),
                'period': 'seconds'
            }
        elif isinstance(schedule, celery.schedules.crontab):
            # TODO : unify this with Crontab in PeriodicTask
            self._task.schedule = {
                'minute': schedule._orig_minute,
                'hour': schedule._orig_hour,
                'day_of_week': schedule._orig_day_of_week,
                'day_of_month': schedule._orig_day_of_month,
                'month_of_year': schedule._orig_month_of_year
            }
        else:
            raise TaskTypeError('New Task schedule type not recognized')

    schedule = property(get_schedule, set_schedule)

    #
    # Overloading ScheduleEntry methods
    #
    def is_due(self):
        """See :meth:`~celery.schedule.schedule.is_due`."""
        due = self.schedule.is_due(self.last_run_at)

        logger.debug('task {0} due : {1}'.format(self.name, due))
        if not self.enabled:
            logger.info('task {0} is disabled. not triggered.'.format(self.name))
            # if the task is disabled, we always return false, but the time that
            # it is next due is returned as usual
            return celery.schedules.schedstate(is_due=False, next=due[1])
        return due

    def __repr__(self):
        return '<RedisScheduleEntry: {0.name} {call} {0.schedule}'.format(
                self,
                call=kombu.utils.reprcall(self.task, self.args or (), self.kwargs or {}),
        )

    def update(self, other):
        """
        Update values from another entry.
        This is used to dynamically update periodic entry from edited redis values
        Does not update "non-editable" fields
        Extra arguments will be updated (considered editable)
        """
        # Handle delegation properly here
        self._task.update(other._task)
        # we should never need to touch the app here

    #
    # ScheduleEntry needs to be an iterable
    #

    # from celery.beat.ScheduleEntry._default_now
    def _default_now(self):
        return self.get_schedule().now() if self.schedule else self.app.now()

    # from celery.beat.ScheduleEntry._next_instance
    def _next_instance(self, last_run_at=None):
        """Return a new instance of the same class, but with
        its date and count fields updated."""
        return self.__class__(**dict(
                self,
                last_run_at=last_run_at or self._default_now(),
                total_run_count=self.total_run_count + 1,
        ))

    __next__ = next = _next_instance  # for 2to3

    def __iter__(self):
        # We need to delegate iter (iterate on task members, not on multiple tasks)
        # Following celery.SchedulerEntry.__iter__() design
        return iter(self._task)

    @staticmethod
    def get_all_as_dict(scheduler_url, key_prefix):
        """get all of the tasks, for best performance with large amount of tasks, return a generator
        """
        # Calling another generator
        for task_key, task_dict in PeriodicTask.get_all_as_dict(scheduler_url, key_prefix):
            yield task_key, task_dict

    @classmethod
    def from_entry(cls, scheduler_url, name, **entry):
        # options = entry.get('options') or {}  # unused variable!
        fields = dict(entry)
        fields['name'] = current_app.conf.CELERY_REDIS_SCHEDULER_KEY_PREFIX + name
        schedule = fields.pop('schedule')
        schedule = celery.schedules.maybe_schedule(schedule)
        if isinstance(schedule, celery.schedules.crontab):
            fields['crontab'] = {
                'minute': schedule._orig_minute,
                'hour': schedule._orig_hour,
                'day_of_week': schedule._orig_day_of_week,
                'day_of_month': schedule._orig_day_of_month,
                'month_of_year': schedule._orig_month_of_year
            }
        elif isinstance(schedule, celery.schedules.schedule):
            fields['interval'] = {'every': max(schedule.run_every.total_seconds(), 0),
                                  'period': 'seconds'}

        fields['args'] = fields.get('args', [])
        fields['kwargs'] = fields.get('kwargs', {})
        fields['key'] = fields['name']
        return cls(PeriodicTask.from_dict(fields, scheduler_url))


def lock_factory(dlm, name, ttl_s):

    @contextlib.contextmanager
    def lock():
        locked = False
        try:
            logger.info('Secure lock for {} with a ttl of {}'.format(name, ttl_s))
            locked = dlm.lock(name, ttl_s*1000)
            if not isinstance(locked, Lock):
                raise redis.exceptions.LockError('Unable to lock')
            yield locked
        except MultipleRedlockException as e:
            raise redis.exceptions.LockError(e)
        finally:
            if locked is not False:
                logger.info('Releasing {}'.format(locked.resource))
                dlm.unlock(locked)
    return lock

DEFAULT_REDIS_URI = 'redis://localhost'


class RedisScheduler(Scheduler):
    Entry = RedisScheduleEntry

    def __init__(self, *args, **kwargs):
        if hasattr(current_app.conf, 'CELERY_REDIS_SCHEDULER_URL'):
            logger.info('backend scheduler using %s',
                        current_app.conf.CELERY_REDIS_SCHEDULER_URL)
        else:
            logger.info('backend scheduler using %s', DEFAULT_REDIS_URI)

        self.update_interval = current_app.conf.get('UPDATE_INTERVAL') or datetime.timedelta(
                seconds=10)

        # how long we should hold on to the redis lock in seconds
        if 'CELERY_REDIS_SCHEDULER_LOCK_TTL' in current_app.conf:
            lock_ttl = current_app.conf.CELERY_REDIS_SCHEDULER_LOCK_TTL
        else:
            lock_ttl = 30

        if lock_ttl < self.update_interval.seconds:
            lock_ttl = self.update_interval.seconds * 2
        self.lock_ttl = lock_ttl

        self._dirty = set()  # keeping modified entries by name for sync later on
        self._schedule = {}  # keeping dynamic schedule from redis DB here
        # self.data is used for statically configured schedule
        try:
            self.schedule_url = current_app.conf.CELERY_REDIS_SCHEDULER_URL
        except AttributeError:
            self.schedule_url = DEFAULT_REDIS_URI

        self.rdb = StrictRedis.from_url(self.schedule_url)
        logger.info('Setting RedLock provider to {}'.format(self.schedule_url))
        self.dlm = Redlock([self.rdb])
        self._secure_cronlock = \
            lock_factory(self.dlm, 'celery:beat:task_lock',  self.lock_ttl)
        self._last_updated = None

        self.Entry.scheduler = self
        self.Entry.rdb = self.rdb

        # This will launch setup_schedule if not lazy
        super(RedisScheduler, self).__init__(*args, **kwargs)
        logger.info('Scheduler ready')

    @catch_errors
    def setup_schedule(self):
        super(RedisScheduler, self).setup_schedule()
        # In case we have a preconfigured schedule
        # self.update_from_dict(self.app.conf.CELERYBEAT_SCHEDULE)

        prefix = current_app.conf.CELERY_REDIS_SCHEDULER_KEY_PREFIX
        signature = None
        deferreds = []
        for name in self.app.conf.CELERYBEAT_SCHEDULE:

            schedule_hash = self.schedule[name].jsonhash()

            key = '{}{}'.format(prefix, name)

            try:
                signature = self.rdb.hget(key, 'hash')
            except redis.exceptions.ResponseError:
                logger.exception('Fetching {} threw an error. Clearing it.'.format(key))
                signature = None

            if signature != schedule_hash:
                self.rdb.delete(key)
                deferreds.append((key, {
                    'hash': schedule_hash,
                    'schedule': self.schedule[name].jsondump()
                    }))

        if deferreds:
            try:
                with self._secure_cronlock() as lock:
                    t_s = time.time()

                    if not lock:
                        logger.debug('Unable to acquire write lock')
                        return
                    for key, value in deferreds:
                        if time.time() - t_s >= 30:
                            self.dlm.touch(lock, 60*1000)
                            t_s = time.time()
                        logger.debug('Update/insert {} into Redis'.format(key))
                        self.rdb.hmset(key, value)

            except redis.exceptions.LockError:
                logger.exception('Unable to acquire write lock, encountered some redis errors')

    @catch_errors
    def tick(self):
        """Run a tick, that is one iteration of the scheduler.
        Executes all due tasks.
        """
        # need to grab all data (might have been updated) from schedule DB.
        # we need to merge it with whatever schedule was set in config, and already
        # installed default tasks
        try:
            s = self.all_as_schedule()
            self.merge_inplace(s)
        except Exception as exc:
            logger.error(
                    "Exception when getting tasks from {url} : {exc}".format(url=self.schedule_url,
                                                                             exc=exc))
            # TODO : atomic merge : be able to cancel it if there s a problem
            raise

        # # displaying the schedule we got from redis
        logger.debug("DB schedule : {0}".format(self.schedule))

        # this will call self.maybe_due() to check if any entry is due.
        return super(RedisScheduler, self).tick()

    @catch_errors
    def all_as_schedule(self, key_prefix=None, entry_class=None):
        logger.debug('RedisScheduler: Fetching database schedule')
        key_prefix = key_prefix or current_app.conf.CELERY_REDIS_SCHEDULER_KEY_PREFIX
        entry_class = entry_class or self.Entry

        d = {}
        for key, task in entry_class.get_all_as_dict(self.rdb, key_prefix):
            # logger.debug('Building {0} from : {1}'.format(entry_class, task))
            d[key] = entry_class(**dict(task, app=self.app))
        return d

    @catch_errors
    def reserve(self, entry):
        # called when the task is about to be run
        # (and data will be modified -> sync() will need to save it)

        new_entry = super(RedisScheduler, self).reserve(entry)
        # Need to store the key of the entry, because the entry may change in the mean time.
        self._dirty.add(new_entry.name)
        return new_entry

    def apply_async(self, entry, producer=None, advance=True, **kwargs):
        singleton = False

        if isinstance(entry, RedisScheduleEntry):
            singleton = entry.singleton
        if not singleton:
            return super(RedisScheduler, self).apply_async(
                entry, producer=producer, advance=advance, **kwargs)

        logger.info('Attempting to secure an exclusive lock for {}'.format(entry))

        lock_name = '{}:run'.format(entry.name)
        ctx_manager = lock_factory(self.dlm, lock_name,  5*60)()
        try:
            lock = ctx_manager.__enter__()
            if lock is False:
                logger.debug('Unable to secure {}'.format(entry.name))
                return EmptyResult()
            assert isinstance(lock, Lock) and lock.validity > 10000
            key = self.rdb.get(lock.resource)
            if key != lock.key:
                logger.warning('Invalid RedLock! {} != {}'.format(key, lock.key))
                return EmptyResult()

        except redis.exceptions.LockError:
            logger.debug('Unable to secure {}'.format(entry.name))
            return EmptyResult()
        except Exception:
            logger.exception('Error in lock secure')
            return EmptyResult()

        logger.info('Running {}'.format(entry.name))
        t_s = time.time()
        ttl = self.rdb.ttl(lock.resource)

        assert self.rdb.get(lock.resource), 'Lock {} never materialized'.format(lock.resource)
        assert ttl > 120, 'Lock {} never materialized with a bad ttl {}'.format(lock.resource, ttl)
        result = super(RedisScheduler, self).apply_async(
            entry, producer=producer, advance=advance, **kwargs)

        logger.info('Attaching callback thread for {}'.format(entry.name))
        t = threading.Thread(
            target=lock_task_until, args=(entry.name, self.dlm, lock, t_s, self.rdb, result,))
        t.daemon = True
        t.start()

        return result

    @catch_errors
    def sync(self):
        prefix = current_app.conf.CELERY_REDIS_SCHEDULER_KEY_PREFIX
        _tried = set()
        try:
            with self._secure_cronlock() as lock:
                logger.info('Writing modified entries...')
                t_s = time.time()
                while self._dirty:
                    if time.time() - t_s >= 30.:
                        self.dlm.touch(lock, 60*1000)
                        t_s = time.time()

                    name = self._dirty.pop()
                    _tried.add(name)
                    key = name
                    if not key.startswith(prefix):
                        key = '{}{}'.format(prefix, name)
                    # Saving the entry back into Redis DB.
                    self.rdb.hmset(key, {
                        'hash': self.schedule[name].jsonhash(),
                        'schedule': self.schedule[name].jsondump()
                        })
        except redis.exceptions.LockError:
            # retry later
            self._dirty |= _tried
            logger.debug('Unable to secure write lock for syncing task state')
        except Exception as exc:
            # retry later
            self._dirty |= _tried
            logger.error('Error while sync: %r', exc, exc_info=1)

    @catch_errors
    def close(self):
        self.sync()

    @property
    def info(self):
        return '    . db -> {self.schedule_url}'.format(self=self)
