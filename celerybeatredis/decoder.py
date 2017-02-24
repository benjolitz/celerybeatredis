#! /usr/bin/env python
# coding: utf-8
# Date: 14/8/31
# Author: konglx
# File:
# Description:
import logging
from datetime import datetime
try:
    import simplejson as json
except ImportError:
    import json

TYPES = {
    'datetime': datetime,
    'frozenset': lambda values: frozenset(values),
    'set': lambda values: set(values),
}


logger = logging.getLogger(__name__)


class DateTimeDecoder(json.JSONDecoder):
    def __init__(self, *args, **kargs):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object,
                                  *args, **kargs)

    def dict_to_object(self, d):
        if '__type__' not in d:
            return d

        type_name = d.pop('__type__')
        try:
            constructor = TYPES[type_name]
            item = constructor(**d)
        except Exception:
            logger.exception('Unable to reify {}'.format(type_name))
            d['__type__'] = type_name
            return d
        return item


class DateTimeEncoder(json.JSONEncoder):
    """ Instead of letting the default encoder convert datetime to string,
        convert datetime objects into a dict, which can be decoded by the
        DateTimeDecoder
    """

    def default(self, obj):
        if isinstance(obj, datetime):
            return {
                '__type__': 'datetime',
                'year': obj.year,
                'month': obj.month,
                'day': obj.day,
                'hour': obj.hour,
                'minute': obj.minute,
                'second': obj.second,
                'microsecond': obj.microsecond,
            }
        elif isinstance(obj, (set, frozenset)):
            return {
                '__type__': type(obj).__name__,
                'values': list(obj)
            }
        else:
            return json.JSONEncoder.default(self, obj)
