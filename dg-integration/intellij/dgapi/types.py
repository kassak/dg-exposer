import datetime
import time


class TypeCode(object):

    def __init__(self, code):
        self._code = code

    def __eq__(self, other):
        return other is self

    def __unicode__(self):
        return self._code

    def __repr__(self):
        return self._code

    def __str__(self):
        return self._code

    def __hash__(self):
        return hash(self._code)


# noinspection PyPep8Naming
def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])


# noinspection PyPep8Naming
def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])


# noinspection PyPep8Naming
def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
Binary = bytes

STRING = TypeCode("STRING")
BINARY = TypeCode("BINARY")
NUMBER = TypeCode("NUMBER")
DATETIME = TypeCode("DATETIME")
ROWID = TypeCode("ROWID")
