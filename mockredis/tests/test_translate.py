"""
Test redis command range translation.
"""
from nose.tools import eq_

from mockredis.client import MockRedis


def test_translate_range():

    # Translation expected to truncate start indices into [0, len], and end indices into [-1, len-1].
    # It also handle digit string input, and convert negative indices to positive by counting from len backwards.
    # It is not expected to enforce start <= end.
    cases = [
        (10, 1, 7, (1, 7)),
        (10, '1', '7', (1, 7)),
        (10, 1, -7, (1, 3)),
        (10, 1, '-7', (1, 3)),
        (10, -1, 7, (9, 7)),
        (10, '-1', -11, (9, -1)),
        (10, -11, -10, (0, 0)),
    ]

    def _test(len_, start, end, expected):
        redis = MockRedis()
        eq_(tuple(redis._translate_range(len_, start, end)), expected)

    for length, start_index, end_index, required in cases:
        yield _test, length, start_index, end_index, required


def test_translate_limit():

    # Translation expected to verify start is not larger than length and num is not less than or equal zero.
    # If condition does not verify, the tuple (0,0) is returned, else (start, num) is returned.
    cases = [
        (10, 3, 0, (0, 0)),
        (10, 3, -4, (0, 0)),
        (10, 11, 2, (0, 0)),
        (10, 11, -4, (0, 0)),
        (10, 3, 2, (3, 2)),
        (10, 3, 11, (3, 11)),
        (10, 3, 9, (3, 9)),
    ]

    def _test(len_, start, num, expected):
        redis = MockRedis()
        eq_(tuple(redis._translate_limit(len_, start, num)), expected)

    for length, start_index, offset, required in cases:
        yield _test, length, start_index, offset, required
