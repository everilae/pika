# -*- encoding: utf-8 -*-
"""
pika.data tests

"""
import datetime
import decimal
import platform
try:
    import unittest2 as unittest
except ImportError:
    import unittest

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

from pika import data
from pika import exceptions
from pika.compat import long


class DataTests(unittest.TestCase):

    maxDiff = None

    FIELD_TBL_ENCODED = (
        b'\x00\x00\x00\xe3'
        b'\x05arrayA\x00\x00\x00\x06b\x01b\x02b\x03'
        b'\x0aemptyarrayA\x00\x00\x00\x00'
        b'\x07boolvalt\x01'
        b'\x07decimalD\x02\x00\x00\x01:'
        b'\x0bdecimal_tooD\x00\x00\x00\x00d'
        b'\x07dictvalF\x00\x00\x00\x0c\x03fooS\x00\x00\x00\x03bar'
        b'\x06intvalI\x00\x01\xe0\xf3'
        # https://www.rabbitmq.com/amqp-0-9-1-errata.html#section_3
        # long long conflict here
        b'\x07longvall\x7f\xff\xff\xff\xff\xff\xff\xff'
        b'\x08floatvalf\x3e\x00\x00\x00'
        b'\x09doublevald\x48\x3d\x63\x29\xf1\xc3\x5c\xa5'
        b'\x04nullV'
        b'\x06strvalS\x00\x00\x00\x04Test'
        b'\x0ctimestampvalT\x00\x00\x00\x00Ec)\x92'
        b'\x07unicodeS\x00\x00\x00\x08utf8=\xe2\x9c\x93'
    )

    FIELD_TBL_VALUE = OrderedDict([
        ('array', [1, 2, 3]),
        ('emptyarray', []),
        ('boolval', True),
        ('decimal', decimal.Decimal('3.14')),
        ('decimal_too', decimal.Decimal('100')),
        ('dictval', {'foo': 'bar'}),
        ('intval', 123123),
        ('longval', 9223372036854775807),
        # Error prone, depends heavily on float precision etc., remove
        # if needed
        ('floatval', .125),
        ('doubleval', 1.e40),
        ('null', None),
        ('strval', 'Test'),
        ('timestampval', datetime.datetime(2006, 11, 21, 16, 30, 10)),
        ('unicode', u'utf8=✓')
    ])

    def test_encode_table(self):
        result = []
        data.encode_table(result, self.FIELD_TBL_VALUE)
        self.assertEqual(b''.join(result), self.FIELD_TBL_ENCODED)

    def test_encode_table_bytes(self):
        result = []
        byte_count = data.encode_table(result, self.FIELD_TBL_VALUE)
        self.assertEqual(byte_count, 231)

    def test_decode_table(self):
        value, byte_count = data.decode_table(self.FIELD_TBL_ENCODED, 0)
        self.assertDictEqual(value, self.FIELD_TBL_VALUE)
        self.assertEqual(byte_count, 231)

    def test_encode_raises(self):
        self.assertRaises(exceptions.UnsupportedAMQPFieldException,
                          data.encode_table, [], {'foo': set([1, 2, 3])})

    def test_decode_raises(self):
        self.assertRaises(exceptions.InvalidFieldTypeException,
                          data.decode_table,
                          b'\x00\x00\x00\t\x03fooZ\x00\x00\x04\xd2', 0)
