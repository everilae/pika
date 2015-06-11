"""AMQP Table Encoding/Decoding"""
import struct
import decimal
import calendar
from datetime import datetime
from functools import partial
from collections import OrderedDict

from pika import exceptions
from pika.compat import unicode_type, PY2, long, as_bytes, int_types


def _encode_string(pieces, value, fmt):
    """Generic string encoding

    :param list pieces: Already encoded values
    :param str value: String value to encode
    :param str fmt: Packing format for length
    :rtype: int
    """
    encoded_value = as_bytes(value)
    length = len(encoded_value)
    pieces.append(struct.pack(fmt, length))
    pieces.append(encoded_value)
    return struct.calcsize(fmt) + length


def encode_short_string(pieces, value):
    """Encode a string value as short string and append it to pieces list
    returning the size of the encoded value.

    :param list pieces: Already encoded values
    :param value: String value to encode
    :type value: str or unicode
    :rtype: int

    """
    # 4.2.5.3
    # Short strings, stored as an 8-bit unsigned integer length followed by zero
    # or more octets of data. Short strings can carry up to 255 octets of UTF-8
    # data, but may not contain binary zero octets.
    try:
        return _encode_string(pieces, value, 'B')

    except struct.error:
        raise ShortStringTooLong(value)


def encode_long_string(pieces, value):
    """Encode a string value as long string and append it to pieces list
    returning the size of the encoded value.

    :param list pieces: Already encoded values
    :param value: String value to encode
    :type value: str or unicode
    :rtype: int

    """
    # If someone is trying to encode over 4G string, let them suffer
    return _encode_string(pieces, value, '>I')


def _simple_encoder(pieces, value, fmt, type_octet, conv=lambda x: x):
    """Encode ``value`` and append to ``pieces``. Return size of encoded
    value.
    """
    fmt = '>c%s' % fmt
    pieces.append(struct.pack(fmt, type_octet, conv(value)))
    return struct.calcsize(fmt)


def encode_table(pieces, table):
    """Encode a dict as an AMQP table appending the encded table to the
    pieces list passed in.

    :param list pieces: Already encoded frame pieces
    :param dict table: The dict to encode
    :rtype: int

    """
    table = table or {}
    length_index = len(pieces)
    pieces.append(None)  # placeholder
    tablesize = 0
    for (key, value) in table.items():
        tablesize += encode_short_string(pieces, key)
        tablesize += encode_value(pieces, value)

    pieces[length_index] = struct.pack('>I', tablesize)
    return tablesize + 4


def encode_float(pieces, value):
    """Encode floating point ``value`` and append to ``pieces``. Return size
    of encoded value.
    """
    try:
        return _simple_encoder(pieces, value, 'f', b'f')

    except OverflowError:
        return _simple_encoder(pieces, value, 'd', b'd')


def encode_integer(pieces, value, fmt=None):
    """Encode an integer ``value`` and append to ``pieces``. Return size of
    encoded value. This function tries to automatically choose the best
    fitting integer representation for given value.
    """
    if -128 <= value <= 127:
        # short-short-int
        return _simple_encoder(pieces, value, 'b', b'b')

    elif -32768 <= value <= 32767:
        # short-int
        return _simple_encoder(pieces, value, 'h', b'U')

    elif -2147483648 <= value <= 2147483647:
        # long-int
        return _simple_encoder(pieces, value, 'i', b'I')

    elif -9223372036854775808 <= value <= 9223372036854775807:
        # long-long-int
        # WARNING: https://www.rabbitmq.com/amqp-0-9-1-errata.html#section_3
        # there's a conflict between AMQP 0-9-1 and RabbitMQ:
        # long long int is 'L' AMQP, but 'l' in RabbitMQ
        return _simple_encoder(pieces, value, 'q', b'l')

    raise OverflowError("integer too large")


def encode_decimal(pieces, value):
    """Encode decimal ``value`` and append to ``pieces``. Return size
    of encoded value.
    """
    # per spec, the "decimals" octet is unsigned (!)
    fmt = '>cBi'
    value = value.normalize()
    if value.as_tuple().exponent < 0:
        decimals = -value.as_tuple().exponent
        raw = int(value * (decimal.Decimal(10) ** decimals))
        pieces.append(struct.pack(fmt, b'D', decimals, raw))

    else:
        pieces.append(struct.pack(fmt, b'D', 0, int(value)))

    return struct.calcsize(fmt)


def encode_array(pieces, value):
    """Encode list ``value`` and append to ``pieces``. Return size
    of encoded value.
    """
    p = []
    for v in value:
        encode_value(p, v)
    piece = b''.join(p)
    fmt = '>cI'
    pieces.append(struct.pack(fmt, b'A', len(piece)))
    pieces.append(piece)
    return struct.calcsize(fmt) + len(piece)


def encode_dict(pieces, value):
    """Encode dict ``value`` and append to ``pieces``. Return size
    of encoded value.
    """
    pieces.append(struct.pack('>c', b'F'))
    return 1 + encode_table(pieces, value)


_table_encoder_lookup = [
    (bool, partial(_simple_encoder, fmt='B', type_octet=b't')),
    (int_types, encode_integer),
    (datetime, partial(_simple_encoder, fmt='Q', type_octet=b'T',
                       conv=lambda v: calendar.timegm(v.utctimetuple()))),
    (float, encode_float),
    (decimal.Decimal, encode_decimal),
    (dict, encode_dict),
    (list, encode_array),
]


def encode_value(pieces, value):
    """Encode the value passed in and append it to the pieces list returning
    the the size of the encoded value.

    :param list pieces: Already encoded values
    :param any value: The value to encode
    :rtype: int

    """

    # support only str on Python 3
    if isinstance(value, basestring if PY2 else str):
        pieces.append(struct.pack('c', b'S'))
        return struct.calcsize('c') + encode_long_string(pieces, value)

    for type, encoder in _table_encoder_lookup:
        if isinstance(value, type):
            return encoder(pieces, value)

    if value is None:
        pieces.append(struct.pack('>c', b'V'))
        return 1

    raise exceptions.UnsupportedAMQPFieldException(pieces, value)


def _simple_decoder(encoded, offset, fmt, type=lambda x: x):
    """Decode ``encoded`` value at ``offset`` returning ``(value, offset)``
    tuple.
    """
    return (type(struct.unpack_from(fmt, encoded, offset)[0]),
            offset + struct.calcsize(fmt))


def _decode_string(encoded, offset, fmt):
    """Decode a string.
    """
    length = struct.unpack_from(fmt, encoded, offset)[0]
    offset += struct.calcsize(fmt)
    value = encoded[offset:offset + length].decode('utf-8')
    offset += length
    return value, offset


if PY2:
    def decode_short_string(encoded, offset):
        """Decode a short string value from ``encoded`` data at ``offset``.
        """
        length = struct.unpack_from('B', encoded, offset)[0]
        offset += 1
        # Purely for compatibility with original python2 code. No idea what
        # and why this does.
        value = encoded[offset:offset + length]
        try:
            value = bytes(value)
        except UnicodeEncodeError:
            pass
        offset += length
        return value, offset

else:
    def decode_short_string(encoded, offset):
        """Decode a short string value from ``encoded`` data at ``offset``.
        """
        return _decode_string(encoded, offset, 'B')


def decode_long_string(encoded, offset):
    """Decode a long string value from ``encoded`` data at ``offset``.
    """
    return _decode_string(encoded, offset, '>I')


def decode_decimal(encoded, offset):
    """Decode a decimal.
    """
    decimals = struct.unpack_from('B', encoded, offset)[0]
    offset += 1
    raw = struct.unpack_from('>i', encoded, offset)[0]
    offset += 4
    value = decimal.Decimal(raw) * (decimal.Decimal(10) ** -decimals)
    return value, offset


def decode_array(encoded, offset):
    """Decode an array.
    """
    length = struct.unpack_from('>I', encoded, offset)[0]
    offset += 4
    offset_end = offset + length
    value = []
    while offset < offset_end:
        v, offset = decode_value(encoded, offset)
        value.append(v)

    return value, offset


def decode_table(encoded, offset):
    """Decode the AMQP table passed in from the encoded value returning the
    decoded result and the number of bytes read plus the offset.

    :param str encoded: The binary encoded data to decode
    :param int offset: The starting byte offset
    :rtype: tuple

    """
    result = OrderedDict()
    tablesize = struct.unpack_from('>I', encoded, offset)[0]
    offset += 4
    limit = offset + tablesize
    while offset < limit:
        key, offset = decode_short_string(encoded, offset)
        value, offset = decode_value(encoded, offset)
        result[key] = value
    return result, offset


_table_decoder_lookup = {
    b't': partial(_simple_decoder, fmt='B', type=bool),
    b'b': partial(_simple_decoder, fmt='b'),
    b'B': partial(_simple_decoder, fmt='B'),
    b'U': partial(_simple_decoder, fmt='>h'),
    b'u': partial(_simple_decoder, fmt='>H'),
    b'I': partial(_simple_decoder, fmt='>i', type=long),
    b'i': partial(_simple_decoder, fmt='>I', type=long),
    # WARNING: https://www.rabbitmq.com/amqp-0-9-1-errata.html#section_3
    # there's a conflict with long long integers between RabbitMQ and
    # AMQP 0-9-1 specification: L -> l for signed long longs
    b'l': partial(_simple_decoder, fmt='>q', type=long),
    #b'l': partial(_simple_decoder, fmt='>Q', type=long),
    b'f': partial(_simple_decoder, fmt='>f'),
    b'd': partial(_simple_decoder, fmt='>d'),
    b'D': decode_decimal,
    b's': decode_short_string,
    b'S': decode_long_string,
    b'A': decode_array,
    b'T': partial(_simple_decoder, fmt='>Q', type=datetime.utcfromtimestamp),
    b'F': decode_table,
    b'V': lambda encoded, offset: (None, offset),
}


def decode_value(encoded, offset):
    """Decode the value passed in returning the decoded value and the number
    of bytes read in addition to the starting offset.

    :param str encoded: The binary encoded data to decode
    :param int offset: The starting byte offset
    :rtype: tuple
    :raises: pika.exceptions.InvalidFieldTypeException

    """
    # slice to get bytes in Python 3 and str in Python 2
    kind = encoded[offset:offset + 1]
    offset += 1

    try:
        return _table_decoder_lookup[kind](encoded, offset)

    except KeyError:
        raise exceptions.InvalidFieldTypeException(kind)
