"""AMQP Table Encoding/Decoding"""
import struct
import decimal
import calendar
from datetime import datetime
from functools import partial
from collections import OrderedDict

from pika import exceptions
from pika.compat import unicode_type, PY2, long, as_bytes


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
    # ...
    # 4.2.5.5
    # The server SHOULD validate field names and upon receiving an invalid field
    # name, it SHOULD signal a connection exception with reply code 503 (syntax
    # error).
    # -> validate length (avoid truncated utf-8 / corrupted data), but skip null
    # byte check.
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


def encode_value(pieces, value):
    """Encode the value passed in and append it to the pieces list returning
    the the size of the encoded value.

    :param list pieces: Already encoded values
    :param any value: The value to encode
    :rtype: int

    """

    if PY2:
        if isinstance(value, basestring):
            if isinstance(value, unicode_type):
                value = value.encode('utf-8')
            pieces.append(struct.pack('>cI', b'S', len(value)))
            pieces.append(value)
            return 5 + len(value)
    else:
        # support only str on Python 3
        if isinstance(value, str):
            value = value.encode('utf-8')
            pieces.append(struct.pack('>cI', b'S', len(value)))
            pieces.append(value)
            return 5 + len(value)
    if isinstance(value, bool):
        pieces.append(struct.pack('>cB', b't', int(value)))
        return 2
    if isinstance(value, long):
        pieces.append(struct.pack('>cq', b'l', value))
        return 9
    elif isinstance(value, int):
        pieces.append(struct.pack('>ci', b'I', value))
        return 5
    elif isinstance(value, decimal.Decimal):
        value = value.normalize()
        if value.as_tuple().exponent < 0:
            decimals = -value.as_tuple().exponent
            raw = int(value * (decimal.Decimal(10) ** decimals))
            pieces.append(struct.pack('>cBi', b'D', decimals, raw))
        else:
            # per spec, the "decimals" octet is unsigned (!)
            pieces.append(struct.pack('>cBi', b'D', 0, int(value)))
        return 6
    elif isinstance(value, datetime):
        pieces.append(struct.pack('>cQ', b'T',
                                  calendar.timegm(value.utctimetuple())))
        return 9
    elif isinstance(value, dict):
        pieces.append(struct.pack('>c', b'F'))
        return 1 + encode_table(pieces, value)
    elif isinstance(value, list):
        p = []
        for v in value:
            encode_value(p, v)
        piece = b''.join(p)
        pieces.append(struct.pack('>cI', b'A', len(piece)))
        pieces.append(piece)
        return 5 + len(piece)
    elif value is None:
        pieces.append(struct.pack('>c', b'V'))
        return 1
    elif isinstance(value, float):
        try:
            pieces.append(struct.pack('>cf', b'f', value))
            return struct.calcsize('>cf')

        except OverflowError:
            pieces.append(struct.pack('>cd', b'd', value))
            return struct.calcsize('>cd')
    else:
        raise exceptions.UnsupportedAMQPFieldException(pieces, value)


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


def decode_decimal(encoded, offset):
    """Decode a decimal.
    """
    decimals = struct.unpack_from('B', encoded, offset)[0]
    offset += 1
    raw = struct.unpack_from('>i', encoded, offset)[0]
    offset += 4
    value = decimal.Decimal(raw) * (decimal.Decimal(10) ** -decimals)
    return value, offset


def _simple_value_decoder(encoded, offset, fmt, type=lambda x: x):
    """Decode ``encoded`` value at ``offset`` returning ``(value, offset)``
    tuple.
    """
    return (type(struct.unpack_from(fmt, encoded, offset)[0]),
            offset + struct.calcsize(fmt))


_table_value_decoder_lookup = {
    b't': partial(_simple_value_decoder, fmt='B', type=bool),
    b'b': partial(_simple_value_decoder, fmt='B'),
    b'B': partial(_simple_value_decoder, fmt='b'),
    b'U': partial(_simple_value_decoder, fmt='>h'),
    b'u': partial(_simple_value_decoder, fmt='>H'),
    b'I': partial(_simple_value_decoder, fmt='>i', type=long),
    b'i': partial(_simple_value_decoder, fmt='>I', type=long),
    b'L': partial(_simple_value_decoder, fmt='>q', type=long),
    b'l': partial(_simple_value_decoder, fmt='>Q', type=long),
    b'f': partial(_simple_value_decoder, fmt='>f'),
    B'd': partial(_simple_value_decoder, fmt='>d'),
    b'D': decode_decimal,
    b's': decode_short_string,
    b'S': decode_long_string,
    b'T': partial(_simple_value_decoder, fmt='>Q',
                  type=datetime.utcfromtimestamp),
    b'F': decode_table,
    b'A': decode_array,
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
        return _table_value_decoder_lookup[kind](encoded, offset)

    except KeyError:
        raise exceptions.InvalidFieldTypeException(kind)
