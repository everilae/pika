"""AMQP Table Encoding/Decoding"""
import struct
import decimal
import calendar
import xdrlib  # doubles
from datetime import datetime
from functools import partial
from collections import OrderedDict

from pika import exceptions
from pika.compat import unicode_type, PY2, long, as_bytes, int_types

TYPE_FMT = '>c'
ENDIAN_FMT = '>%s'


def _type_encoder(pieces, type_octet):
    """Encode ``type_octet`` and append to ``pieces``.
    """
    if type_octet:
        pieces.append(struct.pack(TYPE_FMT, type_octet))
        return struct.calcsize(TYPE_FMT)

    return 0


def _simple_encoder(pieces, value, fmt, conv=lambda x: x):
    """Encode ``value`` and append to ``pieces``. Return size of encoded
    value.
    """
    fmt = ENDIAN_FMT % fmt
    pieces.append(struct.pack(fmt, conv(value)))
    return struct.calcsize(fmt)


def _varlen_encoder(pieces, value, fmt, encoder):
    """Encode variable length data.
    """
    fmt = ENDIAN_FMT % fmt
    hole = len(pieces)
    pieces.append(None)  # placeholder
    size = encoder(pieces, value)
    pieces[hole] = struct.pack(fmt, size)
    return struct.calcsize(fmt) + size


def _string_encoder(pieces, value):
    """Generic string encoding

    :param list pieces: Already encoded values
    :param str value: String value to encode
    :rtype: int
    """
    encoded_value = as_bytes(value)
    pieces.append(encoded_value)
    return len(encoded_value)


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
        return _varlen_encoder(pieces, value, fmt='B', encoder=_string_encoder)

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
    return _varlen_encoder(pieces, value, fmt='I', encoder=_string_encoder)


def encode_float(pieces, value):
    """Encode floating point ``value`` and append to ``pieces``. Return size
    of encoded value.
    """
    try:
        # Floats as IEEE-754
        tmp = []
        size = _type_encoder(tmp, b'f') + _simple_encoder(tmp, value, 'f')
        pieces.extend(tmp)
        return size

    except OverflowError:
        # Doubles as RFC1832 XDR doubles
        packer = xdrlib.Packer()
        packer.pack_double(value)
        buf = packer.get_buffer()
        pieces.extend([struct.pack(TYPE_FMT, b'd'), buf])
        return struct.calcsize(TYPE_FMT) + len(buf)


def encode_integer(pieces, value, fmt=None):
    """Encode an integer ``value`` and append to ``pieces``. Return size of
    encoded value. This function tries to automatically choose the best
    fitting integer representation for given value.
    """
    encode = (lambda type_octet, fmt:
              _type_encoder(pieces, type_octet) +
              _simple_encoder(pieces, value, fmt))

    if -128 <= value <= 127:
        # short-short-int
        return encode(b'b', 'b')

    elif -32768 <= value <= 32767:
        # short-int
        return encode(b'U', 'h')

    elif -2147483648 <= value <= 2147483647:
        # long-int
        return encode(b'I', 'i')

    elif -9223372036854775808 <= value <= 9223372036854775807:
        # long-long-int
        # WARNING: https://www.rabbitmq.com/amqp-0-9-1-errata.html#section_3
        # there's a conflict between AMQP 0-9-1 and RabbitMQ:
        # long long int is 'L' in AMQP, but 'l' in RabbitMQ
        return encode(b'l', 'q')

    raise OverflowError("integer too large")


def encode_decimal(pieces, value):
    """Encode decimal ``value`` and append to ``pieces``. Return size
    of encoded value.
    """
    value = value.normalize()
    _, _, exponent = value.as_tuple()
    decimals = -min([0, exponent])
    integer = int(value.scaleb(decimals))
    # per spec, the "decimals" octet is unsigned (!)
    fmt = '>Bi'
    pieces.append(struct.pack(fmt, decimals, integer))
    return struct.calcsize(fmt)


def _array_encoder(pieces, value):
    """Encode ``value`` array and return size.
    """
    if value:
        return sum(map(partial(encode_value, pieces), value))

    return 0


def encode_array(pieces, value):
    """Encode list ``value`` and append to ``pieces``. Return size
    of encoded value.
    """
    return _varlen_encoder(pieces, value, fmt='I', encoder=_array_encoder)


def _table_encoder(pieces, table):
    """Encode ``table``, return ``size``.
    """
    size = 0
    if table:
        for key, value in table.items():
            size += encode_short_string(pieces, key)
            size += encode_value(pieces, value)
            
    return size


def encode_table(pieces, table):
    """Encode a dict as an AMQP table appending the encded table to the
    pieces list passed in.

    :param list pieces: Already encoded frame pieces
    :param dict table: The dict to encode
    :rtype: int

    """
    return _varlen_encoder(pieces, table, fmt='I', encoder=_table_encoder)

_table_encoder_lookup = [
    (basestring if PY2 else str, b'S', encode_long_string),
    (bool, b't', partial(_simple_encoder, fmt='B')),
    (int_types, None, encode_integer),
    (datetime, b'T', partial(_simple_encoder, fmt='Q',
                             conv=lambda v: calendar.timegm(v.utctimetuple()))),
    (float, None, encode_float),
    (decimal.Decimal, b'D', encode_decimal),
    (dict, b'F', encode_table),
    (list, b'A', encode_array),
]


def encode_value(pieces, value):
    """Encode the value passed in and append it to the pieces list returning
    the the size of the encoded value.

    :param list pieces: Already encoded values
    :param any value: The value to encode
    :rtype: int

    """
    # Special case, in theory doable with (type(None), ...), but not worth it
    if value is None:
        return _type_encoder(pieces, b'V')

    for klass, type_octet, encoder in _table_encoder_lookup:
        if isinstance(value, klass):
            return _type_encoder(pieces, type_octet) + encoder(pieces, value)

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


def decode_double(encoded, offset):
    """Decode an RFC1832 XDR double.
    """
    # The standard defines the encoding for the double-precision
    # floating-point data type "double" (64 bits or 8 bytes).
    size = 8
    unpacker = xdrlib.Unpacker(encoded[offset:offset + size])
    return unpacker.unpack_double(), offset + size


def decode_decimal(encoded, offset):
    """Decode a decimal.
    """
    fmt = '>Bi'
    decimals, integer = struct.unpack_from(fmt, encoded, offset)
    offset += struct.calcsize(fmt)
    value = decimal.Decimal(integer).scaleb(-decimals)
    return value, offset


def _varlen_decoder(encoded, offset, fmt, type, decoder):
    """Decode variable length data.
    """
    fmt = ENDIAN_FMT % fmt
    size = struct.unpack_from(fmt, encoded, offset)[0]
    offset += struct.calcsize(fmt)

    if not size:
        return type(), offset

    end = offset + size
    items = []
    while offset < end:
        value, offset = decoder(encoded, offset)
        items.append(value)

    if offset > end:
        raise RuntimeError(
            "variable length data with incorrect length: offset=%r, end=%r" % (
                offset, end))

    return type(items), offset


def decode_array(encoded, offset):
    """Decode an array.
    """
    return _varlen_decoder(encoded, offset, 'I', list, decode_value)


def decode_table(encoded, offset):
    """Decode the AMQP table passed in from the encoded value returning the
    decoded result and the number of bytes read plus the offset.

    :param str encoded: The binary encoded data to decode
    :param int offset: The starting byte offset
    :rtype: tuple

    """
    def _decoder(encoded, offset):
        key, offset = decode_short_string(encoded, offset)
        value, offset = decode_value(encoded, offset)
        return (key, value), offset

    return _varlen_decoder(encoded, offset, 'I', OrderedDict, _decoder)


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
    b'd': decode_double,
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
