import hashlib
import struct
import zlib


def encode_pack_object(obj_type, data):
    size = len(data)
    byte = (obj_type << 4) | (size & 0x0f)
    size >>= 4
    header = bytearray()
    while size:
        header.append(byte | 0x80)
        byte = size & 0x7f
        size >>= 7
    return bytes(header) + zlib.compress(data)


def create_pack_object(count, objects):
    header = struct.pack('!4sLL', b'PACK', 2, count)
    contents = header
    for obj in objects:
        encode_pack_object(obj[0], obj[1])
    sha1 = hashlib.sha1(contents).digest()
    data = contents + sha1
    return data
