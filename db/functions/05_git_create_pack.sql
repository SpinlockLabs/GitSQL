-- <PYTHON ONLY> --
CREATE OR REPLACE FUNCTION git_create_pack(xhashes TEXT[])
    RETURNS TABLE (
        part BYTEA
    )
AS $BODY$
import hashlib
import struct
import zlib
import plpy

obj_types = {
    'commit': 1,
    'tree': 2,
    'blob': 3,
    'tag': 4
}


def load_object(h):
    rows = plpy.execute("SELECT type, content FROM headers WHERE hash = '%s'" % h)
    if len(rows) != 1:
        raise Exception("Object %s not found." % h)
    row = rows[0]
    return row["type"], row["content"]


def encode_pack_object(obj_type, data):
    out = bytearray()
    size = len(data)
    byte = (obj_type << 4) | (size & 0x0f)
    size >>= 4
    while size:
        out.append(byte | 0x80)
        byte = size & 0x7f
        size >>= 7
    out.append(byte)
    out.extend(zlib.compress(data))
    return out


def create_pack(count, hashes):
    sha = hashlib.new('sha1')
    header = struct.pack('!4sLL', b'PACK', 2, count)
    sha.update(header)
    yield header

    for obj_hash in hashes:
        typ, content = load_object(obj_hash)
        result = encode_pack_object(obj_types[typ], content)
        sha.update(result)
        yield result

    sha1 = sha.digest()
    yield sha1


return create_pack(len(xhashes), xhashes)
$BODY$
LANGUAGE 'plpython3u';
-- </PYTHON ONLY> --
