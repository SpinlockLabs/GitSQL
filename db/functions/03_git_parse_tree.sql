CREATE OR REPLACE FUNCTION git_parse_tree(tree_hash TEXT, blob BYTEA)
    RETURNS TABLE (
        parent TEXT,
        mode TEXT,
        name TEXT,
        hash TEXT
    )
AS $BODY$
import codecs
import plpy

rblob = blob
if isinstance(blob, str):
    tmp = bytearray()
    tmp.extend(blob)
    rblob = tmp

def parse_item(buff):
    parts = buff.split(bytearray([0]), 1)
    headers = parts[0].decode('ascii').split(' ', 1)
    sha_bytes = parts[1]
    return [
        tree_hash,
        headers[0],
        headers[1],
        codecs.encode(sha_bytes, 'hex').decode('ascii')
    ]

def parse():
    if rblob == None:
        raise plpy.Fatal("Blob for tree %s does not exist!" % tree_hash)

    buffer = bytearray()
    inside_sha = 0
    for i, value in enumerate(rblob):
        buffer.append(value)

        if value == 0 and inside_sha == 0:
            inside_sha = i

        if (inside_sha > 0) and (i - inside_sha) == 20:
            yield parse_item(buffer)
            inside_sha = 0
            buffer = bytearray()

return parse()
$BODY$
LANGUAGE 'plpythonu';
