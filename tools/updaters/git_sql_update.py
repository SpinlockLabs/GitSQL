#!/usr/bin/env python3
from sys import stderr, exit, argv
from configparser import ConfigParser
from itertools import islice
from hashlib import sha1

from psycopg2cffi import connect, Binary

import pygit2

if len(argv) < 3:
    print("Usage: gitsrv.py <config> <repository>", file=stderr)
    exit(1)

cfg = ConfigParser()
cfg.read(argv[1])
local_path = cfg.get('local', argv[2], fallback=None)

if not cfg.has_section('connection'):
    print("ERROR: Connection configuration section missing.", file=stderr)
    exit(1)

if not local_path:
    print(
        "ERROR: Local path not defined for repository {0}".format(argv[2]),
        file=stderr
    )
    exit(1)

db_conn_info = ''
for item in cfg.items('connection'):
    db_conn_info += " {}='{}'".format(item[0], item[1])
db_conn_info = db_conn_info.lstrip()

conn = connect(db_conn_info)
repo = pygit2.Repository(local_path)


def translate_type_id(i):
    if i == 1:
        return "commit"
    elif i == 2:
        return "tree"
    elif i == 3:
        return "blob"
    elif i == 4:
        return "tag"
    else:
        raise Exception("Unknown Type: {}" % i)


def encode_git_object(oid: pygit2.Oid):
    type_id, data = repo.read(oid)
    type_name = translate_type_id(type_id)
    encoded = bytearray()
    encoded.extend(type_name.encode())
    encoded.extend(' '.encode())
    encoded.extend(str(len(data)).encode())
    encoded.extend(b'\x00')
    encoded.extend(data)
    sha = sha1()
    sha.update(encoded)
    calc_hash = sha.hexdigest()
    if str(oid) != calc_hash:
        raise Exception(
            "Invalid Object Encoding: expected {}, encoded {}" % oid % calc_hash)
    return encoded


def split_every(n, iterable):
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))


def find_needed(oids: list):
    klist = []
    for id in oids:
        klist.append("'" + id + "'")

    array_str = 'array[' + ','.join(klist) + ']'
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT hash FROM objects WHERE array[hash] <@ {0}'.format(array_str))
        rows = cursor.fetchall()
        print(rows)
        for row in rows:
            nhash = row[0]
            oids.remove(nhash)
    finally:
        cursor.close()
    return oids


cursor = conn.cursor()
cursor.execute('CREATE TEMPORARY TABLE objlist(hash TEXT);')
cursor.execute('TRUNCATE objlist;')
conn.commit()
cursor.execute('PREPARE t(TEXT[]) AS INSERT INTO objlist(hash) SELECT * FROM unnest($1);')
total = 0
for section in split_every(500, repo):
    cursor.execute('EXECUTE t(%s)', ([str(x) for x in section],))
    total += len(section)
    print('loading %i objects for comparison' % total)
cursor.execute('SELECT hash FROM objlist c WHERE NOT EXISTS (SELECT 1 FROM objects s WHERE s.hash = c.hash)')
cn = conn.cursor()

for row in cursor:
    obj_hash = row[0]
    print("insert object %s" % obj_hash)
    bdat = encode_git_object(pygit2.Oid(hex=obj_hash))
    bina = Binary(bdat)
    cn.execute('INSERT INTO objects (hash, content) VALUES (%s, %s)', (obj_hash, bina,))

conn.commit()
for ref_name in repo.references:  # type: str
    ref = repo.references[ref_name]

    cursor.execute('SELECT target FROM refs WHERE name = %s;', (ref_name,))

    found = cursor.rowcount
    current_target = None
    if found > 0:
        current_target = cursor.fetchone()[0]
    target = str(ref.target)

    if target != current_target:
        cn.execute(
            'INSERT INTO refs (name, target) VALUES (%s, %s) ON CONFLICT (name) DO UPDATE SET target = %s;',
            (ref_name, target, target,)
        )
        print('updated %s to %s' % (ref_name, target))

conn.commit()
cn.close()
cursor.close()
conn.close()
