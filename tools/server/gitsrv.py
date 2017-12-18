#!/usr/bin/env python3
from configparser import ConfigParser
from sys import stderr, exit, argv
from zlib import compress

from aiohttp import web

from psycopg2cffi.pool import SimpleConnectionPool

from concurrent.futures import ThreadPoolExecutor

import asyncio
import base64
import pathlib
import json


class NoSuchRepository(Exception):
    def __init__(self, repo):
        self.repo = repo


if len(argv) < 2:
    print("Usage: gitsrv.py <config>", file=stderr)
    exit(1)

cfg = ConfigParser()
cfg.read(argv[1])

executor = ThreadPoolExecutor(max_workers=cfg.getint(
    'workers',
    'threads',
    fallback=10
))

memcached = None  # type: pylibmc.Client

if cfg.getboolean('memcached', 'enabled', fallback=False):
    import pylibmc

    servers = str(cfg.get('memcached', 'servers', fallback='127.0.0.1:11211'))

    memcached = pylibmc.Client(
        servers.split(' '),
        username=cfg.get('memcached', 'username', fallback=None),
        password=cfg.get('memcached', 'password', fallback=None),
        binary=True
    )

if not cfg.has_section('postgres'):
    print("ERROR: PostgreSQL configuration section missing.", file=stderr)
    exit(1)

db_conn_info = ''
for item in cfg.items('postgres'):
    db_conn_info += " {}='{}'".format(item[0], item[1])
db_conn_info = db_conn_info.lstrip()

object_count = 0
cache_hit_count = 0


def map_repo_to_db(name: str):
    mapped = cfg.get('serve', name, fallback=None)
    if mapped is None:
        raise NoSuchRepository(name)
    return mapped


pools = {}  # type: dict[str, SimpleConnectionPool]


def grab_connection(repo_name):
    if repo_name is None:
        raise NoSuchRepository(None)

    db_name = map_repo_to_db(repo_name)
    if db_name not in pools:
        conn_info = db_conn_info + (" dbname='{0}'".format(db_name))
        pools[db_name] = SimpleConnectionPool(
            1,
            cfg.getint(
                'workers',
                'sql',
                fallback=10
            ),
            conn_info
        )
    pool = pools[db_name]
    return pool, pool.getconn()


def put_connection(pool, conn):
    pool.putconn(conn)


def fetch_object(repo_name, object_hash):
    binary = None
    cursor = None
    pool = None
    conn = None

    try:
        pool, conn = grab_connection(repo_name)
        cursor = conn.cursor()
        cursor.execute("SELECT content FROM objects WHERE hash = %s", (object_hash,))
        binary = cursor.fetchone()[0]

    finally:
        cursor.close()
        put_connection(pool, conn)

        return binary


async def fetch_shallow_pack(repo_name, commit_hash, request, resp):
    resp.enable_chunked_encoding()

    cursor = None
    pool = None
    conn = None
    binary = None
    try:
        pool, conn = grab_connection(repo_name)
        cursor = conn.cursor()
        q = cursor.mogrify(
            "WITH objects AS (SELECT hash FROM git_shallow_crawl(%s))" +
            " SELECT part FROM git_create_pack((SELECT array_agg(hash) FROM objects))",
            (commit_hash,))
        cursor.execute(q)
        cursor.itersize = 10
        for row in cursor:
            if binary is None:
                await resp.prepare(request)
            binary = row[0]
            resp.write(binary.tobytes())
        resp.write_eof()
    except Exception:
        if binary is not None:
            await resp.write_eof()
        else:
            await resp.prepare(status=417)
            await resp.write_eof()
    finally:
        cursor.close()
        put_connection(pool, conn)


def fetch_commit_info(repo_name, commit_hash):
    pool = None
    conn = None
    cursor = None

    out = None
    try:
        pool, conn = grab_connection(repo_name)
        cursor = conn.cursor()
        q = cursor.mogrify("SELECT * FROM git_lookup_commit(%s)", (commit_hash,))
        cursor.execute(q)
        for row in cursor:
            out = row
    finally:
        cursor.close()
        put_connection(pool, conn)
    return out


def fetch_tree_info(repo_name, tree_hash):
    pool = None
    conn = None
    cursor = None

    out = None
    try:
        pool, conn = grab_connection(repo_name)
        cursor = conn.cursor()
        q = cursor.mogrify("SELECT * FROM git_lookup_tree(%s)", (tree_hash,))
        cursor.execute(q)
        out = cursor.fetchall()
    finally:
        cursor.close()
        put_connection(pool, conn)
    return out


def fetch_blob(repo_name, blob_hash):
    pool = None
    conn = None
    cursor = None

    out = None
    try:
        pool, conn = grab_connection(repo_name)
        cursor = conn.cursor()
        q = cursor.mogrify("SELECT content FROM contents WHERE hash = %s", (blob_hash,))
        cursor.execute(q)
        for item in cursor:
            out = item[0].tobytes()
    finally:
        cursor.close()
        put_connection(pool, conn)
    return out


async def handle_object_route(request):
    prefix = request.match_info['hash_prefix']
    suffix = request.match_info['hash_suffix']
    object_hash = prefix + suffix

    binary = None

    if memcached:
        cache_key = 'git.object[%s]' % object_hash
        cached_b64 = memcached.get(cache_key, None)
        if cached_b64 is not None:
            global cache_hit_count
            binary = base64.b64decode(cached_b64)
            cache_hit_count += 1

    if binary is None:
        binary = await asyncio.get_event_loop().run_in_executor(
            executor,
            fetch_object,
            request.match_info['repo'],
            object_hash
        )

        if memcached and len(binary) < 1024 * 1024:
            try:
                cache_key = 'git.object[%s]' % object_hash
                b64 = base64.b64encode(binary)
                memcached.set(cache_key, b64)
            except pylibmc.TooBig:
                pass

    if binary is None:
        print("[WARN] Tried to fetch object %s, but it does not exist." % object_hash)
        return web.Response(text='Object not found.', status=404)

    if type(binary) is bytearray or type(binary) is memoryview:
        binary = binary.tobytes()

    global object_count
    object_count += 1
    return web.Response(body=compress(binary))


async def handle_dlpack_route(request):
    resp = web.StreamResponse(status=200)
    f = await asyncio.get_event_loop().run_in_executor(
        executor,
        fetch_shallow_pack,
        request.match_info['repo'],
        request.match_info['commit'],
        request,
        resp
    )
    await f
    return resp


async def handle_commit_info_route(request):
    info = await asyncio.get_event_loop().run_in_executor(
        executor,
        fetch_commit_info,
        request.match_info['repo'],
        request.match_info['commit']
    )

    if info is None:
        return web.Response(status=404, text=json.dumps({
            "error": "not found"
        }))

    out = {
        'hash': info[0],
        'tree': info[1],
        'parent': info[2],
        'author': info[3],
        'committer': info[4],
        'author_time': str(info[5]),
        'commit_time': str(info[6]),
        'message': info[7],
        'pgp': info[8]
    }

    return web.Response(text=json.dumps(out, indent=2))


async def handle_tree_info_route(request):
    info = await asyncio.get_event_loop().run_in_executor(
        executor,
        fetch_tree_info,
        request.match_info['repo'],
        request.match_info['tree']
    )

    if info is None:
        return web.Response(status=404, text=json.dumps({
            "error": "not found"
        }))

    out = []

    for item in info:
        out.append({
            'parent': item[0],
            'mode': item[1],
            'name': item[2],
            'leaf': item[3]
        })

    return web.Response(text=json.dumps(out, indent=2))


async def handle_blob_route(request):
    data = await asyncio.get_event_loop().run_in_executor(
        executor,
        fetch_blob,
        request.match_info['repo'],
        request.match_info['blob']
    )

    if data is None:
        return web.Response(status=404, text=json.dumps({
            "error": "not found"
        }))

    return web.Response(body=data)


def handle_info_route(request):
    return web.Response(text='Not found.', status=404)


def handle_refs_route(request):
    pool, conn = grab_connection(request.match_info['repo'])
    cursor = conn.cursor()

    try:
        cursor.execute('SELECT name FROM refs')
        rows = cursor.fetchall()
        result = ""
        for row in rows:
            ref = row[0]
            cursor.execute("SELECT git_resolve_ref(%s)", (ref,))
            real = cursor.fetchone()[0]
            result += '{0}\t{1}\n'.format(real, ref)

        return web.Response(text=result)
    finally:
        cursor.close()
        put_connection(pool, conn)


app = web.Application()

app.router.add_get(
    '/{repo}/objects/info/{info_type}',
    handle_info_route
)

app.router.add_get(
    '/{repo}/dlpack/{commit}',
    handle_dlpack_route
)

app.router.add_get(
    '/{repo}/objects/{hash_prefix}/{hash_suffix}',
    handle_object_route
)

app.router.add_get(
    '/{repo}/info/refs',
    handle_refs_route
)

app.router.add_get(
    '/{repo}/commits/{commit}',
    handle_commit_info_route
)

app.router.add_get(
    '/{repo}/blobs/{blob}',
    handle_blob_route
)

app.router.add_get(
    '/{repo}/trees/{tree}',
    handle_tree_info_route
)


def object_count_info():
    global object_count
    global cache_hit_count

    if object_count > 0:
        print("[Statistics] %i objects were fetched" % object_count)
        object_count = 0

    if cache_hit_count > 0:
        print("[Statistics] %i objects hit the cache" % cache_hit_count)
        cache_hit_count = 0

    app.loop.call_later(1, object_count_info)


asyncio.get_event_loop().call_later(1, object_count_info)

web.run_app(
    app,
    host=cfg.get('bind', 'host', fallback='0.0.0.0'),
    port=cfg.getint('bind', 'port', fallback=8080)
)
