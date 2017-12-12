#!/usr/bin/env python3
from configparser import ConfigParser
from sys import stderr, exit, argv
from zlib import compress

from japronto import Application
from psycopg2cffi.pool import SimpleConnectionPool

from concurrent.futures import ThreadPoolExecutor

import asyncio

executor = ThreadPoolExecutor(max_workers=10)


class NoSuchRepository(Exception):
    def __init__(self, repo):
        self.repo = repo


if len(argv) < 2:
    print("Usage: gitsrv.py <config>", file=stderr)
    exit(1)

cfg = ConfigParser()
cfg.read(argv[1])

if not cfg.has_section('connection'):
    print("ERROR: Connection configuration section missing.", file=stderr)
    exit(1)

db_conn_info = ''
for item in cfg.items('connection'):
    db_conn_info += " {}='{}'".format(item[0], item[1])
db_conn_info = db_conn_info.lstrip()

object_count = 0


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
    if not db_name in pools:
        conn_info = db_conn_info + (" dbname='{0}'".format(db_name))
        pools[db_name] = SimpleConnectionPool(1, 10, conn_info)
    pool = pools[db_name]
    return pool, pool.getconn()


def put_connection(pool, conn):
    pool.putconn(conn)


def fetch_object(repo_name, object_hash):
    cursor = None
    pool = None
    conn = None
    binary = None

    try:
        pool, conn = grab_connection(repo_name)
        cursor = conn.cursor()
        cursor.execute("SELECT content FROM objects WHERE hash = %s", (object_hash,))
        binary = cursor.fetchone()[0]
    finally:
        cursor.close()
        put_connection(pool, conn)
        return binary


async def handle_object_route(request):
    prefix = request.match_dict['hash_prefix']
    suffix = request.match_dict['hash_suffix']
    object_hash = prefix + suffix

    binary = await asyncio.get_event_loop().run_in_executor(
        executor,
        fetch_object,
        request.match_dict['repo'],
        object_hash
    )
    global object_count
    object_count += 1
    return request.Response(body=compress(binary.tobytes()))


def handle_repo_not_found(request, exception):
    return request.Response(text='Repository Not Found.', code=404)


def handle_refs_route(request):
    pool, conn = grab_connection(request.match_dict['repo'])
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

        return request.Response(text=result)
    finally:
        cursor.close()
        put_connection(pool, conn)


app = Application()

app.router.add_route(
    '/{repo}/objects/{hash_prefix}/{hash_suffix}',
    handle_object_route
)

app.router.add_route(
    '/{repo}/info/refs',
    handle_refs_route
)

app.add_error_handler(
    NoSuchRepository,
    handle_repo_not_found
)


def object_count_info():
    global object_count
    if object_count > 0:
        print("[Statistics] %i objects were fetched" % object_count)
        object_count = 0
    app.loop.call_later(1, object_count_info)


app.loop.call_later(1, object_count_info)

app.run()
