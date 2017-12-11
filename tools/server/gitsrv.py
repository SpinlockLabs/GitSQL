#!/usr/bin/env python3
from sys import stderr, exit, argv
from zlib import compress
from configparser import ConfigParser

from japronto import Application
from psycopg2cffi import connect


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


def map_repo_to_db(name: str):
    mapped = cfg.get('serve', name, fallback=None)
    if mapped is None:
        raise NoSuchRepository(name)
    return mapped


def grab_connection(request):
    repo_name = request.match_dict['repo']
    if repo_name is None:
        raise NoSuchRepository(None)

    db_name = map_repo_to_db(repo_name)
    conn_info = db_conn_info + (" dbname='{0}'".format(db_name))
    conn = connect(conn_info)
    return conn


def handle_object_route(request):
    conn = grab_connection(request)
    cursor = conn.cursor()
    try:
        prefix = request.match_dict['hash_prefix']
        suffix = request.match_dict['hash_suffix']
        object_hash = prefix + suffix

        cursor.execute("SELECT content FROM objects WHERE hash = %s", (object_hash,))
        binary = cursor.fetchone()[0]
        return request.Response(body=compress(binary.tobytes()))
    finally:
        cursor.close()
        conn.close()


def handle_repo_not_found(request, exception):
    return request.Response(text='Repository Not Found.', code=404)


def handle_refs_route(request):
    conn = grab_connection(request)
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
        conn.close()


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

app.run()
