from hashlib import sha1
from argparse import ArgumentParser

import sys
import pygit2

obj_prepared = 'PREPARE obj(TEXT, TEXT) AS ' + \
               'INSERT INTO objects VALUES ($1, decode($2, \'hex\'))' + \
               ' ON CONFLICT DO NOTHING;'

parser = ArgumentParser()
parser.add_argument("repository")
parser.add_argument("output")

parser.add_argument(
    "--no-prepared-header",
    help="Disables the PREPARE statements.",
    action="store_true"
)

parser.add_argument(
    "--update",
    help="Disables Truncation",
    action="store_true"
)

parser.add_argument(
    "--total",
    help="Total Number of Objects"
)

total = 0

args = parser.parse_args()

if args.total:
    total = int(args.total)

repo = pygit2.Repository(args.repository)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def show_info(cnt: int):
    if total == 0:
        eprint("{0} objects generated".format(cnt))
    else:
        percent = (cnt / total) * 100.0
        eprint("{0}% ({1} objects out of {2})".format(int(percent), cnt, total))


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


def generate_sql_object(oid):
    data = encode_git_object(oid)
    sql = "EXECUTE obj('" + str(oid) + "', '" + data.hex() + "');"
    return sql


def generate_sql_objects():
    if not args.no_prepared_header:
        yield obj_prepared

    count = 0
    for oid in repo:
        yield generate_sql_object(oid)
        count += 1
        show_info(count)


def generate_sql_ref(ref):
    sql = 'INSERT INTO "refs" ("name", "target") VALUES ('
    sql += "'" + ref.name + "'"
    sql += ", '" + str(ref.target) + "'"
    sql += ') ON CONFLICT ("name") DO UPDATE SET "target" = '
    sql += "'" + str(ref.target) + "';"
    return sql


def generate_sql_refs():
    for ref in repo.references:
        yield generate_sql_ref(repo.references[ref])
    yield generate_sql_ref(repo.lookup_reference("HEAD"))


def generate_sql_file():
    if not args.update:
        yield 'TRUNCATE "objects";'
        yield 'TRUNCATE "refs";'

    yield from generate_sql_objects()
    yield from generate_sql_refs()


if args.output == '-':
    for line in generate_sql_file():
        print(line)
else:
    with open(args.output, mode='w+') as file:
        file.seek(0)
        for line in generate_sql_file():
            file.write(line)
            file.write('\n')
