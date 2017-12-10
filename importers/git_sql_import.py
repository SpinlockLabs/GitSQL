from hashlib import sha1
from argparse import ArgumentParser

from pygit2 import Repository

parser = ArgumentParser()
parser.add_argument("repository")
parser.add_argument("output")
parser.add_argument(
    "--update",
    help="Disables Truncation",
    action="store_true"
)

args = parser.parse_args()

repo = Repository(args.repository)


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


def encode_git_object(oid):
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
            "Invalid Object Encoding: expected {}, encoded {}" % str(oid) % calc_hash)
    return encoded


def generate_sql_object(oid):
    data = encode_git_object(oid)
    rid = str(oid)
    sql = 'INSERT INTO "objects" ("hash", "content") VALUES ('
    sql += "'" + rid + "', "
    sql += "decode('"
    sql += data.hex()
    sql += "', 'hex')) ON CONFLICT DO NOTHING;"
    return sql


def generate_sql_objects():
    for oid in repo:
        yield generate_sql_object(oid)


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


with open(args.output, mode='w+') as file:
    file.seek(0)
    for line in generate_sql_file():
        file.write(line)
        file.write('\n')
