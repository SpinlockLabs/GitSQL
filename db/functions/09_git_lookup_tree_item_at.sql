-- <PYTHON ONLY> --
CREATE OR REPLACE FUNCTION git_lookup_tree_item_at(path TEXT, commit_hash TEXT)
    RETURNS TEXT
AS $BODY$
import plpy


def lookup_tree(h):
    return plpy.execute("SELECT t.leaf as leaf, " +
                        "(h.type)::TEXT as type," +
                        " t.name FROM" +
                        (" git_lookup_tree('%s') t INNER JOIN" % h) +
                        " headers h ON (h.hash = t.leaf)")


def lookup_commit(c):
    return plpy.execute("SELECT * FROM git_lookup_commit('%s')" % c)


parts = str(path).split('/')
if len(parts[0]) == 0:
    parts.pop()

commit = lookup_commit(commit_hash)[0]

current_tree = commit["tree"]

for part in parts:
    tree_to_find = current_tree
    tree = lookup_tree(tree_to_find)
    current_tree = None
    for item in tree:
        if item["name"] != part:
            continue
        current_tree = item["leaf"]
        break
    else:
        break

    if current_tree is None:
        break

if current_tree is None:
    return None

return current_tree
$BODY$
LANGUAGE 'plpython3u';
-- </PYTHON ONLY> --
