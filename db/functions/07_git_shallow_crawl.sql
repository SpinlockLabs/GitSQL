-- <PYTHON ONLY> --
CREATE OR REPLACE FUNCTION git_shallow_crawl(commit_hash TEXT)
    RETURNS TABLE (
      hash TEXT,
      type TEXT
    )
AS $BODY$
import plpy

def lookup_tree(h):
    query = "SELECT t.hash as leaf, (h.type)::TEXT as type FROM git_lookup_tree('%s') t INNER JOIN headers h ON (h.hash = t.hash)" % h
    result = plpy.execute(query)
    return result

def lookup_commit(h):
    return plpy.execute("SELECT tree FROM git_lookup_commit('%s')" % h)[0]

def crawl_tree(h, typ):
    stack = [(h, typ)]

    while stack:
        node = stack.pop()
        yield {"hash": node[0], "type": node[1]}
        tree_iter = None
        if node[1] == 'tree':
            try:
                tree_iter = lookup_tree(node[0])
            except Exception as e:
                raise plpy.Fatal("Failed to load tree for %s (%s)" % (node[0], str(e)))
            for child in tree_iter:
                stack.append((child['leaf'], child['type']))

def crawl_commit(cm_hash):
    cm = lookup_commit(cm_hash)
    yield {"hash": cm_hash, "type": "commit"}
    for h in crawl_tree(cm["tree"], 'tree'):
        yield h

return crawl_commit(commit_hash)
$BODY$
LANGUAGE 'plpython3u';
-- </PYTHON ONLY> --
