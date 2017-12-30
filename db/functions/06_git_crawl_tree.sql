-- <PYTHON ONLY> --
CREATE OR REPLACE FUNCTION git_crawl_tree(root_tree_hash TEXT)
    RETURNS TABLE (
      hash TEXT,
      type objtype,
      name TEXT
    )
AS $BODY$
import plpy

def lookup_tree(h):
    return plpy.execute("SELECT t.leaf as leaf, " +
                        "(h.type)::TEXT as type," +
                        " t.name FROM" +
                        (" git_lookup_tree('%s') t INNER JOIN" % h) +
                        " headers h ON (h.hash = t.leaf)")


def crawl_tree(h, typ, name, pname):
    stack = [(h, typ, name, pname)]

    while stack:
        node = stack.pop()
        ppath = node[3] + ('' if node[3].endswith('/') else '/') + node[2]
        yield {"hash": node[0], "type": node[1], "name": ppath}
        if node[1] == 'tree':
            try:
                tree_iter = lookup_tree(node[0])
            except Exception as e:
                raise plpy.Fatal("Failed to load tree for %s (%s)" % (node[0], str(e)))
            for child in tree_iter:
                stack.append((child['leaf'], child['type'], child['name'], ppath))

return crawl_tree(root_tree_hash, 'tree', '', '')
$BODY$
LANGUAGE 'plpython3u';
-- </PYTHON ONLY> --
