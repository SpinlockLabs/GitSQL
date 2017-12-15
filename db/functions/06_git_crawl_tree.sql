CREATE OR REPLACE FUNCTION git_crawl_tree(root_tree_hash TEXT)
    RETURNS TABLE (
      hash TEXT
    )
AS $BODY$
import plpy

def lookup_tree(h):
    return plpy.execute("SELECT t.leaf, h.type FROM git_lookup_tree('%s') t INNER JOIN headers h ON (h.hash = t.leaf)" % h)


def crawl_tree(h, typ):
    yield h

    if typ != 'tree':
        return

    for row in lookup_tree(h):
        for n in crawl_tree(row["leaf"], row["type"]):
            yield n

return crawl_tree(root_tree_hash, 'tree')
$BODY$
LANGUAGE 'plpythonu';
