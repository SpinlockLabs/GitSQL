CREATE OR REPLACE FUNCTION git_commit_log(commitish TEXT)
  RETURNS SETOF "commits"
AS $BODY$
DECLARE 
  head TEXT;
BEGIN
  head := git_resolve_ref(commitish);
  
  RETURN QUERY
  WITH RECURSIVE tree(hash, parent, depth) AS (
	SELECT
    "hash",
    "parent",
    1
	FROM "commits" c
	WHERE "hash" = head
    UNION ALL
    SELECT c.hash, c.parent, t.depth + 1
    FROM "commits" c, tree t
    WHERE t.parent @> array[c.hash]
  )
  SELECT commits.* FROM tree INNER JOIN "commits" ON tree.hash = commits.hash
  ORDER BY tree.depth ASC;
END
$BODY$
LANGUAGE 'plpgsql';
