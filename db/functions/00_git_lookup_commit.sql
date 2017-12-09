CREATE OR REPLACE FUNCTION git_lookup_commit(commit_hash TEXT)
  RETURNS SETOF "commits"
AS $BODY$

SELECT *
FROM "commits"
WHERE "hash" = commit_hash

$BODY$
LANGUAGE SQL;
