CREATE OR REPLACE FUNCTION git_lookup_commit(commit_hash TEXT)
  RETURNS "commit"
AS $BODY$
DECLARE
  blob BYTEA;
BEGIN

SELECT
content INTO blob
FROM "headers"
WHERE "hash" = commit_hash
AND "type" = 'commit';

RETURN git_parse_commit(commit_hash, blob);
END;
$BODY$
LANGUAGE 'plpgsql';
