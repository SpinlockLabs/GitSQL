CREATE OR REPLACE FUNCTION git_lookup_tree(tree_hash TEXT)
    RETURNS TABLE (
        parent TEXT,
        mode TEXT,
        name TEXT,
        leaf TEXT
    )
AS $BODY$
DECLARE 
  tmp_content BYTEA;
BEGIN
    SELECT "content" INTO tmp_content FROM "headers" WHERE "hash" = tree_hash AND type = 'tree';
    RETURN QUERY
    SELECT * FROM git_parse_tree(tree_hash, tmp_content);
END
$BODY$
LANGUAGE 'plpgsql';
