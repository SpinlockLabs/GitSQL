DROP FUNCTION IF EXISTS git_lookup_tree(TEXT);

CREATE OR REPLACE FUNCTION git_lookup_tree(tree_hash TEXT)
    RETURNS SETOF "tree_entry"
AS $BODY$
DECLARE 
  tmp_content BYTEA;
BEGIN
    SELECT "content" INTO tmp_content FROM "headers" WHERE "hash" = tree_hash AND type = 'tree';

    IF tmp_content IS NOT NULL THEN
        RETURN QUERY
        SELECT * FROM git_parse_tree(tree_hash, tmp_content);
    END IF;

    RETURN QUERY
    SELECT hash as parent, hash as mode, hash as name, hash as leaf FROM commits WHERE 1=0; 
END
$BODY$
LANGUAGE 'plpgsql';
