DROP FUNCTION IF EXISTS git_fast_object_type(TEXT) CASCADE;

CREATE OR REPLACE FUNCTION git_fast_object_type(_hash TEXT)
    RETURNS TEXT
    IMMUTABLE
AS $BODY$
DECLARE
  tmp_content BYTEA;
  tmp_type_cached OBJTYPE;
BEGIN
    SELECT "type" INTO tmp_type_cached FROM object_type_cache WHERE hash = _hash;

    IF tmp_type_cached IS NOT NULL THEN
        RETURN tmp_type_cached;
    END IF;

    SELECT "content" INTO tmp_content FROM "objects" WHERE hash = _hash;
    RETURN git_parse_object_type(tmp_content);
END
$BODY$
LANGUAGE 'plpgsql';
