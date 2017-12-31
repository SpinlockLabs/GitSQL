CREATE OR REPLACE FUNCTION git_load_object(target TEXT)
    RETURNS BYTEA
AS $BODY$
DECLARE
    blob BYTEA;
BEGIN
    SELECT content INTO blob FROM headers WHERE hash = target;
    RETURN blob;
END;
$BODY$
LANGUAGE 'plpgsql';
