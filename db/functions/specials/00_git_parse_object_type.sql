CREATE OR REPLACE FUNCTION git_parse_object_type(blob BYTEA)
  RETURNS objtype
IMMUTABLE
AS $BODY$
DECLARE
  type    TEXT;
  _header TEXT;
BEGIN
  _header := trim(both ' ' FROM encode((substring(blob FROM 0 FOR position('\000'::BYTEA IN blob))), 'escape'));
  type := trim(both ' ' FROM substring(_header FROM 0 FOR position(' ' IN _header)));

  IF type = 'commit' THEN
    RETURN 'commit'::objtype;
  ELSEIF type = 'tree' THEN
    RETURN 'tree'::objtype;
  ELSEIF type = 'tag' THEN
    RETURN 'tag'::objtype;
  ELSE
    RETURN 'blob'::objtype;
  END IF;
END;
$BODY$
LANGUAGE 'plpgsql';
