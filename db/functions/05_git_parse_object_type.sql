CREATE OR REPLACE FUNCTION git_parse_object_type(blob BYTEA)
  RETURNS TEXT
IMMUTABLE
AS $BODY$
DECLARE
  type    TEXT;
  _header TEXT;
BEGIN
  _header := substring(blob FROM 0 FOR position('\000'::BYTEA IN blob));
  type := substring(_header FROM 0 FOR position(' ' IN _header));
  RETURN type;
END;
$BODY$
LANGUAGE 'plpgsql';
