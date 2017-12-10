CREATE OR REPLACE FUNCTION git_resolve_ref(ref_name TEXT)
  RETURNS TEXT
AS $BODY$
DECLARE 
  tmp TEXT;
  current TEXT;
BEGIN
  tmp := ref_name;
  WHILE tmp != '' AND tmp != null OR (tmp = 'HEAD' OR tmp LIKE '%/%') LOOP
    SELECT "target" INTO tmp FROM "refs" WHERE "name" = tmp;
  END LOOP;
  RETURN tmp;
END
$BODY$
LANGUAGE 'plpgsql';