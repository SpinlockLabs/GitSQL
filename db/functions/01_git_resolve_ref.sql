CREATE OR REPLACE FUNCTION git_resolve_ref(ref_name TEXT)
  RETURNS TEXT
AS $BODY$
DECLARE 
  tmp TEXT;
BEGIN
  tmp := ref_name;
  WHILE tmp = 'HEAD' OR tmp LIKE '%/%' LOOP
    SELECT "target" INTO tmp FROM "refs" WHERE "name" = tmp;
  END LOOP;
  RETURN tmp;
END
$BODY$
LANGUAGE 'plpgsql';
