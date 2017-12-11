CREATE OR REPLACE FUNCTION __do_create_object_type__()
  RETURNS VOID
AS $BODY$

BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'objtype') THEN
    CREATE TYPE objtype AS ENUM ('commit', 'tree', 'blob', 'tag');
  END IF;
END;

$BODY$
LANGUAGE 'plpgsql';

SELECT __do_create_object_type__();

DROP FUNCTION __do_create_object_type__();
