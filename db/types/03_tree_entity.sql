CREATE OR REPLACE FUNCTION __do_create_tree_entity_type__()
    RETURNS VOID
AS $BODY$

BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'tree_entity') THEN
        CREATE TYPE tree_entity AS (
            parent TEXT,
            hash TEXT,
            name TEXT,
            path TEXT,
            type objtype,
            level INT
        );
    END IF;
END;

$BODY$
LANGUAGE 'plpgsql';

SELECT __do_create_tree_entity_type__();

DROP FUNCTION __do_create_tree_entity_type__();
