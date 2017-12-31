CREATE OR REPLACE FUNCTION __do_create_internal_tree_stack__()
    RETURNS VOID
AS $BODY$

BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'tree_stack_entry') THEN
        CREATE TYPE tree_stack_entry AS (
            hash TEXT,
            name TEXT,
            type objtype,
            parent_name TEXT,
            parent_hash TEXT,
            level INT
        );
    END IF;
END;

$BODY$
LANGUAGE 'plpgsql';

SELECT __do_create_internal_tree_stack__();

DROP FUNCTION __do_create_internal_tree_stack__();
