CREATE OR REPLACE FUNCTION __do_create_tree_entry_type__()
    RETURNS VOID
AS $BODY$

BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'tree_entry') THEN
        CREATE TYPE tree_entry AS (
            parent TEXT,
            mode TEXT,
            name TEXT,
            hash TEXT
        );
    END IF;
END;

$BODY$
LANGUAGE 'plpgsql';

SELECT __do_create_tree_entry_type__();

DROP FUNCTION __do_create_tree_entry_type__();
