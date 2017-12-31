DROP FUNCTION IF EXISTS git_crawl_tree(TEXT);

CREATE OR REPLACE FUNCTION git_crawl_tree(root_tree_hash TEXT)
    RETURNS SETOF "tree_entity"
AS $BODY$
DECLARE
    stack tree_stack_entry[];
    current_hash TEXT;
    current_type OBJTYPE;
    current_name TEXT;
    current_parent_name TEXT;
    current_parent_hash TEXT;
    current_level INT;
    current_path TEXT;
    current_entry tree_stack_entry;
    child tree_entry;
    child_type OBJTYPE;
BEGIN
    current_parent_name := '';
    current_hash := root_tree_hash;
    current_name := '/';
    current_parent_hash := '';
    current_path := '';
    current_level = 0;
    current_type := 'blob'::objtype;
    current_entry := NULL;

    SELECT type INTO current_type FROM headers WHERE hash = current_hash;

    IF current_type != 'tree'::OBJTYPE THEN
        RETURN;
    END IF;

    stack := ARRAY[
        ROW (
            current_hash,
            '/',
            current_type,
            '',
            '',
            0
        )::tree_stack_entry
    ];

    LOOP
        IF array_length(stack, 1) = 0 OR array_length(stack, 1) IS NULL THEN
            EXIT;
        END IF;

        current_entry = stack[1];
        stack = stack[2:array_length(stack, 1)];

        current_hash = current_entry.hash;
        current_name = current_entry.name;
        current_type = current_entry.type;
        current_parent_name = current_entry.parent_name;
        current_parent_hash = current_entry.parent_hash;
        current_path = current_parent_name;
        current_level = current_entry.level;

        IF current_parent_hash IS NOT NULL AND length(current_parent_hash) = 0 THEN
            current_parent_hash = NULL;
        END IF;

        IF current_name != '/' AND substring(current_path FROM length(current_path) - 1) != '/' THEN
            current_path = current_path || '/';
        END IF;

        current_path = current_path || current_name;

        RETURN NEXT (
            current_parent_hash,
            current_hash,
            current_name,
            current_path,
            current_type,
            current_level
        )::tree_entity;

        IF current_type = 'tree'::objtype THEN
            FOR child IN
            SELECT * FROM git_lookup_tree(current_hash)
            LOOP
                SELECT type INTO child_type FROM headers WHERE hash = child.hash;

                IF child_type IS NULL OR child.name IS NULL THEN
                    CONTINUE;
                END IF;

                stack = array_append(stack, ROW (
                    child.hash,
                    child.name,
                    child_type,
                    current_path,
                    current_hash,
                    current_level + 1
                )::tree_stack_entry);
            END LOOP;
        END IF;
    END LOOP;
END;
$BODY$
LANGUAGE 'plpgsql';
