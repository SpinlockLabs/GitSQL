DROP FUNCTION IF EXISTS git_parse_tree(TEXT, BYTEA);

CREATE OR REPLACE FUNCTION git_parse_tree(tree_hash TEXT, blob BYTEA)
    RETURNS SETOF tree_entry
AS $BODY$
DECLARE
    buffer BYTEA;
    b INT;
    inside_sha INT;
    tot_len INT;
    id INT;
    tmp BYTEA;
    headers TEXT;
    mode TEXT;
    name TEXT;
BEGIN
    inside_sha := 0;
    id := 0;
    tmp := E'\\000';
    tot_len := octet_length(blob);
    buffer := E'';

    LOOP
        IF id = tot_len THEN
            EXIT;
        END IF;

        b = get_byte(blob, id);
        buffer = buffer || set_byte(tmp, 0, b);

        IF b = 0 AND inside_sha = 0 THEN
            inside_sha = id;
        END IF;

        IF inside_sha > 0 AND (id - inside_sha) = 20 THEN
            headers = substring(buffer for (position(E'\\000' in buffer)));
            mode = substring(headers for (position(E' ' in headers) - 1));
            name = substring(headers from (octet_length(mode) + 2));
            name = substring(name for (octet_length(name) - 4));

            RETURN NEXT (
                tree_hash,
                mode::TEXT,
                name::TEXT,
                encode(substring(buffer from octet_length(headers) - 2), 'hex')
            )::tree_entry;

            buffer = b'';
            inside_sha = 0;
        END IF;

        id = id + 1;
    END LOOP;
END;
$BODY$
LANGUAGE 'plpgsql';
