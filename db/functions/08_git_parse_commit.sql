CREATE OR REPLACE FUNCTION git_parse_commit(commit_hash TEXT, blob BYTEA)
    RETURNS "commit"
AS $BODY$
DECLARE
    tree TEXT;
    parent TEXT[];
    author TEXT;
    committer TEXT;
    author_time TIMESTAMP WITH TIME ZONE;
    commit_time TIMESTAMP WITH TIME ZONE;
    message TEXT;
    pgp TEXT;
    blob TEXT;
    meta TEXT;
BEGIN
SELECT encode(content, 'escape') INTO blob FROM headers WHERE hash = commit_hash;

message := substring(blob from position(E'\n\n' in blob) + 2);
meta := substring(blob from 0 for position(E'\n\n' in blob));

tree := (regexp_matches(meta, 'tree ([^\s]+)'))[1];
parent := COALESCE(
    (SELECT regexp_matches(meta, 'parent ([^\s]+)') AS regexp_matches),
    ARRAY[]::TEXT[]
);

committer := (regexp_matches(meta, 'committer ([^\n]+\>)'))[1];
author := (regexp_matches(meta, 'author ([^\n]+\>)'))[1];

author_time := to_timestamp((regexp_matches(meta, 'author (?:[^\n]+) (([\d])+)'::TEXT))[1]::BIGINT);
commit_time := to_timestamp((regexp_matches(meta, 'committer (?:[^\n]+) (([\d])+)'::TEXT))[1]::BIGINT);
pgp := (regexp_matches(meta, '-----BEGIN.*SIGNATURE-----'))[1];

RETURN ROW(
    commit_hash,
    tree,
    parent,
    author,
    committer,
    author_time,
    commit_time,
    message,
    pgp
)::commit;
END
$BODY$
LANGUAGE 'plpgsql';
