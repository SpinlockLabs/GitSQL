CREATE OR REPLACE FUNCTION __do_create_commit_type__()
  RETURNS VOID
AS $BODY$

BEGIN
  IF NOT EXISTS(SELECT 1
                FROM pg_type
                WHERE typname = 'commit')
  THEN
    CREATE TYPE commit AS (
      hash        TEXT,
      tree        TEXT,
      parent      TEXT[],
      author      TEXT,
      committer   TEXT,
      author_time TIMESTAMP WITH TIME ZONE,
      commit_time TIMESTAMP WITH TIME ZONE,
      message     TEXT,
      pgp         TEXT
    );
  END IF;
END;

$BODY$
LANGUAGE 'plpgsql';

SELECT __do_create_commit_type__();

DROP FUNCTION __do_create_commit_type__();
