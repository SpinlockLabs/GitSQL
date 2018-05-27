DROP MATERIALIZED VIEW IF EXISTS object_type_cache CASCADE;

CREATE MATERIALIZED VIEW object_type_cache AS
  SELECT
    hash,
    (git_parse_object_type(content))::objtype as type
  FROM objects;
