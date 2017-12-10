DROP VIEW IF EXISTS headers CASCADE;

CREATE VIEW headers AS
  SELECT
    hash,
    git_parse_object_type(content) as type
  FROM objects;
