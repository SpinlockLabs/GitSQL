DROP VIEW IF EXISTS headers CASCADE;

CREATE VIEW headers AS
  SELECT
    hash,
    (git_parse_object_type(content))::objtype as type,
    substring(content from position(E'\\000' in content) + 1) as content
  FROM objects;
