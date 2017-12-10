DROP VIEW IF EXISTS contents CASCADE;

CREATE VIEW contents AS SELECT
  objects.hash,
  substring(objects.content from position(E'\\000' in objects.content) + 1) AS content
FROM objects;
