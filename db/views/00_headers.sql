DROP VIEW IF EXISTS headers CASCADE;

CREATE VIEW headers AS WITH _headers AS (
    SELECT
      objects.hash,
      substring(objects.content from 0 for position('\000'::BYTEA in objects.content))::TEXT AS header,
      objects.content
    FROM objects
)
SELECT
  _headers.hash,
  substring(_headers.header from 0 for position(' ' in _headers.header)) AS type,
  substring(_headers.header from position(' ' in _headers.header))::INTEGER AS size,
  substring(_headers.content from position(E'\\000' in _headers.content) + 1) AS content
FROM _headers;
