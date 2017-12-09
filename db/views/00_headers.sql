CREATE VIEW headers AS WITH _headers AS (
    SELECT
      objects.hash,
      substring(objects.content from 0 for position('\000'::BYTEA in objects.content))::TEXT AS header
    FROM objects
)
SELECT
  _headers.hash,
  substring(_headers.header from 0 for position(' ' in _headers.header)) AS type,
  substring(_headers.header from position(' ' in _headers.header))::INTEGER AS size
FROM _headers;
