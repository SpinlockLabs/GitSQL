DROP VIEW IF EXISTS commits CASCADE;

CREATE VIEW commits AS WITH raw_commit_texts AS (
    SELECT
      headers.hash,
      encode(headers.content, 'escape'::TEXT) AS info
    FROM headers
    WHERE headers.type::TEXT = 'commit'::TEXT
),
subsections AS (
    SELECT
      raw_commit_texts.hash,
      substring(raw_commit_texts.info from position(E'\n\n' in raw_commit_texts.info) + 2) AS message,
      substring(raw_commit_texts.info from 0 for position(E'\n\n' in raw_commit_texts.info)) AS meta
    FROM raw_commit_texts
)
SELECT
  subsections.hash,
  (regexp_matches(subsections.meta, 'tree ([^\s]+)'))[1] AS tree,
  COALESCE(
      (SELECT regexp_matches(subsections.meta, 'parent ([^\s]+)') AS regexp_matches),
      ARRAY[]::TEXT[]
  ) AS parent,
  (regexp_matches(subsections.meta, 'author ([^\n]+\>)'))[1] AS author,
  (regexp_matches(subsections.meta, 'committer ([^\n]+\>)'))[1] AS committer,
  to_timestamp((regexp_matches(subsections.meta, 'author (?:[^\n]+) (([\d])+)'::TEXT))[1]::BIGINT) AS author_time,
  to_timestamp((regexp_matches(subsections.meta, 'committer (?:[^\n]+) (([\d])+)'::TEXT))[1]::BIGINT) AS commit_time,
  subsections.message,
  (regexp_matches(subsections.meta, '-----BEGIN.*SIGNATURE-----'))[1] as pgp
FROM subsections
ORDER BY (
  to_timestamp((regexp_matches(subsections.meta, 'committer (?:[^\n]+) (([\d])+)'))[1]::BIGINT)
) DESC;
