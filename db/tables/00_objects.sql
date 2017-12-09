CREATE TABLE objects (
  hash TEXT NOT NULL,
  content BYTEA NOT NULL,
  CONSTRAINT hash PRIMARY KEY (hash),
  CONSTRAINT "object-content-valid" CHECK (
    encode(digest(content, 'sha1'), 'hex') = hash
  )
);

COMMENT ON TABLE objects
IS 'Git Objects';

COMMENT ON CONSTRAINT "object-content-valid"
ON objects
IS 'Checks if the object hash matches the content hash.';
