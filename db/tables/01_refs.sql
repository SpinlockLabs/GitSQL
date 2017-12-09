CREATE TABLE refs (
  name TEXT NOT NULL,
  target TEXT NOT NULL,
  CONSTRAINT refs_pkey PRIMARY KEY (name)
);

COMMENT ON TABLE refs
IS 'Git References';
