CREATE TABLE IF NOT EXISTS refs (
  name TEXT NOT NULL,
  target TEXT NOT NULL,
  CONSTRAINT refs_pkey PRIMARY KEY (name)
);

COMMENT ON TABLE refs
IS 'Git References';

COMMENT ON COLUMN refs.name
IS 'Reference Name (ex: refs/heads/master)';

COMMENT ON COLUMN refs.target
IS 'Reference Target (ex: d1dee1299555cbd126755df887db06503e0ab811 or refs/heads/master)';
