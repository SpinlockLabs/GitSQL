CREATE INDEX IF NOT EXISTS "objects.types"
  ON objects(git_parse_object_type(content));
