CREATE INDEX IF NOT EXISTS "objects.types"
  ON objects(git_fast_object_type(hash));
