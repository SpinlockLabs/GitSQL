CREATE OR REPLACE FUNCTION git_lookup_tree_item_at(path TEXT, commit_hash TEXT)
    RETURNS TEXT as $BODY$
DECLARE
  parts TEXT[];
  tree_hash TEXT;
  child_tree_hash TEXT;
BEGIN

parts := regexp_split_to_array(path, '/');

IF array_length(parts, 1) = 0 THEN
  RETURN NULL;
END IF;

IF parts[1] = '' THEN
  parts = array_remove(parts, parts[1]);
END IF;
SELECT tree INTO tree_hash FROM git_lookup_commit(commit_hash);

IF tree_hash IS NULL OR tree_hash = '' THEN
  RETURN NULL;
END IF;

WHILE parts IS NOT NULL AND array_length(parts, 1) > 0 LOOP
  child_tree_hash := NULL;
  SELECT hash INTO child_tree_hash FROM git_lookup_tree(tree_hash) WHERE name = parts[1];
  IF child_tree_hash IS NULL THEN
    RETURN NULL;
  END IF;
  tree_hash := child_tree_hash;
  parts = array_remove(parts, parts[1]);
END LOOP;

IF tree_hash IS NULL THEN
  RETURN NULL;
END IF;

RETURN tree_hash;

END;
$BODY$
LANGUAGE 'plpgsql';
