BEGIN;

-- storage_dim should not exist in role schemas anymore
DROP VIEW IF EXISTS public_structured.storage_dim;
DROP VIEW IF EXISTS nongenresearch.storage_dim;
DROP VIEW IF EXISTS genresearch.storage_dim;
DROP VIEW IF EXISTS restoration.storage_dim;
DROP VIEW IF EXISTS farmbreed.storage_dim;

COMMIT;
