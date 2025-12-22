-- ============================================================
-- 015_matrix_enforce_storage_location_role_views.sql
-- Enforce role-based exposure for storage_dim + location_dim
-- aligned to the access matrix.
--
-- Matrix interpretation (non-internal roles):
--   storage_dim:
--     - allowed: storage_id (join key), storage_details_id (ID)
--     - NOT allowed: medium, temperature*, rack_id, position_id, storage_location
--   location_dim:
--     - allowed: location_id (join key), country, collection_site, latitude_dd, longitude_dd
--     - coord_status/coord_format: NOT in matrix -> keep non-internal FALSE
--
-- IMPORTANT:
-- - Commit policy updates BEFORE refresh.
-- - Drop role-facing views BEFORE refresh to avoid CREATE OR REPLACE column reorder errors.
-- ============================================================

-- ------------------------------------------------------------
-- 1) POLICY UPDATES (commit first)
-- ------------------------------------------------------------
BEGIN;

-- ---------- storage_dim ----------
-- Join key + "ID" should be visible to all role schemas (including public)
UPDATE admin.view_column_policy
SET allow_public = TRUE,
    allow_nongenresearch = TRUE,
    allow_genresearch = TRUE,
    allow_restoration = TRUE,
    allow_farmbreed = TRUE,
    -- internal already allowed; leave as-is if you prefer, but keeping true is fine
    allow_internal = TRUE,
    notes = COALESCE(notes,'') || ' | 015: storage join+id (matrix)'
WHERE base_schema='structured'
  AND base_table='storage_dim'
  AND column_name IN ('storage_id','storage_details_id');

-- Internal-only storage fields (explicitly turn OFF for non-internal roles)
UPDATE admin.view_column_policy
SET allow_public = FALSE,
    allow_nongenresearch = FALSE,
    allow_genresearch = FALSE,
    allow_restoration = FALSE,
    allow_farmbreed = FALSE,
    -- internal stays as-is (should be TRUE); if you want force true, set allow_internal=TRUE
    notes = COALESCE(notes,'') || ' | 015: storage internals internal-only (matrix)'
WHERE base_schema='structured'
  AND base_table='storage_dim'
  AND column_name IN (
    'position_id','rack_id','storage_location',
    'temperature_c','temperature_c_raw','medium'
  );

-- ---------- location_dim ----------
-- Join key + matrix-allowed fields visible to all role schemas (including public)
UPDATE admin.view_column_policy
SET allow_public = TRUE,
    allow_nongenresearch = TRUE,
    allow_genresearch = TRUE,
    allow_restoration = TRUE,
    allow_farmbreed = TRUE,
    allow_internal = TRUE,
    notes = COALESCE(notes,'') || ' | 015: location join+public fields (matrix)'
WHERE base_schema='structured'
  AND base_table='location_dim'
  AND column_name IN ('location_id','country','collection_site','latitude_dd','longitude_dd');

-- coord_status/coord_format not in matrix -> keep non-internal FALSE (internal can still have it)
UPDATE admin.view_column_policy
SET allow_public = FALSE,
    allow_nongenresearch = FALSE,
    allow_genresearch = FALSE,
    allow_restoration = FALSE,
    allow_farmbreed = FALSE,
    notes = COALESCE(notes,'') || ' | 015: coord meta internal-only (matrix)'
WHERE base_schema='structured'
  AND base_table='location_dim'
  AND column_name IN ('coord_status','coord_format');

COMMIT;

-- ------------------------------------------------------------
-- 2) DROP ROLE-FACING VIEWS (avoid CREATE OR REPLACE conflicts)
--    Drop dependents first if you have any (catalog_plus later),
--    but at this step we only need to refresh dims cleanly.
-- ------------------------------------------------------------
BEGIN;

DROP VIEW IF EXISTS public_structured.storage_dim;
DROP VIEW IF EXISTS nongenresearch.storage_dim;
DROP VIEW IF EXISTS genresearch.storage_dim;
DROP VIEW IF EXISTS restoration.storage_dim;
DROP VIEW IF EXISTS farmbreed.storage_dim;

DROP VIEW IF EXISTS public_structured.location_dim;
DROP VIEW IF EXISTS nongenresearch.location_dim;
DROP VIEW IF EXISTS genresearch.location_dim;
DROP VIEW IF EXISTS restoration.location_dim;
DROP VIEW IF EXISTS farmbreed.location_dim;

COMMIT;

-- ------------------------------------------------------------
-- 3) REFRESH ROLE VIEWS FROM POLICY
-- ------------------------------------------------------------
SELECT admin.refresh_role_views();

-- ------------------------------------------------------------
-- 4) VERIFICATION
-- ------------------------------------------------------------

-- A) Confirm keys exist in each role schema (should return 10 rows)
SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema IN ('public_structured','nongenresearch','genresearch','restoration','farmbreed')
  AND table_name IN ('storage_dim','location_dim')
  AND column_name IN ('storage_id','location_id')
ORDER BY table_schema, table_name, column_name;

-- B) Confirm storage internals are NOT exposed in non-internal roles (should be 0 rows)
SELECT table_schema, column_name
FROM information_schema.columns
WHERE table_schema IN ('public_structured','nongenresearch','genresearch','restoration','farmbreed')
  AND table_name='storage_dim'
  AND column_name IN ('position_id','rack_id','storage_location','temperature_c','temperature_c_raw','medium')
ORDER BY table_schema, column_name;

-- C) Confirm coord meta NOT exposed in non-internal roles (should be 0 rows)
SELECT table_schema, column_name
FROM information_schema.columns
WHERE table_schema IN ('public_structured','nongenresearch','genresearch','restoration','farmbreed')
  AND table_name='location_dim'
  AND column_name IN ('coord_status','coord_format')
ORDER BY table_schema, column_name;
