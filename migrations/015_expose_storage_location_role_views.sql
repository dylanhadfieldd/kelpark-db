-- ============================================================
-- 015_expose_storage_location_role_views.sql
-- Expose storage_dim + location_dim into role-facing schemas
-- so the app can join via storage_id and location_id.
--
-- IMPORTANT:
-- - We commit policy changes BEFORE refresh to avoid rollback if refresh fails.
-- - We drop the affected role-facing views BEFORE refresh so Postgres doesn't
--   error on column order/name changes (CREATE OR REPLACE limitation).
-- ============================================================

-- ------------------------------------------------------------
-- 1) POLICY UPDATES (commit first)
-- ------------------------------------------------------------
BEGIN;

-- storage_dim: allow join key + public-safe display fields
UPDATE admin.view_column_policy
SET allow_public = TRUE,
    allow_nongenresearch = TRUE,
    allow_genresearch = TRUE,
    allow_restoration = TRUE,
    allow_farmbreed = TRUE,
    notes = COALESCE(notes,'') || ' | 015: expose storage_dim'
WHERE base_schema='structured'
  AND base_table='storage_dim'
  AND column_name IN (
    'storage_id',
    'storage_details_id',
    'position_id',
    'rack_id',
    'storage_location',
    'temperature_c',
    'temperature_c_raw',
    'medium'
  );

-- location_dim: allow join key + public-safe display fields
UPDATE admin.view_column_policy
SET allow_public = TRUE,
    allow_nongenresearch = TRUE,
    allow_genresearch = TRUE,
    allow_restoration = TRUE,
    allow_farmbreed = TRUE,
    notes = COALESCE(notes,'') || ' | 015: expose location_dim'
WHERE base_schema='structured'
  AND base_table='location_dim'
  AND column_name IN (
    'location_id',
    'country',
    'collection_site',
    'latitude_dd',
    'longitude_dd',
    'coord_status',
    'coord_format'
  );

COMMIT;

-- ------------------------------------------------------------
-- 2) DROP ROLE-FACING VIEWS (avoid CREATE OR REPLACE column rename conflicts)
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
-- 4) QUICK VERIFICATION (optional, but recommended to leave in-file)
-- ------------------------------------------------------------
-- Confirm keys exist in each role schema (should return 10 rows: 5 schemas x 2 keys)
SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema IN ('public_structured','nongenresearch','genresearch','restoration','farmbreed')
  AND table_name IN ('storage_dim','location_dim')
  AND column_name IN ('storage_id','location_id')
ORDER BY table_schema, table_name, column_name;
