-- ============================================================
-- 013_fix_storage_dim_exposure.sql
-- Restrict storage_dim to internal only (remove from role schemas)
-- ============================================================

BEGIN;

-- Lock down ALL columns in storage_dim for non-internal domains
UPDATE admin.view_column_policy
SET allow_public = FALSE,
    allow_nongenresearch = FALSE,
    allow_genresearch = FALSE,
    allow_restoration = FALSE,
    allow_farmbreed = FALSE,
    allow_internal = TRUE,
    notes = COALESCE(notes,'') || ' | patch013: storage_dim restricted to internal only'
WHERE base_schema='structured'
  AND base_table='storage_dim';

-- Rebuild views from policy
SELECT admin.refresh_role_views();

-- Re-grant SELECT on any newly created/replaced views (safe to rerun)
DO $$
DECLARE
  v RECORD;
BEGIN
  FOR v IN
    SELECT table_schema, table_name
    FROM information_schema.views
    WHERE table_schema IN ('public_structured','nongenresearch','genresearch','restoration','farmbreed')
  LOOP
    IF v.table_schema = 'public_structured' THEN
      EXECUTE format('GRANT SELECT ON %I.%I TO public_user;', v.table_schema, v.table_name);
    ELSIF v.table_schema = 'nongenresearch' THEN
      EXECUTE format('GRANT SELECT ON %I.%I TO nongenresearch_user;', v.table_schema, v.table_name);
    ELSIF v.table_schema = 'genresearch' THEN
      EXECUTE format('GRANT SELECT ON %I.%I TO genresearch_user;', v.table_schema, v.table_name);
    ELSIF v.table_schema = 'restoration' THEN
      EXECUTE format('GRANT SELECT ON %I.%I TO restoration_user;', v.table_schema, v.table_name);
    ELSIF v.table_schema = 'farmbreed' THEN
      EXECUTE format('GRANT SELECT ON %I.%I TO farmbreed_user;', v.table_schema, v.table_name);
    END IF;

    -- internal/app can read everything in view schemas
    EXECUTE format('GRANT SELECT ON %I.%I TO internal_user, kara_app;', v.table_schema, v.table_name);
  END LOOP;
END$$;

COMMIT;
