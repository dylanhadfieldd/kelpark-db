-- PATCHES 
-- ============================================================
-- 014a_allow_kelp_taxonomy_id_in_role_views.sql
-- Expose kelp_taxonomy_id in role-facing kelp_taxonomy_dim views
-- so catalog joins can work.
-- ============================================================

BEGIN;

-- Allow the taxonomy surrogate key everywhere (not sensitive, join-critical)
UPDATE admin.view_column_policy
SET allow_public = TRUE,
    allow_nongenresearch = TRUE,
    allow_genresearch = TRUE,
    allow_restoration = TRUE,
    allow_farmbreed = TRUE,
    allow_internal = TRUE,
    notes = COALESCE(notes,'') || ' | patch014a: allow kelp_taxonomy_id for joins'
WHERE base_schema = 'structured'
  AND base_table  = 'kelp_taxonomy_dim'
  AND column_name = 'kelp_taxonomy_id';

-- Rebuild views from policy (this will recreate role-schema kelp_taxonomy_dim with the id)
SELECT admin.refresh_role_views();

COMMIT;

-- ============================================================
-- 014b_reset_kelp_taxonomy_views.sql
-- Drop role-facing kelp_taxonomy_dim views so column order
-- changes (kelp_taxonomy_id) can be applied cleanly.
-- ============================================================

BEGIN;

DROP VIEW IF EXISTS public_structured.kelp_taxonomy_dim;
DROP VIEW IF EXISTS nongenresearch.kelp_taxonomy_dim;
DROP VIEW IF EXISTS genresearch.kelp_taxonomy_dim;
DROP VIEW IF EXISTS restoration.kelp_taxonomy_dim;
DROP VIEW IF EXISTS farmbreed.kelp_taxonomy_dim;

-- Rebuild all role views from policy (now includes kelp_taxonomy_id)
SELECT admin.refresh_role_views();

COMMIT;


