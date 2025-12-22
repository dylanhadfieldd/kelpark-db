-- ============================================================
-- 011_seed_policies_and_build_views.sql
-- Seed table registry + column policy (pattern-based) and build views
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- 0) Register base tables
--    (kelps vs microbes)
-- ------------------------------------------------------------

INSERT INTO admin.view_base_table_registry (base_schema, base_table, dataset_family, is_active)
VALUES
  ('structured','kelp_sample','kelps',TRUE),
  ('structured','kelp_sample_person_role','kelps',TRUE),
  ('structured','kelp_taxonomy_dim','kelps',TRUE),
  ('structured','location_dim','kelps',TRUE),
  ('structured','person_dim','kelps',TRUE),
  ('structured','storage_dim','kelps',TRUE),

  ('structured','microbe_isolate','microbes',TRUE),
  ('structured','microbe_isolate_person_role','microbes',TRUE),
  ('structured','microbe_kelp_link','microbes',TRUE),
  ('structured','microbe_taxonomy_dim','microbes',TRUE)
ON CONFLICT (base_schema, base_table) DO UPDATE
SET dataset_family = EXCLUDED.dataset_family,
    is_active      = EXCLUDED.is_active;

-- Ensure there is at least one policy row per (table, column)
SELECT admin.ensure_default_policies();

-- ------------------------------------------------------------
-- 1) Microbes: internal-only (default already does this)
--    If you ever want to expose *any* microbe columns, you'd flip them explicitly.
-- ------------------------------------------------------------

UPDATE admin.view_column_policy p
SET allow_public         = FALSE,
    allow_nongenresearch = FALSE,
    allow_genresearch    = FALSE,
    allow_restoration    = FALSE,
    allow_farmbreed      = FALSE,
    allow_internal       = TRUE,
    notes = COALESCE(notes,'') || ' | microbes locked to internal'
WHERE (p.base_schema, p.base_table) IN (
  ('structured','microbe_isolate'),
  ('structured','microbe_isolate_person_role'),
  ('structured','microbe_kelp_link'),
  ('structured','microbe_taxonomy_dim')
);

-- ------------------------------------------------------------
-- 2) Kelps: apply your use-case matrix via column-name rules
--    Everything remains internal unless explicitly allowed below.
--
-- IMPORTANT:
--   - Adjust patterns if your real column names differ.
--   - This is meant to get you ~80% there quickly.
-- ------------------------------------------------------------

-- Helper macro mindset:
-- PUBLIC = public_structured
-- NONGEN = nongenresearch
-- GEN    = genresearch
-- RESTO  = restoration
-- FARM   = farmbreed
-- INTERNAL always TRUE already

-- 2A) Taxonomy: Genus / Species / Variety or Form / Sex → ALL roles
UPDATE admin.view_column_policy
SET allow_public = TRUE,
    allow_nongenresearch = TRUE,
    allow_genresearch = TRUE,
    allow_restoration = TRUE,
    allow_farmbreed = TRUE,
    notes = COALESCE(notes,'') || ' | taxonomy: all roles'
WHERE base_schema='structured'
  AND base_table='kelp_taxonomy_dim'
  AND (
    column_name ILIKE '%genus%' OR
    column_name ILIKE '%species%' OR
    column_name ILIKE '%variety%' OR
    column_name ILIKE '%form%' OR
    column_name ILIKE '%sex%'
  );

-- 2B) Sampling metadata: country/lat/lon/collection date/site/permit → ALL roles
-- Internal-only: collected_by, deposited_by, personnel
UPDATE admin.view_column_policy
SET allow_public = TRUE,
    allow_nongenresearch = TRUE,
    allow_genresearch = TRUE,
    allow_restoration = TRUE,
    allow_farmbreed = TRUE,
    notes = COALESCE(notes,'') || ' | sampling: all roles'
WHERE base_schema='structured'
  AND base_table='kelp_sample'
  AND (
    column_name ILIKE '%country%' OR
    column_name ILIKE '%latitude%' OR
    column_name ILIKE '%lat%' OR
    column_name ILIKE '%longitude%' OR
    column_name ILIKE '%lon%' OR
    column_name ILIKE '%collection_date%' OR
    column_name ILIKE '%date_of_collection%' OR
    column_name ILIKE '%permit%' OR
    column_name ILIKE '%collection_site%' OR
    column_name ILIKE '%site%'
  );

-- Sampling metadata – internal-only people fields
UPDATE admin.view_column_policy
SET allow_public = FALSE,
    allow_nongenresearch = FALSE,
    allow_genresearch = FALSE,
    allow_restoration = FALSE,
    allow_farmbreed = FALSE,
    allow_internal = TRUE,
    notes = COALESCE(notes,'') || ' | sampling people: internal only'
WHERE base_schema='structured'
  AND base_table='kelp_sample'
  AND (
    column_name ILIKE '%personnel%' OR
    column_name ILIKE '%collected_by%' OR
    column_name ILIKE '%collector%' OR
    column_name ILIKE '%deposited_by%' OR
    column_name ILIKE '%deposit%by%'
  );

-- 2C) Storage details:
-- ID is allowed for ALL roles; medium/temperature/location/rack/position internal only
UPDATE admin.view_column_policy
SET allow_public = TRUE,
    allow_nongenresearch = TRUE,
    allow_genresearch = TRUE,
    allow_restoration = TRUE,
    allow_farmbreed = TRUE,
    notes = COALESCE(notes,'') || ' | storage id: all roles'
WHERE base_schema='structured'
  AND base_table='storage_dim'
  AND (
    column_name ILIKE '%storage%id%' OR
    column_name = 'storage_id'
  );

UPDATE admin.view_column_policy
SET allow_public = FALSE,
    allow_nongenresearch = FALSE,
    allow_genresearch = FALSE,
    allow_restoration = FALSE,
    allow_farmbreed = FALSE,
    allow_internal = TRUE,
    notes = COALESCE(notes,'') || ' | storage details: internal only'
WHERE base_schema='structured'
  AND base_table='storage_dim'
  AND (
    column_name ILIKE '%medium%' OR
    column_name ILIKE '%temperature%' OR
    column_name ILIKE '%location%' OR
    column_name ILIKE '%rack%' OR
    column_name ILIKE '%position%'
  );

-- 2D) Sponsorship:
-- status: internal + public
-- code: internal only
UPDATE admin.view_column_policy
SET allow_public = TRUE,
    allow_nongenresearch = FALSE,
    allow_genresearch = FALSE,
    allow_restoration = FALSE,
    allow_farmbreed = FALSE,
    notes = COALESCE(notes,'') || ' | sponsorship status: public+internal'
WHERE base_schema='structured'
  AND base_table='kelp_sample'
  AND column_name ILIKE '%sponsor%status%';

UPDATE admin.view_column_policy
SET allow_public = FALSE,
    allow_nongenresearch = FALSE,
    allow_genresearch = FALSE,
    allow_restoration = FALSE,
    allow_farmbreed = FALSE,
    allow_internal = TRUE,
    notes = COALESCE(notes,'') || ' | sponsorship code: internal only'
WHERE base_schema='structured'
  AND base_table='kelp_sample'
  AND column_name ILIKE '%sponsor%code%';

-- 2E) Phenotypic data:
-- Public + Non-gen + Gen + Farmbreed (NOT Restoration by default)
UPDATE admin.view_column_policy
SET allow_public = TRUE,
    allow_nongenresearch = TRUE,
    allow_genresearch = TRUE,
    allow_restoration = FALSE,
    allow_farmbreed = TRUE,
    notes = COALESCE(notes,'') || ' | phenotypic: public+nongen+gen+farm'
WHERE base_schema='structured'
  AND base_table='kelp_sample'
  AND (
    column_name ILIKE '%growth_rate%' OR
    column_name ILIKE '%optimal_growth%' OR
    column_name ILIKE '%optimal%condition%' OR
    column_name ILIKE '%viability%' OR
    column_name ILIKE '%lifespan%' OR
    column_name ILIKE '%thermal%stressor%' OR
    column_name ILIKE '%water%quality%stressor%' OR
    column_name ILIKE '%tolerance%'
  );

-- 2F) Ecological role:
-- Non-gen + Restoration (optionally public if you decide later)
UPDATE admin.view_column_policy
SET allow_public = FALSE,
    allow_nongenresearch = TRUE,
    allow_genresearch = FALSE,
    allow_restoration = TRUE,
    allow_farmbreed = FALSE,
    notes = COALESCE(notes,'') || ' | ecological role: nongen+restoration'
WHERE base_schema='structured'
  AND base_table='kelp_sample'
  AND (
    column_name ILIKE '%trophic%' OR
    column_name ILIKE '%primary_producer%' OR
    column_name ILIKE '%carbon_sink%' OR
    column_name ILIKE '%habitat_former%'
  );

-- 2G) iNaturalist link:
-- Public + Restoration
UPDATE admin.view_column_policy
SET allow_public = TRUE,
    allow_nongenresearch = FALSE,
    allow_genresearch = FALSE,
    allow_restoration = TRUE,
    allow_farmbreed = FALSE,
    notes = COALESCE(notes,'') || ' | iNaturalist: public+restoration'
WHERE base_schema='structured'
  AND base_table='kelp_sample'
  AND column_name ILIKE '%inat%';

-- ------------------------------------------------------------
-- 3) Build/refresh the views based on the policy
-- ------------------------------------------------------------
SELECT admin.refresh_role_views();

-- ------------------------------------------------------------
-- 4) Grant SELECT on views to roles (in case new views were created)
-- ------------------------------------------------------------
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
