-- ============================================================
-- 014_create_kelp_catalog_views.sql
-- Role-based denormalized catalog views for Kelps
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- Public catalog
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW public_structured.kelp_catalog AS
SELECT
  ks.*,
  kt.genus            AS taxonomy_genus_dim,
  kt.species          AS taxonomy_species_dim,
  kt.sex              AS taxonomy_sex_dim,
  kt.variety_or_form  AS taxonomy_variety_or_form_dim
FROM public_structured.kelp_sample ks
LEFT JOIN public_structured.kelp_taxonomy_dim kt
  ON kt.kelp_taxonomy_id = ks.kelp_taxonomy_id;

-- ------------------------------------------------------------
-- Non-genetic research catalog
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW nongenresearch.kelp_catalog AS
SELECT
  ks.*,
  kt.genus            AS taxonomy_genus_dim,
  kt.species          AS taxonomy_species_dim,
  kt.sex              AS taxonomy_sex_dim,
  kt.variety_or_form  AS taxonomy_variety_or_form_dim
FROM nongenresearch.kelp_sample ks
LEFT JOIN nongenresearch.kelp_taxonomy_dim kt
  ON kt.kelp_taxonomy_id = ks.kelp_taxonomy_id;

-- ------------------------------------------------------------
-- Genetic research catalog
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW genresearch.kelp_catalog AS
SELECT
  ks.*,
  kt.genus            AS taxonomy_genus_dim,
  kt.species          AS taxonomy_species_dim,
  kt.sex              AS taxonomy_sex_dim,
  kt.variety_or_form  AS taxonomy_variety_or_form_dim
FROM genresearch.kelp_sample ks
LEFT JOIN genresearch.kelp_taxonomy_dim kt
  ON kt.kelp_taxonomy_id = ks.kelp_taxonomy_id;

-- ------------------------------------------------------------
-- Restoration catalog
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW restoration.kelp_catalog AS
SELECT
  ks.*,
  kt.genus            AS taxonomy_genus_dim,
  kt.species          AS taxonomy_species_dim,
  kt.sex              AS taxonomy_sex_dim,
  kt.variety_or_form  AS taxonomy_variety_or_form_dim
FROM restoration.kelp_sample ks
LEFT JOIN restoration.kelp_taxonomy_dim kt
  ON kt.kelp_taxonomy_id = ks.kelp_taxonomy_id;

-- ------------------------------------------------------------
-- Farming & breeding catalog
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW farmbreed.kelp_catalog AS
SELECT
  ks.*,
  kt.genus            AS taxonomy_genus_dim,
  kt.species          AS taxonomy_species_dim,
  kt.sex              AS taxonomy_sex_dim,
  kt.variety_or_form  AS taxonomy_variety_or_form_dim
FROM farmbreed.kelp_sample ks
LEFT JOIN farmbreed.kelp_taxonomy_dim kt
  ON kt.kelp_taxonomy_id = ks.kelp_taxonomy_id;

-- ------------------------------------------------------------
-- Grants
-- ------------------------------------------------------------
GRANT SELECT ON public_structured.kelp_catalog TO public_user, internal_user, kara_app;
GRANT SELECT ON nongenresearch.kelp_catalog    TO nongenresearch_user, internal_user, kara_app;
GRANT SELECT ON genresearch.kelp_catalog       TO genresearch_user, internal_user, kara_app;
GRANT SELECT ON restoration.kelp_catalog       TO restoration_user, internal_user, kara_app;
GRANT SELECT ON farmbreed.kelp_catalog         TO farmbreed_user, internal_user, kara_app;

COMMIT;

-- ------------------------------------------------------------
-- Optional compile smoke tests (uncomment if desired)
-- ------------------------------------------------------------
-- SELECT 1 FROM public_structured.kelp_catalog LIMIT 0;
-- SELECT 1 FROM nongenresearch.kelp_catalog LIMIT 0;
-- SELECT 1 FROM genresearch.kelp_catalog LIMIT 0;
-- SELECT 1 FROM restoration.kelp_catalog LIMIT 0;
-- SELECT 1 FROM farmbreed.kelp_catalog LIMIT 0;
