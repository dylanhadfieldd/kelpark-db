-- ============================================================
-- 016_all_kelp_catalog_plus_views.sql
-- Create role-based kelp_catalog_plus views (matrix-aligned)
--
-- Adds:
--  - storage_details_id_dim (ID only; no temp/medium/rack/location)
--  - location_country_dim, location_collection_site_dim,
--    location_latitude_dd_dim, location_longitude_dd_dim
--
-- Notes:
--  - Uses kc.* so each role keeps its allowed kelp fields naturally.
--  - Avoids coord_status/coord_format and storage internals (matrix says internal-only).
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- Public
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW public_structured.kelp_catalog_plus AS
SELECT
  kc.*,
  sd.storage_details_id AS storage_details_id_dim,
  ld.country           AS location_country_dim,
  ld.collection_site   AS location_collection_site_dim,
  ld.latitude_dd       AS location_latitude_dd_dim,
  ld.longitude_dd      AS location_longitude_dd_dim
FROM public_structured.kelp_catalog kc
LEFT JOIN public_structured.storage_dim  sd ON sd.storage_id  = kc.storage_id
LEFT JOIN public_structured.location_dim ld ON ld.location_id = kc.location_id;

-- ------------------------------------------------------------
-- Non-genetic research
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW nongenresearch.kelp_catalog_plus AS
SELECT
  kc.*,
  sd.storage_details_id AS storage_details_id_dim,
  ld.country           AS location_country_dim,
  ld.collection_site   AS location_collection_site_dim,
  ld.latitude_dd       AS location_latitude_dd_dim,
  ld.longitude_dd      AS location_longitude_dd_dim
FROM nongenresearch.kelp_catalog kc
LEFT JOIN nongenresearch.storage_dim  sd ON sd.storage_id  = kc.storage_id
LEFT JOIN nongenresearch.location_dim ld ON ld.location_id = kc.location_id;

-- ------------------------------------------------------------
-- Genetic research
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW genresearch.kelp_catalog_plus AS
SELECT
  kc.*,
  sd.storage_details_id AS storage_details_id_dim,
  ld.country           AS location_country_dim,
  ld.collection_site   AS location_collection_site_dim,
  ld.latitude_dd       AS location_latitude_dd_dim,
  ld.longitude_dd      AS location_longitude_dd_dim
FROM genresearch.kelp_catalog kc
LEFT JOIN genresearch.storage_dim  sd ON sd.storage_id  = kc.storage_id
LEFT JOIN genresearch.location_dim ld ON ld.location_id = kc.location_id;

-- ------------------------------------------------------------
-- Restoration
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW restoration.kelp_catalog_plus AS
SELECT
  kc.*,
  sd.storage_details_id AS storage_details_id_dim,
  ld.country           AS location_country_dim,
  ld.collection_site   AS location_collection_site_dim,
  ld.latitude_dd       AS location_latitude_dd_dim,
  ld.longitude_dd      AS location_longitude_dd_dim
FROM restoration.kelp_catalog kc
LEFT JOIN restoration.storage_dim  sd ON sd.storage_id  = kc.storage_id
LEFT JOIN restoration.location_dim ld ON ld.location_id = kc.location_id;

-- ------------------------------------------------------------
-- Farming & breeding
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW farmbreed.kelp_catalog_plus AS
SELECT
  kc.*,
  sd.storage_details_id AS storage_details_id_dim,
  ld.country           AS location_country_dim,
  ld.collection_site   AS location_collection_site_dim,
  ld.latitude_dd       AS location_latitude_dd_dim,
  ld.longitude_dd      AS location_longitude_dd_dim
FROM farmbreed.kelp_catalog kc
LEFT JOIN farmbreed.storage_dim  sd ON sd.storage_id  = kc.storage_id
LEFT JOIN farmbreed.location_dim ld ON ld.location_id = kc.location_id;

-- ------------------------------------------------------------
-- Grants
-- ------------------------------------------------------------
GRANT SELECT ON public_structured.kelp_catalog_plus TO public_user, internal_user, kara_app;
GRANT SELECT ON nongenresearch.kelp_catalog_plus    TO nongenresearch_user, internal_user, kara_app;
GRANT SELECT ON genresearch.kelp_catalog_plus       TO genresearch_user, internal_user, kara_app;
GRANT SELECT ON restoration.kelp_catalog_plus       TO restoration_user, internal_user, kara_app;
GRANT SELECT ON farmbreed.kelp_catalog_plus         TO farmbreed_user, internal_user, kara_app;

COMMIT;

-- ------------------------------------------------------------
-- Optional smoke tests
-- ------------------------------------------------------------
-- SELECT 1 FROM public_structured.kelp_catalog_plus LIMIT 0;
-- SELECT 1 FROM nongenresearch.kelp_catalog_plus LIMIT 0;
-- SELECT 1 FROM genresearch.kelp_catalog_plus LIMIT 0;
-- SELECT 1 FROM restoration.kelp_catalog_plus LIMIT 0;
-- SELECT 1 FROM farmbreed.kelp_catalog_plus LIMIT 0;
