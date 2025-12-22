-- 016_microbe_catalog_views.sql
-- MVP: internal-only Microbe catalog views
-- Design: 1 row per microbe_isolate_id (Option A)
-- Notes:
--   - microbe_kelp_link.kelp_sample_id is currently NULL for all rows
--   - so "plus" aggregates link data using kelp_ka_sample_id (text) and link table columns,
--     and does NOT attempt a join to structured.kelp_sample.

BEGIN;

-- -------------------------------------------------------------------
-- 0) Preflight: optional visibility checks (safe to run)
-- -------------------------------------------------------------------
-- SELECT COUNT(*) FROM structured.microbe_isolate;
-- SELECT COUNT(*) FROM structured.microbe_taxonomy_dim;
-- SELECT COUNT(*) FROM structured.microbe_kelp_link;

-- -------------------------------------------------------------------
-- 1) Drop old views if they exist (prevents column mismatch issues)
-- -------------------------------------------------------------------
DROP VIEW IF EXISTS structured.microbe_catalog_plus CASCADE;
DROP VIEW IF EXISTS structured.microbe_catalog CASCADE;

-- -------------------------------------------------------------------
-- 2) Base microbe catalog: 1 row per isolate
-- -------------------------------------------------------------------
CREATE VIEW structured.microbe_catalog AS
SELECT
  mi.microbe_isolate_id,
  mi.staging_id,
  mi.ingest_batch_id,
  mi.source_filename,
  mi.source_row_num,
  mi.loaded_at,

  -- identity
  mi.microbe_id,
  mi.original_code,

  -- isolation & stewardship (raw strings kept from source)
  mi.institution_isolation_physically_conducted,
  mi.isolated_year,
  mi.isolated_year_raw,
  mi.isolated_by,
  mi.maintained_by,
  mi.maintained_at,

  -- kelp context captured on isolate record
  mi.kelp_host,
  mi.kelp_location,
  mi.kelp_collection_temp_c,
  mi.kelp_collection_temp_raw,
  mi.kelp_collection_month,
  mi.kelp_collection_season,
  mi.kelp_thallus_collection,
  mi.kelp_collection_approach,
  mi.kelp_collection_method,

  -- methods & protocols
  mi.microbe_isolation_methods,
  mi.microbe_isolation_protocol,
  mi.isolation_media,

  -- storage locations
  mi.location_stored1,
  mi.location_1_temperature_c,
  mi.location_1_temperature_raw,
  mi.location_stored2,
  mi.location_2_temperature_c,
  mi.location_2_temperature_raw,

  -- cryo
  mi.cryopreservation_date,
  mi.cryopreservation_date_raw,
  mi.cryo_storage_medium,
  mi.cryo_storage_preservative,
  mi.cryo_revival_tested,
  mi.cryo_backups_created,
  mi.cryopreservation_protocol,

  -- MALDI / sequencing
  mi.malditof_procedure,
  mi.malditof_dataanalysis_complete,
  mi.high_quality_malditof_data,
  mi.s16_pcr_completed,
  mi.pcr_conducted_by,
  mi.sanger_sequencing_completed,
  mi.sequencing_date,
  mi.sequencing_date_raw,
  mi.primers_used,
  mi.sequencing_notes,
  mi.sequencing_conducted_by,

  -- sequence / ids
  mi.total_bp_length_after_trimming,
  mi.closest_ncbi_blast_tax_id,
  mi.ncbi_blast_query_cover,
  mi.percent_identity,
  mi.percent_identity_raw,
  mi.accession,

  -- taxonomy from isolate record
  mi.taxonomy_kingdom,

  -- sequences
  mi.s16_sequence,
  mi.its2_sequence,

  -- activity flags
  mi.pathogen_activity_kelp,
  mi.pathogen_activity_humans,
  mi.pathogen_activity_plants,
  mi.pathogen_activity_animals,

  -- growth ranges
  mi.growth_temperature_c_range,
  mi.growth_salinity_range,
  mi.growth_ph_range,
  mi.growth_optimal_media,

  -- morphology
  mi.morphology_colony_color,
  mi.morphology_colony_size,
  mi.morphology_colony_shape,
  mi.morphology_colony_texture,
  mi.gram_stain,
  mi.morphology_cell_shape,

  -- probiotic
  mi.probiotic_activity,
  mi.probiotic_known_host,

  -- join key to taxonomy_dim
  mi.microbe_taxonomy_id,

  -- taxonomy_dim fields (currently minimal, but future-proof)
  mtd.taxonomy_kingdom AS dim_taxonomy_kingdom,
  mtd.dedupe_key       AS taxonomy_dedupe_key

FROM structured.microbe_isolate mi
LEFT JOIN structured.microbe_taxonomy_dim mtd
  ON mtd.microbe_taxonomy_id = mi.microbe_taxonomy_id;

-- -------------------------------------------------------------------
-- 3) "Plus" microbe catalog: adds link + people aggregates
-- -------------------------------------------------------------------
CREATE VIEW structured.microbe_catalog_plus AS
WITH link_agg AS (
  SELECT
    l.microbe_isolate_id,
    COUNT(*) AS kelp_link_count,

    -- KA sample ids are the only populated sample identifier right now
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(btrim(l.kelp_ka_sample_id), '')), NULL) AS kelp_ka_sample_ids,

    -- optional contextual fields captured on the link rows
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(btrim(l.kelp_host), '')), NULL) AS linked_kelp_hosts,
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(btrim(l.kelp_location), '')), NULL) AS linked_kelp_locations
  FROM structured.microbe_kelp_link l
  GROUP BY 1
),
people_agg AS (
  SELECT
    mpr.microbe_isolate_id,

    ARRAY_REMOVE(ARRAY_AGG(DISTINCT pd.person_name_raw)
      FILTER (WHERE mpr.role='isolated_by'), NULL) AS isolated_by_people,

    ARRAY_REMOVE(ARRAY_AGG(DISTINCT pd.person_name_raw)
      FILTER (WHERE mpr.role='maintained_by'), NULL) AS maintained_by_people,

    ARRAY_REMOVE(ARRAY_AGG(DISTINCT pd.person_name_raw)
      FILTER (WHERE mpr.role='pcr_conducted_by'), NULL) AS pcr_conducted_by_people,

    ARRAY_REMOVE(ARRAY_AGG(DISTINCT pd.person_name_raw)
      FILTER (WHERE mpr.role='sequencing_conducted_by'), NULL) AS sequencing_conducted_by_people

  FROM structured.microbe_isolate_person_role mpr
  JOIN structured.person_dim pd
    ON pd.person_id = mpr.person_id
  GROUP BY 1
)
SELECT
  mc.*,

  COALESCE(la.kelp_link_count, 0) AS kelp_link_count,
  la.kelp_ka_sample_ids,
  la.linked_kelp_hosts,
  la.linked_kelp_locations,

  pa.isolated_by_people,
  pa.maintained_by_people,
  pa.pcr_conducted_by_people,
  pa.sequencing_conducted_by_people

FROM structured.microbe_catalog mc
LEFT JOIN link_agg la
  ON la.microbe_isolate_id = mc.microbe_isolate_id
LEFT JOIN people_agg pa
  ON pa.microbe_isolate_id = mc.microbe_isolate_id;

-- -------------------------------------------------------------------
-- 4) Grants (internal-only)
-- -------------------------------------------------------------------
GRANT USAGE ON SCHEMA structured TO internal_user, kara_app;
GRANT SELECT ON structured.microbe_catalog      TO internal_user, kara_app;
GRANT SELECT ON structured.microbe_catalog_plus TO internal_user, kara_app;

COMMIT;

-- -------------------------------------------------------------------
-- 5) Quick validation queries (run manually)
-- -------------------------------------------------------------------
-- Row counts: should both be 131
-- SELECT COUNT(*) AS n_catalog FROM structured.microbe_catalog;
-- SELECT COUNT(*) AS n_catalog_plus FROM structured.microbe_catalog_plus;

-- Link sanity: should show some kelp_ka_sample_ids for a subset (40 links total w/ ka ids)
-- SELECT microbe_isolate_id, kelp_link_count, kelp_ka_sample_ids
-- FROM structured.microbe_catalog_plus
-- WHERE kelp_link_count > 0
-- ORDER BY kelp_link_count DESC, microbe_isolate_id
-- LIMIT 25;

-- People sanity: verify arrays populate
-- SELECT microbe_isolate_id, isolated_by_people, maintained_by_people, pcr_conducted_by_people, sequencing_conducted_by_people
-- FROM structured.microbe_catalog_plus
-- WHERE isolated_by_people IS NOT NULL OR maintained_by_people IS NOT NULL
-- LIMIT 25;
