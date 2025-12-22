BEGIN;

-- ============================================================
-- 005_structured_core.sql
-- Core structured tables (MVP):
--   structured.kelp_sample
--   structured.microbe_isolate
--   structured.microbe_kelp_link  (link + unresolved refs)
--
-- Source-of-truth for this step: staging.kelps_typed / staging.microbes_typed
-- ============================================================

CREATE SCHEMA IF NOT EXISTS structured;

-- --------------------------------------------
-- 1) Drop / recreate (repeatable rebuild)
-- --------------------------------------------

DROP TABLE IF EXISTS structured.microbe_kelp_link CASCADE;
DROP TABLE IF EXISTS structured.microbe_isolate CASCADE;
DROP TABLE IF EXISTS structured.kelp_sample CASCADE;

-- --------------------------------------------
-- 2) Core entity tables
-- --------------------------------------------

CREATE TABLE structured.kelp_sample (
  kelp_sample_id UUID PRIMARY KEY,
  staging_id BIGINT NOT NULL UNIQUE,

  ingest_batch_id UUID NULL,
  source_filename TEXT NULL,
  source_row_num INTEGER NULL,
  loaded_at TIMESTAMPTZ NOT NULL,

  taxonomy_genus TEXT NULL,
  taxonomy_species TEXT NULL,
  taxonomy_sex TEXT NULL,
  taxonomy_variety_or_form TEXT NULL,

  storage_details_id TEXT NULL,
  storage_details_position_id TEXT NULL,
  storage_details_rack_id TEXT NULL,
  storage_details_location TEXT NULL,
  storage_details_temperature_c NUMERIC NULL,
  storage_details_temperature_c_raw TEXT NULL,
  storage_details_medium TEXT NULL,

  sampling_metadata_country TEXT NULL,
  sampling_metadata_latitude_dd NUMERIC NULL,
  sampling_metadata_longitude_dd NUMERIC NULL,
  sampling_metadata_latitude_raw TEXT NULL,
  sampling_metadata_longitude_raw TEXT NULL,

  sampling_metadata_collection_date DATE NULL,
  sampling_metadata_collection_date_raw TEXT NULL,
  sampling_metadata_isolation_date DATE NULL,
  sampling_metadata_isolation_date_raw TEXT NULL,
  sampling_metadata_deposit_date DATE NULL,
  sampling_metadata_deposit_date_raw TEXT NULL,

  sampling_metadata_personnel_collected TEXT NULL,
  sampling_metadata_deposited_by TEXT NULL,
  sampling_metadata_permit TEXT NULL,
  sampling_metadata_collection_site TEXT NULL,

  other_previously_housed_location TEXT NULL,
  sponsorship_strain_sponsorship_status TEXT NULL,
  sponsorship_code TEXT NULL,

  phenotypic_data_growth_rate NUMERIC NULL,
  phenotypic_data_growth_rate_raw TEXT NULL,
  phenotypic_data_optimal_growth_conditions TEXT NULL,
  phenotypic_data_percent_viability NUMERIC NULL,
  phenotypic_data_percent_viability_raw TEXT NULL,
  phenotypic_data_lifespan TEXT NULL,
  phenotypic_data_tolerance_to_thermal_stressor TEXT NULL,
  phenotypic_data_tolerance_to_water_quality_stressors TEXT NULL,

  inaturalist TEXT NULL
);

CREATE TABLE structured.microbe_isolate (
  microbe_isolate_id UUID PRIMARY KEY,
  staging_id BIGINT NOT NULL UNIQUE,

  ingest_batch_id UUID NULL,
  source_filename TEXT NULL,
  source_row_num INTEGER NULL,
  loaded_at TIMESTAMPTZ NOT NULL,

  microbe_id TEXT NULL UNIQUE,
  original_code TEXT NULL,

  institution_isolation_physically_conducted TEXT NULL,

  isolated_year INTEGER NULL,
  isolated_year_raw TEXT NULL,
  isolated_by TEXT NULL,
  maintained_by TEXT NULL,
  maintained_at TEXT NULL,

  kelp_host TEXT NULL,
  kelp_location TEXT NULL,

  kelp_collection_temp_c NUMERIC NULL,
  kelp_collection_temp_raw TEXT NULL,
  kelp_collection_month TEXT NULL,
  kelp_collection_season TEXT NULL,
  kelp_thallus_collection TEXT NULL,
  kelp_collection_approach TEXT NULL,
  kelp_collection_method TEXT NULL,

  microbe_isolation_methods TEXT NULL,
  microbe_isolation_protocol TEXT NULL,
  isolation_media TEXT NULL,

  location_stored1 TEXT NULL,
  location_1_temperature_c NUMERIC NULL,
  location_1_temperature_raw TEXT NULL,

  location_stored2 TEXT NULL,
  location_2_temperature_c NUMERIC NULL,
  location_2_temperature_raw TEXT NULL,

  cryopreservation_date DATE NULL,
  cryopreservation_date_raw TEXT NULL,
  cryo_storage_medium TEXT NULL,
  cryo_storage_preservative TEXT NULL,
  cryo_revival_tested TEXT NULL,
  cryo_backups_created TEXT NULL,
  cryopreservation_protocol TEXT NULL,

  malditof_procedure TEXT NULL,
  malditof_dataanalysis_complete TEXT NULL,
  high_quality_malditof_data TEXT NULL,

  s16_pcr_completed TEXT NULL,
  pcr_conducted_by TEXT NULL,
  sanger_sequencing_completed TEXT NULL,

  sequencing_date DATE NULL,
  sequencing_date_raw TEXT NULL,
  primers_used TEXT NULL,
  sequencing_notes TEXT NULL,
  sequencing_conducted_by TEXT NULL,

  total_bp_length_after_trimming TEXT NULL,
  closest_ncbi_blast_tax_id TEXT NULL,
  ncbi_blast_query_cover TEXT NULL,

  percent_identity NUMERIC NULL,
  percent_identity_raw TEXT NULL,

  accession TEXT NULL,
  taxonomy_kingdom TEXT NULL,
  s16_sequence TEXT NULL,
  its2_sequence TEXT NULL,

  pathogen_activity_kelp TEXT NULL,
  pathogen_activity_humans TEXT NULL,
  pathogen_activity_plants TEXT NULL,
  pathogen_activity_animals TEXT NULL,

  growth_temperature_c_range TEXT NULL,
  growth_salinity_range TEXT NULL,
  growth_ph_range TEXT NULL,
  growth_optimal_media TEXT NULL,

  morphology_colony_color TEXT NULL,
  morphology_colony_size TEXT NULL,
  morphology_colony_shape TEXT NULL,
  morphology_colony_texture TEXT NULL,
  gram_stain TEXT NULL,
  morphology_cell_shape TEXT NULL,

  probiotic_activity TEXT NULL,
  probiotic_known_host TEXT NULL
);

CREATE TABLE structured.microbe_kelp_link (
  microbe_kelp_link_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  microbe_isolate_id UUID NOT NULL
    REFERENCES structured.microbe_isolate(microbe_isolate_id) ON DELETE CASCADE,

  kelp_sample_id UUID NULL
    REFERENCES structured.kelp_sample(kelp_sample_id) ON DELETE SET NULL,

  kelp_ka_sample_id TEXT NULL,
  source_if_ka_id TEXT NULL,
  source_if_no_ka_id TEXT NULL,

  kelp_host TEXT NULL,
  kelp_location TEXT NULL,

  microbe_staging_id BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_microbe_kelp_link_microbe
  ON structured.microbe_kelp_link(microbe_isolate_id);

CREATE INDEX IF NOT EXISTS idx_microbe_kelp_link_kelp
  ON structured.microbe_kelp_link(kelp_sample_id);

CREATE INDEX IF NOT EXISTS idx_microbe_kelp_link_ka_sample_id
  ON structured.microbe_kelp_link(kelp_ka_sample_id);

-- --------------------------------------------
-- 3) Load from typed
-- --------------------------------------------

INSERT INTO structured.kelp_sample (
  kelp_sample_id, staging_id,
  ingest_batch_id, source_filename, source_row_num, loaded_at,
  taxonomy_genus, taxonomy_species, taxonomy_sex, taxonomy_variety_or_form,
  storage_details_id, storage_details_position_id, storage_details_rack_id,
  storage_details_location, storage_details_temperature_c, storage_details_temperature_c_raw,
  storage_details_medium,
  sampling_metadata_country,
  sampling_metadata_latitude_dd, sampling_metadata_longitude_dd,
  sampling_metadata_latitude_raw, sampling_metadata_longitude_raw,
  sampling_metadata_collection_date, sampling_metadata_collection_date_raw,
  sampling_metadata_isolation_date, sampling_metadata_isolation_date_raw,
  sampling_metadata_deposit_date, sampling_metadata_deposit_date_raw,
  sampling_metadata_personnel_collected,
  sampling_metadata_deposited_by, sampling_metadata_permit, sampling_metadata_collection_site,
  other_previously_housed_location,
  sponsorship_strain_sponsorship_status, sponsorship_code,
  phenotypic_data_growth_rate, phenotypic_data_growth_rate_raw,
  phenotypic_data_optimal_growth_conditions,
  phenotypic_data_percent_viability, phenotypic_data_percent_viability_raw,
  phenotypic_data_lifespan,
  phenotypic_data_tolerance_to_thermal_stressor,
  phenotypic_data_tolerance_to_water_quality_stressors,
  inaturalist
)
SELECT
  kelps_typed_id,
  staging_id,
  ingest_batch_id, source_filename, source_row_num, loaded_at,
  taxonomy_genus, taxonomy_species, taxonomy_sex, taxonomy_variety_or_form,
  storage_details_id, storage_details_position_id, storage_details_rack_id,
  storage_details_location, storage_details_temperature_c, storage_details_temperature_c_raw,
  storage_details_medium,
  sampling_metadata_country,
  sampling_metadata_latitude_dd, sampling_metadata_longitude_dd,
  sampling_metadata_latitude_raw, sampling_metadata_longitude_raw,
  sampling_metadata_collection_date, sampling_metadata_collection_date_raw,
  sampling_metadata_isolation_date, sampling_metadata_isolation_date_raw,
  sampling_metadata_deposit_date, sampling_metadata_deposit_date_raw,
  sampling_metadata_personnel_collected,
  sampling_metadata_deposited_by, sampling_metadata_permit, sampling_metadata_collection_site,
  other_previously_housed_location,
  sponsorship_strain_sponsorship_status, sponsorship_code,
  phenotypic_data_growth_rate, phenotypic_data_growth_rate_raw,
  phenotypic_data_optimal_growth_conditions,
  phenotypic_data_percent_viability, phenotypic_data_percent_viability_raw,
  phenotypic_data_lifespan,
  phenotypic_data_tolerance_to_thermal_stressor,
  phenotypic_data_tolerance_to_water_quality_stressors,
  inaturalist
FROM staging.kelps_typed;

INSERT INTO structured.microbe_isolate (
  microbe_isolate_id, staging_id,
  ingest_batch_id, source_filename, source_row_num, loaded_at,
  microbe_id, original_code,
  institution_isolation_physically_conducted,
  isolated_year, isolated_year_raw,
  isolated_by, maintained_by, maintained_at,
  kelp_host, kelp_location,
  kelp_collection_temp_c, kelp_collection_temp_raw,
  kelp_collection_month, kelp_collection_season, kelp_thallus_collection,
  kelp_collection_approach, kelp_collection_method,
  microbe_isolation_methods, microbe_isolation_protocol, isolation_media,
  location_stored1, location_1_temperature_c, location_1_temperature_raw,
  location_stored2, location_2_temperature_c, location_2_temperature_raw,
  cryopreservation_date, cryopreservation_date_raw,
  cryo_storage_medium, cryo_storage_preservative, cryo_revival_tested, cryo_backups_created,
  cryopreservation_protocol,
  malditof_procedure, malditof_dataanalysis_complete, high_quality_malditof_data,
  s16_pcr_completed, pcr_conducted_by, sanger_sequencing_completed,
  sequencing_date, sequencing_date_raw,
  primers_used, sequencing_notes, sequencing_conducted_by,
  total_bp_length_after_trimming, closest_ncbi_blast_tax_id, ncbi_blast_query_cover,
  percent_identity, percent_identity_raw,
  accession, taxonomy_kingdom, s16_sequence, its2_sequence,
  pathogen_activity_kelp, pathogen_activity_humans, pathogen_activity_plants, pathogen_activity_animals,
  growth_temperature_c_range, growth_salinity_range, growth_ph_range, growth_optimal_media,
  morphology_colony_color, morphology_colony_size, morphology_colony_shape, morphology_colony_texture,
  gram_stain, morphology_cell_shape,
  probiotic_activity, probiotic_known_host
)
SELECT
  microbes_typed_id,
  staging_id,
  ingest_batch_id, source_filename, source_row_num, loaded_at,
  microbe_id, original_code,
  institution_isolation_physically_conducted,
  isolated_year, isolated_year_raw,
  isolated_by, maintained_by, maintained_at,
  kelp_host, kelp_location,
  kelp_collection_temp_c, kelp_collection_temp_raw,
  kelp_collection_month, kelp_collection_season, kelp_thallus_collection,
  kelp_collection_approach, kelp_collection_method,
  microbe_isolation_methods, microbe_isolation_protocol, isolation_media,
  location_stored1, location_1_temperature_c, location_1_temperature_raw,
  location_stored2, location_2_temperature_c, location_2_temperature_raw,
  cryopreservation_date, cryopreservation_date_raw,
  cryo_storage_medium, cryo_storage_preservative, cryo_revival_tested, cryo_backups_created,
  cryopreservation_protocol,
  malditof_procedure, malditof_dataanalysis_complete, high_quality_malditof_data,
  s16_pcr_completed, pcr_conducted_by, sanger_sequencing_completed,
  sequencing_date, sequencing_date_raw,
  primers_used, sequencing_notes, sequencing_conducted_by,
  total_bp_length_after_trimming, closest_ncbi_blast_tax_id, ncbi_blast_query_cover,
  percent_identity, percent_identity_raw,
  accession, taxonomy_kingdom, s16_sequence, its2_sequence,
  pathogen_activity_kelp, pathogen_activity_humans, pathogen_activity_plants, pathogen_activity_animals,
  growth_temperature_c_range, growth_salinity_range, growth_ph_range, growth_optimal_media,
  morphology_colony_color, morphology_colony_size, morphology_colony_shape, morphology_colony_texture,
  gram_stain, morphology_cell_shape,
  probiotic_activity, probiotic_known_host
FROM staging.microbes_typed;

-- Correct link insert (no empty-string links)
INSERT INTO structured.microbe_kelp_link (
  microbe_isolate_id,
  kelp_sample_id,
  kelp_ka_sample_id,
  source_if_ka_id,
  source_if_no_ka_id,
  kelp_host,
  kelp_location,
  microbe_staging_id
)
SELECT
  mi.microbe_isolate_id,
  NULL::uuid AS kelp_sample_id,
  NULLIF(btrim(m.kelp_ka_sample_id), '') AS kelp_ka_sample_id,
  NULLIF(btrim(m.source_if_ka_id), '') AS source_if_ka_id,
  NULLIF(btrim(m.source_if_no_ka_id), '') AS source_if_no_ka_id,
  NULLIF(btrim(m.kelp_host), '') AS kelp_host,
  NULLIF(btrim(m.kelp_location), '') AS kelp_location,
  m.staging_id
FROM staging.microbes_typed m
JOIN structured.microbe_isolate mi
  ON mi.staging_id = m.staging_id
WHERE
  COALESCE(
    NULLIF(btrim(m.kelp_ka_sample_id), ''),
    NULLIF(btrim(m.source_if_ka_id), ''),
    NULLIF(btrim(m.source_if_no_ka_id), '')
  ) IS NOT NULL;

COMMIT;
