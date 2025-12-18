-- 002_create_staging_tables.sql
BEGIN;

-- KELPS raw load table:
-- Stores key columns as columns + full row JSON for complete fidelity.
CREATE TABLE IF NOT EXISTS staging.kelps_raw (
  staging_row_id BIGSERIAL PRIMARY KEY,
  ingest_batch_id UUID NOT NULL REFERENCES structured.ingest_batch(ingest_batch_id) ON DELETE CASCADE,
  source_filename TEXT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Authoritative specimen identifier
  storage_details_id TEXT NULL,

  -- Commonly used “required variables” (as columns for convenience)
  taxonomy_genus TEXT NULL,
  taxonomy_species TEXT NULL,
  taxonomy_sex TEXT NULL,
  taxonomy_variety_or_form TEXT NULL,

  sampling_country TEXT NULL,
  sampling_latitude_raw TEXT NULL,
  sampling_longitude_raw TEXT NULL,
  sampling_collection_date_raw TEXT NULL,
  sampling_collection_site TEXT NULL,
  sampling_permit_type TEXT NULL,
  sampling_personnel_collected TEXT NULL,
  sampling_deposited_by TEXT NULL,

  storage_location_site TEXT NULL,
  storage_position_id TEXT NULL,
  storage_rack_id TEXT NULL,
  storage_temperature_c_raw TEXT NULL,
  storage_medium TEXT NULL,
  other_previously_housed_location TEXT NULL,

  sponsorship_status TEXT NULL,
  sponsorship_code TEXT NULL,

  phenotypic_growth_rate TEXT NULL,
  phenotypic_optimal_growth_conditions TEXT NULL,
  phenotypic_percent_viability TEXT NULL,
  phenotypic_lifespan TEXT NULL,
  phenotypic_tolerance_thermal_stressor TEXT NULL,
  phenotypic_tolerance_water_quality_stressors TEXT NULL,

  inaturalist_url TEXT NULL,

  -- Full raw row from Excel/CSV (all columns)
  raw_jsonb JSONB NOT NULL,

  -- Optional: store the original CSV row text for auditing
  raw_row_text TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_stg_kelps_storage_id ON staging.kelps_raw(storage_details_id);
CREATE INDEX IF NOT EXISTS idx_stg_kelps_tax ON staging.kelps_raw(taxonomy_genus, taxonomy_species);
CREATE INDEX IF NOT EXISTS idx_stg_kelps_country ON staging.kelps_raw(sampling_country);

-- MICROBES raw load table
CREATE TABLE IF NOT EXISTS staging.microbes_raw (
  staging_row_id BIGSERIAL PRIMARY KEY,
  ingest_batch_id UUID NOT NULL REFERENCES structured.ingest_batch(ingest_batch_id) ON DELETE CASCADE,
  source_filename TEXT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Authoritative isolate identifier
  microbe_id TEXT NULL,
  original_code TEXT NULL,

  isolated_year TEXT NULL,
  isolated_by TEXT NULL,
  maintained_by TEXT NULL,
  maintained_at TEXT NULL,

  kelp_host TEXT NULL,
  kelp_ka_sample_id TEXT NULL,
  kelp_location TEXT NULL,
  kelp_collection_temp_raw TEXT NULL,
  kelp_collection_month TEXT NULL,
  kelp_collection_season TEXT NULL,

  cryopreservation_date_raw TEXT NULL,
  cryo_storage_medium TEXT NULL,
  cryo_storage_preservative TEXT NULL,
  cryo_revival_tested TEXT NULL,
  cryo_backups_created TEXT NULL,

  closest_ncbi_blast_tax_id TEXT NULL,
  percent_identity TEXT NULL,
  accession TEXT NULL,
  taxonomy_kingdom TEXT NULL,

  raw_jsonb JSONB NOT NULL,
  raw_row_text TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_stg_microbes_id ON staging.microbes_raw(microbe_id);
CREATE INDEX IF NOT EXISTS idx_stg_microbes_host ON staging.microbes_raw(kelp_host);
CREATE INDEX IF NOT EXISTS idx_stg_microbes_blast ON staging.microbes_raw(closest_ncbi_blast_tax_id);

COMMIT;
