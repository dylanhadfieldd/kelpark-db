-- 004_create_typed_staging.sql
-- Purpose:
--   Create typed staging tables from staging.*_raw, normalizing placeholders
--   (NA, None, Not_Yet_Assessed, TBD, empty) to NULL, and casting where safe.
--
-- Key fix vs prior version:
--   - Safe numeric parsing no longer produces invalid casts like "12-"
--   - Values like "12-Oct" (Excel auto-date style) will parse to 12 (first numeric token)
--
-- Assumptions:
--   - Raw tables exist: staging.kelps_raw, staging.microbes_raw
--   - staging.kelps_raw and staging.microbes_raw include the referenced columns

BEGIN;

CREATE SCHEMA IF NOT EXISTS staging;

-- ============================================================
-- TEMP helper functions (session-scoped; no persistent objects)
-- ============================================================

-- Normalize placeholders to NULL and trim whitespace
CREATE OR REPLACE FUNCTION pg_temp.norm_text(p_text text)
RETURNS text
LANGUAGE sql
AS $$
  SELECT CASE
    WHEN p_text IS NULL THEN NULL
    WHEN btrim(p_text) = '' THEN NULL
    WHEN lower(btrim(p_text)) IN (
      'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed'
    ) THEN NULL
    ELSE btrim(p_text)
  END;
$$;

-- Extract first numeric token from a string and cast to numeric
-- Examples:
--   '10'      -> 10
--   '10 C'    -> 10
--   '12-Oct'  -> 12     (fixes your current issue)
--   '12-'     -> 12     (safe)
--   '--'      -> NULL
CREATE OR REPLACE FUNCTION pg_temp.safe_numeric(p_text text)
RETURNS numeric
LANGUAGE sql
AS $$
  SELECT CASE
    WHEN pg_temp.norm_text(p_text) IS NULL THEN NULL
    ELSE (
      SELECT (regexp_match(pg_temp.norm_text(p_text), '(-?\d+(?:\.\d+)?)'))[1]::numeric
    )
  END;
$$;

-- Extract first integer token and cast to int
CREATE OR REPLACE FUNCTION pg_temp.safe_int(p_text text)
RETURNS integer
LANGUAGE sql
AS $$
  SELECT CASE
    WHEN pg_temp.norm_text(p_text) IS NULL THEN NULL
    ELSE (
      SELECT (regexp_match(pg_temp.norm_text(p_text), '(\d+)'))[1]::int
    )
  END;
$$;

-- Best-effort boolean parsing
CREATE OR REPLACE FUNCTION pg_temp.safe_bool(p_text text)
RETURNS boolean
LANGUAGE sql
AS $$
  SELECT CASE
    WHEN pg_temp.norm_text(p_text) IS NULL THEN NULL
    WHEN lower(pg_temp.norm_text(p_text)) IN ('yes','true','1','y','t') THEN true
    WHEN lower(pg_temp.norm_text(p_text)) IN ('no','false','0','n','f') THEN false
    ELSE NULL
  END;
$$;

-- Best-effort date parsing (keeps NULL on failure)
-- Supports:
--   YYYY-MM-DD
--   DD-Mon-YY / DD-Mon-YYYY
--   MM/DD/YYYY
CREATE OR REPLACE FUNCTION pg_temp.safe_date(p_text text)
RETURNS date
LANGUAGE sql
AS $$
  SELECT CASE
    WHEN pg_temp.norm_text(p_text) IS NULL THEN NULL
    WHEN pg_temp.norm_text(p_text) ~ '^\d{4}-\d{2}-\d{2}$'
      THEN to_date(pg_temp.norm_text(p_text), 'YYYY-MM-DD')
    WHEN pg_temp.norm_text(p_text) ~ '^\d{1,2}-[A-Za-z]{3}-\d{2}$'
      THEN to_date(pg_temp.norm_text(p_text), 'DD-Mon-YY')
    WHEN pg_temp.norm_text(p_text) ~ '^\d{1,2}-[A-Za-z]{3}-\d{4}$'
      THEN to_date(pg_temp.norm_text(p_text), 'DD-Mon-YYYY')
    WHEN pg_temp.norm_text(p_text) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
      THEN to_date(pg_temp.norm_text(p_text), 'MM/DD/YYYY')
    ELSE NULL
  END;
$$;

-- ============================================================
-- Drop & recreate typed tables
-- ============================================================
DROP TABLE IF EXISTS staging.kelps_typed;
DROP TABLE IF EXISTS staging.microbes_typed;

-- ============================================================
-- KELPS_TYPED
-- ============================================================
CREATE TABLE staging.kelps_typed (
  typed_id          BIGSERIAL PRIMARY KEY,

  -- Ingest metadata carried forward
  staging_id        BIGINT NOT NULL,
  ingest_batch_id   UUID NULL,
  source_filename   TEXT NULL,
  source_row_num    INTEGER NULL,
  loaded_at         TIMESTAMPTZ NULL,

  -- Taxonomy
  taxonomy_genus               TEXT NULL,
  taxonomy_species             TEXT NULL,
  taxonomy_sex                 TEXT NULL,
  taxonomy_variety_or_form     TEXT NULL,

  -- Storage details
  storage_details_id           TEXT NULL,
  storage_details_position_id  TEXT NULL,
  storage_details_rack_id      TEXT NULL,
  storage_details_location     TEXT NULL,
  storage_details_temperature_c_raw TEXT NULL,
  storage_details_temperature_c_num NUMERIC NULL,
  storage_details_medium       TEXT NULL,

  -- Sampling metadata
  sampling_metadata_country    TEXT NULL,
  sampling_metadata_latitude_raw  TEXT NULL,
  sampling_metadata_longitude_raw TEXT NULL,
  sampling_metadata_latitude_num  NUMERIC NULL,
  sampling_metadata_longitude_num NUMERIC NULL,
  sampling_metadata_collection_date_raw TEXT NULL,
  sampling_metadata_collection_date DATE NULL,
  sampling_metadata_personnel_collected TEXT NULL,
  sampling_metadata_isolation_date_raw TEXT NULL,
  sampling_metadata_isolation_date DATE NULL,
  sampling_metadata_deposit_date_raw TEXT NULL,
  sampling_metadata_deposit_date DATE NULL,
  sampling_metadata_deposited_by TEXT NULL,
  sampling_metadata_permit      TEXT NULL,
  sampling_metadata_collection_site TEXT NULL,

  -- Misc provenance
  other_previously_housed_location TEXT NULL,
  sponsorship_strain_sponsorship_status TEXT NULL,
  sponsorship_code TEXT NULL,

  -- Phenotypic
  phenotypic_data_growth_rate_raw TEXT NULL,
  phenotypic_data_growth_rate_num NUMERIC NULL,
  phenotypic_data_optimal_growth_conditions TEXT NULL,
  phenotypic_data_percent_viability_raw TEXT NULL,
  phenotypic_data_percent_viability_num NUMERIC NULL,
  phenotypic_data_lifespan TEXT NULL,
  phenotypic_data_tolerance_to_thermal_stressor TEXT NULL,
  phenotypic_data_tolerance_to_water_quality_stressors TEXT NULL,

  -- Links / annotations
  inaturalist TEXT NULL,

  -- Ecological roles
  ecological_role_primary_producer TEXT NULL,
  ecological_role_carbon_sink TEXT NULL,
  ecological_role_habitat_former TEXT NULL,

  -- Pathways / annotation ids
  metabolic_pathways_kegg_pathway_id TEXT NULL,
  metabolic_pathways_metacyc_pathway_id TEXT NULL,
  functional_annotation_gene_function_id TEXT NULL,
  functional_annotation_protein_function_id TEXT NULL,

  -- Genetic variation
  genetic_variation_data_variant_id TEXT NULL,
  genetic_variation_data_gene_id TEXT NULL,
  genetic_variation_data_chromosome TEXT NULL,
  genetic_variation_data_reference_allele TEXT NULL,
  genetic_variation_data_alternate_allele TEXT NULL,
  genetic_variation_data_variant_type TEXT NULL,
  genetic_variation_data_allele_frequency_raw TEXT NULL,
  genetic_variation_data_allele_frequency_num NUMERIC NULL,
  genetic_variation_data_read_depth_raw TEXT NULL,
  genetic_variation_data_read_depth_num NUMERIC NULL,
  genetic_variation_data_quality_score_raw TEXT NULL,
  genetic_variation_data_quality_score_num NUMERIC NULL,
  genetic_variation_data_genotype TEXT NULL,

  -- Diversity stats
  genetic_diversity_fst_raw TEXT NULL,
  genetic_diversity_fst_num NUMERIC NULL,
  genetic_diversity_observed_heterozygosity_raw TEXT NULL,
  genetic_diversity_observed_heterozygosity_num NUMERIC NULL,
  genetic_diversity_observed_homozygosity_raw TEXT NULL,
  genetic_diversity_observed_homozygosity_num NUMERIC NULL,
  genetic_diversity_allele_count_raw TEXT NULL,
  genetic_diversity_allele_count_num NUMERIC NULL,
  genetic_diversity_nucleotide_diversity_raw TEXT NULL,
  genetic_diversity_nucleotide_diversity_num NUMERIC NULL,

  phenotypic_diversity_trait_id_name TEXT NULL,
  phenotypic_diversity_trait_variance_raw TEXT NULL,
  phenotypic_diversity_trait_variance_num NUMERIC NULL,
  phenotypic_diversity_trait_mean_raw TEXT NULL,
  phenotypic_diversity_trait_mean_num NUMERIC NULL,
  phenotypic_diversity_trait_standard_deviation_raw TEXT NULL,
  phenotypic_diversity_trait_standard_deviation_num NUMERIC NULL,
  phenotypic_diversity_trait_range TEXT NULL,

  -- IUCN flags
  iucn_red_list_extinct_ex BOOLEAN NULL,
  iucn_red_list_extinct_in_the_wild_ew BOOLEAN NULL,
  iucn_red_list_critically_endangered_cr BOOLEAN NULL,
  iucn_red_list_endangered_en BOOLEAN NULL,
  iucn_red_list_vulnerable_vu BOOLEAN NULL,
  iucn_red_list_least_concern BOOLEAN NULL,
  iucn_red_list_data_deficient_dd BOOLEAN NULL,
  iucn_red_list_not_evaluated_ne BOOLEAN NULL,

  -- Ecosystem flags
  ecosystem_endemic BOOLEAN NULL,
  ecosystem_naturalized BOOLEAN NULL,
  ecosystem_invasive BOOLEAN NULL,
  ecosystem_adventive BOOLEAN NULL,
  ecosystem_extirpated BOOLEAN NULL,
  ecosystem_weed BOOLEAN NULL,
  ecosystem_cultivated_horticultural BOOLEAN NULL,
  ecosystem_ruderal BOOLEAN NULL,
  ecosystem_pioneer BOOLEAN NULL,

  -- Commercial
  commercial_bio_variables_harvestable_yield_per_cycle_raw TEXT NULL,
  commercial_bio_variables_harvestable_yield_per_cycle_num NUMERIC NULL,
  commercial_bio_variables_harvest_season TEXT NULL,
  commercial_bio_variables_light_needed TEXT NULL,
  commercial_production_spoilage_rate_raw TEXT NULL,
  commercial_production_spoilage_rate_num NUMERIC NULL,
  commercial_production_operational_cost_per_cycle_raw TEXT NULL,
  commercial_production_operational_cost_per_cycle_num NUMERIC NULL,
  commercial_production_gross_margin_raw TEXT NULL,
  commercial_production_gross_margin_num NUMERIC NULL,
  commercial_market_price_volatility_index_raw TEXT NULL,
  commercial_market_price_volatility_index_num NUMERIC NULL,
  commercial_market_demand_index_sector TEXT NULL,
  commercial_market_market_growth_rate_raw TEXT NULL,
  commercial_market_market_growth_rate_num NUMERIC NULL,
  commercial_processing_moisture_content_raw TEXT NULL,
  commercial_processing_moisture_content_num NUMERIC NULL,
  commercial_processing_protein_content_raw TEXT NULL,
  commercial_processing_protein_content_num NUMERIC NULL,
  commercial_processing_alginate_or_carrageenan_content_raw TEXT NULL,
  commercial_processing_alginate_or_carrageenan_content_num NUMERIC NULL,
  commercial_processing_contaminants TEXT NULL,
  commercial_processing_shelf_life TEXT NULL,
  commercial_processing_grade_quality_score_raw TEXT NULL,
  commercial_processing_grade_quality_score_num NUMERIC NULL,
  commercial_supply_logistics_transport_cost_raw TEXT NULL,
  commercial_supply_logistics_transport_cost_num NUMERIC NULL,
  commercial_supply_logistics_distribution_channel TEXT NULL,
  commercial_supply_logistics_carbon_footprint_transport_raw TEXT NULL,
  commercial_supply_logistics_carbon_footprint_transport_num NUMERIC NULL
);

INSERT INTO staging.kelps_typed
SELECT
  -- Ingest metadata
  k.staging_id,
  k.ingest_batch_id,
  k.source_filename,
  k.source_row_num,
  k.loaded_at,

  -- Taxonomy
  pg_temp.norm_text(k.taxonomy_genus),
  pg_temp.norm_text(k.taxonomy_species),
  pg_temp.norm_text(k.taxonomy_sex),
  pg_temp.norm_text(k.taxonomy_variety_or_form),

  -- Storage
  pg_temp.norm_text(k.storage_details_id),
  pg_temp.norm_text(k.storage_details_position_id),
  pg_temp.norm_text(k.storage_details_rack_id),
  pg_temp.norm_text(k.storage_details_location),
  pg_temp.norm_text(k.storage_details_temperature_c),
  pg_temp.safe_numeric(k.storage_details_temperature_c),
  pg_temp.norm_text(k.storage_details_medium),

  -- Sampling metadata
  pg_temp.norm_text(k.sampling_metadata_country),
  pg_temp.norm_text(k.sampling_metadata_latitude),
  pg_temp.norm_text(k.sampling_metadata_longitude),
  CASE
    WHEN pg_temp.norm_text(k.sampling_metadata_latitude) ~ '^\s*-?\d+(\.\d+)?\s*$'
      THEN pg_temp.safe_numeric(k.sampling_metadata_latitude)
    ELSE NULL
  END,
  CASE
    WHEN pg_temp.norm_text(k.sampling_metadata_longitude) ~ '^\s*-?\d+(\.\d+)?\s*$'
      THEN pg_temp.safe_numeric(k.sampling_metadata_longitude)
    ELSE NULL
  END,
  pg_temp.norm_text(k.sampling_metadata_collection_date),
  pg_temp.safe_date(k.sampling_metadata_collection_date),
  pg_temp.norm_text(k.sampling_metadata_personnel_collected),
  pg_temp.norm_text(k.sampling_metadata_isolation_date),
  pg_temp.safe_date(k.sampling_metadata_isolation_date),
  pg_temp.norm_text(k.sampling_metadata_deposit_date),
  pg_temp.safe_date(k.sampling_metadata_deposit_date),
  pg_temp.norm_text(k.sampling_metadata_deposited_by),
  pg_temp.norm_text(k.sampling_metadata_permit),
  pg_temp.norm_text(k.sampling_metadata_collection_site),

  -- Misc provenance
  pg_temp.norm_text(k.other_previously_housed_location),
  pg_temp.norm_text(k.sponsorship_strain_sponsorship_status),
  pg_temp.norm_text(k.sponsorship_code),

  -- Phenotypic
  pg_temp.norm_text(k.phenotypic_data_growth_rate),
  pg_temp.safe_numeric(k.phenotypic_data_growth_rate),
  pg_temp.norm_text(k.phenotypic_data_optimal_growth_conditions),
  pg_temp.norm_text(k.phenotypic_data_percent_viability),
  pg_temp.safe_numeric(k.phenotypic_data_percent_viability),
  pg_temp.norm_text(k.phenotypic_data_lifespan),
  pg_temp.norm_text(k.phenotypic_data_tolerance_to_thermal_stressor),
  pg_temp.norm_text(k.phenotypic_data_tolerance_to_water_quality_stressors),

  -- Links / annotations
  pg_temp.norm_text(k.inaturalist),

  -- Ecological roles
  pg_temp.norm_text(k.ecological_role_primary_producer),
  pg_temp.norm_text(k.ecological_role_carbon_sink),
  pg_temp.norm_text(k.ecological_role_habitat_former),

  -- Pathways / annotation ids
  pg_temp.norm_text(k.metabolic_pathways_kegg_pathway_id),
  pg_temp.norm_text(k.metabolic_pathways_metacyc_pathway_id),
  pg_temp.norm_text(k.functional_annotation_gene_function_id),
  pg_temp.norm_text(k.functional_annotation_protein_function_id),

  -- Genetic variation
  pg_temp.norm_text(k.genetic_variation_data_variant_id),
  pg_temp.norm_text(k.genetic_variation_data_gene_id),
  pg_temp.norm_text(k.genetic_variation_data_chromosome),
  pg_temp.norm_text(k.genetic_variation_data_reference_allele),
  pg_temp.norm_text(k.genetic_variation_data_alternate_allele),
  pg_temp.norm_text(k.genetic_variation_data_variant_type),
  pg_temp.norm_text(k.genetic_variation_data_allele_frequency),
  pg_temp.safe_numeric(k.genetic_variation_data_allele_frequency),
  pg_temp.norm_text(k.genetic_variation_data_read_depth),
  pg_temp.safe_numeric(k.genetic_variation_data_read_depth),
  pg_temp.norm_text(k.genetic_variation_data_quality_score),
  pg_temp.safe_numeric(k.genetic_variation_data_quality_score),
  pg_temp.norm_text(k.genetic_variation_data_genotype),

  -- Diversity stats (note: your raw has truncated column names here; keep using what worked for you)
  pg_temp.norm_text(k.genetic_diversity_within_geography_sample_sets_fst),
  pg_temp.safe_numeric(k.genetic_diversity_within_geography_sample_sets_fst),
  pg_temp.norm_text(k.genetic_diversity_within_geography_sample_sets_observed_heteroz),
  pg_temp.safe_numeric(k.genetic_diversity_within_geography_sample_sets_observed_heteroz),
  pg_temp.norm_text(k.genetic_diversity_within_geography_sample_sets_observed_homozyg),
  pg_temp.safe_numeric(k.genetic_diversity_within_geography_sample_sets_observed_homozyg),
  pg_temp.norm_text(k.genetic_diversity_within_geography_sample_sets_allele_count),
  pg_temp.safe_numeric(k.genetic_diversity_within_geography_sample_sets_allele_count),
  pg_temp.norm_text(k.genetic_diversity_within_geography_sample_sets_nucleotide_diver),
  pg_temp.safe_numeric(k.genetic_diversity_within_geography_sample_sets_nucleotide_diver),

  pg_temp.norm_text(k.phenotypic_diversity_within_geography_sample_sets_trait_id_name),
  pg_temp.norm_text(k.phenotypic_diversity_within_geography_sample_sets_trait_varianc),
  pg_temp.safe_numeric(k.phenotypic_diversity_within_geography_sample_sets_trait_varianc),
  pg_temp.norm_text(k.phenotypic_diversity_within_geography_sample_sets_trait_mean),
  pg_temp.safe_numeric(k.phenotypic_diversity_within_geography_sample_sets_trait_mean),
  pg_temp.norm_text(k.phenotypic_diversity_within_geography_sample_sets_trait_standar),
  pg_temp.safe_numeric(k.phenotypic_diversity_within_geography_sample_sets_trait_standar),
  pg_temp.norm_text(k.phenotypic_diversity_within_geography_sample_sets_trait_range),

  -- IUCN flags
  pg_temp.safe_bool(k.iucn_red_list_extinct_ex),
  pg_temp.safe_bool(k.iucn_red_list_extinct_in_the_wild_ew),
  pg_temp.safe_bool(k.iucn_red_list_critically_endangered_cr),
  pg_temp.safe_bool(k.iucn_red_list_endangered_en),
  pg_temp.safe_bool(k.iucn_red_list_vulnerable_vu),
  pg_temp.safe_bool(k.iucn_red_list_least_concern),
  pg_temp.safe_bool(k.iucn_red_list_data_deficient_dd),
  pg_temp.safe_bool(k.iucn_red_list_not_evaluated_ne),

  -- Ecosystem flags
  pg_temp.safe_bool(k.ecosystem_endemic),
  pg_temp.safe_bool(k.ecosystem_naturalized),
  pg_temp.safe_bool(k.ecosystem_invasive),
  pg_temp.safe_bool(k.ecosystem_adventive),
  pg_temp.safe_bool(k.ecosystem_extirpated),
  pg_temp.safe_bool(k.ecosystem_weed),
  pg_temp.safe_bool(k.ecosystem_cultivated_horticultural),
  pg_temp.safe_bool(k.ecosystem_ruderal),
  pg_temp.safe_bool(k.ecosystem_pioneer),

  -- Commercial
  pg_temp.norm_text(k.commercial_bio_variables_harvestable_yield_per_cycle),
  pg_temp.safe_numeric(k.commercial_bio_variables_harvestable_yield_per_cycle),
  pg_temp.norm_text(k.commercial_bio_variables_harvest_season),
  pg_temp.norm_text(k.commercial_bio_variables_light_needed),
  pg_temp.norm_text(k.commercial_production_spoilage_rate),
  pg_temp.safe_numeric(k.commercial_production_spoilage_rate),
  pg_temp.norm_text(k.commercial_production_operational_cost_per_cycle),
  pg_temp.safe_numeric(k.commercial_production_operational_cost_per_cycle),
  pg_temp.norm_text(k.commercial_production_gross_margin),
  pg_temp.safe_numeric(k.commercial_production_gross_margin),
  pg_temp.norm_text(k.commercial_market_price_volatility_index),
  pg_temp.safe_numeric(k.commercial_market_price_volatility_index),
  pg_temp.norm_text(k.commercial_market_demand_index_sector),
  pg_temp.norm_text(k.commercial_market_market_growth_rate),
  pg_temp.safe_numeric(k.commercial_market_market_growth_rate),
  pg_temp.norm_text(k.commercial_processing_moisture_content),
  pg_temp.safe_numeric(k.commercial_processing_moisture_content),
  pg_temp.norm_text(k.commercial_processing_protein_content),
  pg_temp.safe_numeric(k.commercial_processing_protein_content),
  pg_temp.norm_text(k.commercial_processing_alginate_or_carrageenan_content),
  pg_temp.safe_numeric(k.commercial_processing_alginate_or_carrageenan_content),
  pg_temp.norm_text(k.commercial_processing_contaminants),
  pg_temp.norm_text(k.commercial_processing_shelf_life),
  pg_temp.norm_text(k.commercial_processing_grade_quality_score),
  pg_temp.safe_numeric(k.commercial_processing_grade_quality_score),
  pg_temp.norm_text(k.commercial_supply_logistics_transport_cost),
  pg_temp.safe_numeric(k.commercial_supply_logistics_transport_cost),
  pg_temp.norm_text(k.commercial_supply_logistics_distribution_channel),
  pg_temp.norm_text(k.commercial_supply_logistics_carbon_footprint_transport),
  pg_temp.safe_numeric(k.commercial_supply_logistics_carbon_footprint_transport)

FROM staging.kelps_raw k;

CREATE INDEX IF NOT EXISTS idx_kelps_typed_batch ON staging.kelps_typed (ingest_batch_id);
CREATE INDEX IF NOT EXISTS idx_kelps_typed_storage_id ON staging.kelps_typed (storage_details_id);
CREATE INDEX IF NOT EXISTS idx_kelps_typed_taxon ON staging.kelps_typed (taxonomy_genus, taxonomy_species);

-- ============================================================
-- MICROBES_TYPED
-- ============================================================
CREATE TABLE staging.microbes_typed (
  typed_id          BIGSERIAL PRIMARY KEY,

  staging_id        BIGINT NOT NULL,
  ingest_batch_id   UUID NULL,
  source_filename   TEXT NULL,
  source_row_num    INTEGER NULL,
  loaded_at         TIMESTAMPTZ NULL,

  microbe_id        TEXT NULL,
  original_code     TEXT NULL,
  institution_isolation_physically_conducted TEXT NULL,
  isolated_year     INTEGER NULL,
  isolated_by       TEXT NULL,
  maintained_by     TEXT NULL,
  maintained_at     TEXT NULL,

  kelp_host         TEXT NULL,
  kelp_ka_sample_id TEXT NULL,
  source_if_ka_id   TEXT NULL,
  source_if_no_ka_id TEXT NULL,
  kelp_location     TEXT NULL,
  kelp_collection_temp_raw TEXT NULL,
  kelp_collection_temp_num NUMERIC NULL,
  kelp_collection_month TEXT NULL,
  kelp_collection_season TEXT NULL,
  kelp_thallus_collection TEXT NULL,
  kelp_collection_approach TEXT NULL,
  kelp_collection_method TEXT NULL,

  microbe_isolation_methods TEXT NULL,
  microbe_isolation_protocol TEXT NULL,
  isolation_media TEXT NULL,

  location_stored1 TEXT NULL,
  location_1_temperature TEXT NULL,
  location_stored2 TEXT NULL,
  location_2_temperature TEXT NULL,

  cryopreservation_date_raw TEXT NULL,
  cryopreservation_date DATE NULL,
  cryo_storage_medium TEXT NULL,
  cryo_storage_preservative TEXT NULL,
  cryo_revival_tested BOOLEAN NULL,
  cryo_backups_created BOOLEAN NULL,
  cryopreservation_protocol TEXT NULL,

  malditof_procedure BOOLEAN NULL,
  malditof_dataanalysis_complete BOOLEAN NULL,
  high_quality_malditof_data TEXT NULL,

  s16_pcr_completed BOOLEAN NULL,
  pcr_conducted_by TEXT NULL,
  sanger_sequencing_completed BOOLEAN NULL,
  sequencing_date_raw TEXT NULL,
  sequencing_date DATE NULL,
  primers_used TEXT NULL,
  sequencing_notes TEXT NULL,
  sequencing_conducted_by TEXT NULL,

  total_bp_length_after_trimming INTEGER NULL,
  closest_ncbi_blast_tax_id TEXT NULL,
  ncbi_blast_query_cover NUMERIC NULL,
  percent_identity NUMERIC NULL,
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

INSERT INTO staging.microbes_typed
SELECT
  m.staging_id, m.ingest_batch_id, m.source_filename, m.source_row_num, m.loaded_at,

  pg_temp.norm_text(m.microbe_id),
  pg_temp.norm_text(m.original_code),
  pg_temp.norm_text(m.institution_isolation_physically_conducted),
  pg_temp.safe_int(m.isolated_year),

  pg_temp.norm_text(m.isolated_by),
  pg_temp.norm_text(m.maintained_by),
  pg_temp.norm_text(m.maintained_at),

  pg_temp.norm_text(m.kelp_host),
  pg_temp.norm_text(m.kelp_ka_sample_id),
  pg_temp.norm_text(m.source_if_ka_id),
  pg_temp.norm_text(m.source_if_no_ka_id),
  pg_temp.norm_text(m.kelp_location),

  pg_temp.norm_text(m.kelp_collection_temp),
  pg_temp.safe_numeric(m.kelp_collection_temp),
  pg_temp.norm_text(m.kelp_collection_month),
  pg_temp.norm_text(m.kelp_collection_season),
  pg_temp.norm_text(m.kelp_thallus_collection),
  pg_temp.norm_text(m.kelp_collection_approach),
  pg_temp.norm_text(m.kelp_collection_method),

  pg_temp.norm_text(m.microbe_isolation_methods),
  pg_temp.norm_text(m.microbe_isolation_protocol),
  pg_temp.norm_text(m.isolation_media),

  pg_temp.norm_text(m.location_stored1),
  pg_temp.norm_text(m.location_1_temperature),
  pg_temp.norm_text(m.location_stored2),
  pg_temp.norm_text(m.location_2_temperature),

  pg_temp.norm_text(m.cryopreservation_date),
  pg_temp.safe_date(m.cryopreservation_date),
  pg_temp.norm_text(m.cryo_storage_medium),
  pg_temp.norm_text(m.cryo_storage_preservative),
  pg_temp.safe_bool(m.cryo_revival_tested),
  pg_temp.safe_bool(m.cryo_backups_created),
  pg_temp.norm_text(m.cryopreservation_protocol),

  pg_temp.safe_bool(m.malditof_procedure),
  pg_temp.safe_bool(m.malditof_dataanalysis_complete),
  pg_temp.norm_text(m.high_quality_malditof_data),

  pg_temp.safe_bool(m.s16_pcr_completed),
  pg_temp.norm_text(m.pcr_conducted_by),
  pg_temp.safe_bool(m.sanger_sequencing_completed),
  pg_temp.norm_text(m.sequencing_date),
  pg_temp.safe_date(m.sequencing_date),
  pg_temp.norm_text(m.primers_used),
  pg_temp.norm_text(m.sequencing_notes),
  pg_temp.norm_text(m.sequencing_conducted_by),

  pg_temp.safe_int(m.total_bp_length_after_trimming),
  pg_temp.norm_text(m.closest_ncbi_blast_tax_id),
  pg_temp.safe_numeric(m.ncbi_blast_query_cover),
  pg_temp.safe_numeric(m.percent_identity),
  pg_temp.norm_text(m.accession),
  pg_temp.norm_text(m.taxonomy_kingdom),

  pg_temp.norm_text(m.s16_sequence),
  pg_temp.norm_text(m.its2_sequence),

  pg_temp.norm_text(m.pathogen_activity_kelp),
  pg_temp.norm_text(m.pathogen_activity_humans),
  pg_temp.norm_text(m.pathogen_activity_plants),
  pg_temp.norm_text(m.pathogen_activity_animals),

  pg_temp.norm_text(m.growth_temperature_c_range),
  pg_temp.norm_text(m.growth_salinity_range),
  pg_temp.norm_text(m.growth_ph_range),
  pg_temp.norm_text(m.growth_optimal_media),

  pg_temp.norm_text(m.morphology_colony_color),
  pg_temp.norm_text(m.morphology_colony_size),
  pg_temp.norm_text(m.morphology_colony_shape),
  pg_temp.norm_text(m.morphology_colony_texture),
  pg_temp.norm_text(m.gram_stain),
  pg_temp.norm_text(m.morphology_cell_shape),

  pg_temp.norm_text(m.probiotic_activity),
  pg_temp.norm_text(m.probiotic_known_host)

FROM staging.microbes_raw m;

CREATE INDEX IF NOT EXISTS idx_microbes_typed_batch ON staging.microbes_typed (ingest_batch_id);
CREATE INDEX IF NOT EXISTS idx_microbes_typed_microbe_id ON staging.microbes_typed (microbe_id);
CREATE INDEX IF NOT EXISTS idx_microbes_typed_kelp_sample ON staging.microbes_typed (kelp_ka_sample_id);

COMMIT;

-- ============================================================
-- Optional sanity checks (run after script)
-- ============================================================
-- SELECT
--   (SELECT count(*) FROM staging.kelps_raw)   AS kelps_raw_rows,
--   (SELECT count(*) FROM staging.kelps_typed) AS kelps_typed_rows,
--   (SELECT count(*) FROM staging.microbes_raw)   AS microbes_raw_rows,
--   (SELECT count(*) FROM staging.microbes_typed) AS microbes_typed_rows;
--
-- -- Confirm the bad Excel-ish value now parses numerically:
-- SELECT storage_details_temperature_c_raw,
--        storage_details_temperature_c_num,
--        count(*)
-- FROM staging.kelps_typed
-- GROUP BY 1,2
-- ORDER BY count(*) DESC;
--
-- -- Show raw values that did NOT parse to numeric (good for cleanup rules):
-- SELECT storage_details_temperature_c_raw, count(*)
-- FROM staging.kelps_typed
-- WHERE storage_details_temperature_c_raw IS NOT NULL
--   AND storage_details_temperature_c_num IS NULL
-- GROUP BY 1
-- ORDER BY count(*) DESC;
