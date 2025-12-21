-- 004_create_typed_staging.sql
-- Purpose:
--   Create typed staging tables from staging.*_raw, normalizing placeholders
--   (NA, None, Not_Yet_Assessed, TBD, empty) to NULL, and casting where safe.
--
-- Assumptions:
--   - Raw tables exist: staging.kelps_raw, staging.microbes_raw
--   - Extensions: pgcrypto available for gen_random_uuid() (already in your plan)
--
-- Notes:
--   - We DO NOT drop raw tables.
--   - We keep a copy of original raw text columns where useful (e.g., lat/long)
--   - We cast conservatively (dates, numerics, booleans). Anything ambiguous stays TEXT.
--   - Postgres identifiers > 63 chars are truncated; use column_name lookups if you extend.

BEGIN;

CREATE SCHEMA IF NOT EXISTS staging;

-- =========================
-- Helpers (inline patterns)
-- =========================
-- We'll normalize common placeholders to NULL during SELECTs using NULLIF + CASE.
-- Placeholder set (case-insensitive):
--   '', 'na', 'n/a', 'none', 'null', 'not_yet_assessed', 'not yet assessed', 'tbd', 'to_be_analyzed'
--
-- We'll also trim whitespace on all fields.

-- =========================
-- Drop & recreate typed tables
-- =========================
DROP TABLE IF EXISTS staging.kelps_typed;
DROP TABLE IF EXISTS staging.microbes_typed;

-- =========================
-- KELPS_TYPED
-- =========================
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
  storage_details_temperature_c_num NUMERIC NULL,   -- best-effort numeric extraction
  storage_details_medium       TEXT NULL,

  -- Sampling metadata
  sampling_metadata_country    TEXT NULL,
  sampling_metadata_latitude_raw  TEXT NULL,
  sampling_metadata_longitude_raw TEXT NULL,
  sampling_metadata_latitude_num  NUMERIC NULL,     -- only if decimal degrees parseable
  sampling_metadata_longitude_num NUMERIC NULL,     -- only if decimal degrees parseable
  sampling_metadata_collection_date_raw TEXT NULL,
  sampling_metadata_collection_date DATE NULL,      -- parsed if possible
  sampling_metadata_personnel_collected TEXT NULL,
  sampling_metadata_isolation_date_raw TEXT NULL,
  sampling_metadata_isolation_date DATE NULL,       -- parsed if possible
  sampling_metadata_deposit_date_raw TEXT NULL,
  sampling_metadata_deposit_date DATE NULL,         -- parsed if possible
  sampling_metadata_deposited_by TEXT NULL,
  sampling_metadata_permit      TEXT NULL,
  sampling_metadata_collection_site TEXT NULL,

  -- Misc provenance
  other_previously_housed_location TEXT NULL,
  sponsorship_strain_sponsorship_status TEXT NULL,
  sponsorship_code TEXT NULL,

  -- Phenotypic (kept mostly TEXT; a few common numerics cast where safe)
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

  -- Ecological roles (often categorical)
  ecological_role_primary_producer TEXT NULL,
  ecological_role_carbon_sink TEXT NULL,
  ecological_role_habitat_former TEXT NULL,

  -- Pathways / annotation ids
  metabolic_pathways_kegg_pathway_id TEXT NULL,
  metabolic_pathways_metacyc_pathway_id TEXT NULL,
  functional_annotation_gene_function_id TEXT NULL,
  functional_annotation_protein_function_id TEXT NULL,

  -- Genetic variation (kept TEXT; numeric casts where clearly numeric)
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

  -- Diversity stats (kept raw + numeric best effort)
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

  -- IUCN flags (best-effort boolean)
  iucn_red_list_extinct_ex BOOLEAN NULL,
  iucn_red_list_extinct_in_the_wild_ew BOOLEAN NULL,
  iucn_red_list_critically_endangered_cr BOOLEAN NULL,
  iucn_red_list_endangered_en BOOLEAN NULL,
  iucn_red_list_vulnerable_vu BOOLEAN NULL,
  iucn_red_list_least_concern BOOLEAN NULL,
  iucn_red_list_data_deficient_dd BOOLEAN NULL,
  iucn_red_list_not_evaluated_ne BOOLEAN NULL,

  -- Ecosystem flags (best-effort boolean)
  ecosystem_endemic BOOLEAN NULL,
  ecosystem_naturalized BOOLEAN NULL,
  ecosystem_invasive BOOLEAN NULL,
  ecosystem_adventive BOOLEAN NULL,
  ecosystem_extirpated BOOLEAN NULL,
  ecosystem_weed BOOLEAN NULL,
  ecosystem_cultivated_horticultural BOOLEAN NULL,
  ecosystem_ruderal BOOLEAN NULL,
  ecosystem_pioneer BOOLEAN NULL,

  -- Commercial (kept mostly text + a few numerics)
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

-- Insert transformed data
INSERT INTO staging.kelps_typed (
  staging_id,
  ingest_batch_id,
  source_filename,
  source_row_num,
  loaded_at,

  taxonomy_genus,
  taxonomy_species,
  taxonomy_sex,
  taxonomy_variety_or_form,

  storage_details_id,
  storage_details_position_id,
  storage_details_rack_id,
  storage_details_location,
  storage_details_temperature_c_raw,
  storage_details_temperature_c_num,
  storage_details_medium,

  sampling_metadata_country,
  sampling_metadata_latitude_raw,
  sampling_metadata_longitude_raw,
  sampling_metadata_latitude_num,
  sampling_metadata_longitude_num,
  sampling_metadata_collection_date_raw,
  sampling_metadata_collection_date,
  sampling_metadata_personnel_collected,
  sampling_metadata_isolation_date_raw,
  sampling_metadata_isolation_date,
  sampling_metadata_deposit_date_raw,
  sampling_metadata_deposit_date,
  sampling_metadata_deposited_by,
  sampling_metadata_permit,
  sampling_metadata_collection_site,

  other_previously_housed_location,
  sponsorship_strain_sponsorship_status,
  sponsorship_code,

  phenotypic_data_growth_rate_raw,
  phenotypic_data_growth_rate_num,
  phenotypic_data_optimal_growth_conditions,
  phenotypic_data_percent_viability_raw,
  phenotypic_data_percent_viability_num,
  phenotypic_data_lifespan,
  phenotypic_data_tolerance_to_thermal_stressor,
  phenotypic_data_tolerance_to_water_quality_stressors,

  inaturalist,
  ecological_role_primary_producer,
  ecological_role_carbon_sink,
  ecological_role_habitat_former,

  metabolic_pathways_kegg_pathway_id,
  metabolic_pathways_metacyc_pathway_id,
  functional_annotation_gene_function_id,
  functional_annotation_protein_function_id,

  genetic_variation_data_variant_id,
  genetic_variation_data_gene_id,
  genetic_variation_data_chromosome,
  genetic_variation_data_reference_allele,
  genetic_variation_data_alternate_allele,
  genetic_variation_data_variant_type,
  genetic_variation_data_allele_frequency_raw,
  genetic_variation_data_allele_frequency_num,
  genetic_variation_data_read_depth_raw,
  genetic_variation_data_read_depth_num,
  genetic_variation_data_quality_score_raw,
  genetic_variation_data_quality_score_num,
  genetic_variation_data_genotype,

  genetic_diversity_fst_raw,
  genetic_diversity_fst_num,
  genetic_diversity_observed_heterozygosity_raw,
  genetic_diversity_observed_heterozygosity_num,
  genetic_diversity_observed_homozygosity_raw,
  genetic_diversity_observed_homozygosity_num,
  genetic_diversity_allele_count_raw,
  genetic_diversity_allele_count_num,
  genetic_diversity_nucleotide_diversity_raw,
  genetic_diversity_nucleotide_diversity_num,

  phenotypic_diversity_trait_id_name,
  phenotypic_diversity_trait_variance_raw,
  phenotypic_diversity_trait_variance_num,
  phenotypic_diversity_trait_mean_raw,
  phenotypic_diversity_trait_mean_num,
  phenotypic_diversity_trait_standard_deviation_raw,
  phenotypic_diversity_trait_standard_deviation_num,
  phenotypic_diversity_trait_range,

  iucn_red_list_extinct_ex,
  iucn_red_list_extinct_in_the_wild_ew,
  iucn_red_list_critically_endangered_cr,
  iucn_red_list_endangered_en,
  iucn_red_list_vulnerable_vu,
  iucn_red_list_least_concern,
  iucn_red_list_data_deficient_dd,
  iucn_red_list_not_evaluated_ne,

  ecosystem_endemic,
  ecosystem_naturalized,
  ecosystem_invasive,
  ecosystem_adventive,
  ecosystem_extirpated,
  ecosystem_weed,
  ecosystem_cultivated_horticultural,
  ecosystem_ruderal,
  ecosystem_pioneer,

  commercial_bio_variables_harvestable_yield_per_cycle_raw,
  commercial_bio_variables_harvestable_yield_per_cycle_num,
  commercial_bio_variables_harvest_season,
  commercial_bio_variables_light_needed,
  commercial_production_spoilage_rate_raw,
  commercial_production_spoilage_rate_num,
  commercial_production_operational_cost_per_cycle_raw,
  commercial_production_operational_cost_per_cycle_num,
  commercial_production_gross_margin_raw,
  commercial_production_gross_margin_num,
  commercial_market_price_volatility_index_raw,
  commercial_market_price_volatility_index_num,
  commercial_market_demand_index_sector,
  commercial_market_market_growth_rate_raw,
  commercial_market_market_growth_rate_num,
  commercial_processing_moisture_content_raw,
  commercial_processing_moisture_content_num,
  commercial_processing_protein_content_raw,
  commercial_processing_protein_content_num,
  commercial_processing_alginate_or_carrageenan_content_raw,
  commercial_processing_alginate_or_carrageenan_content_num,
  commercial_processing_contaminants,
  commercial_processing_shelf_life,
  commercial_processing_grade_quality_score_raw,
  commercial_processing_grade_quality_score_num,
  commercial_supply_logistics_transport_cost_raw,
  commercial_supply_logistics_transport_cost_num,
  commercial_supply_logistics_distribution_channel,
  commercial_supply_logistics_carbon_footprint_transport_raw,
  commercial_supply_logistics_carbon_footprint_transport_num
)
SELECT
  k.staging_id,
  k.ingest_batch_id,
  k.source_filename,
  k.source_row_num,
  k.loaded_at,

  -- Normalize placeholders to NULL
  NULLIF(trim(k.taxonomy_genus), '')::text,
  NULLIF(trim(k.taxonomy_species), '')::text,
  CASE
    WHEN lower(trim(coalesce(k.taxonomy_sex,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE trim(k.taxonomy_sex)
  END,
  CASE
    WHEN lower(trim(coalesce(k.taxonomy_variety_or_form,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE trim(k.taxonomy_variety_or_form)
  END,

  CASE WHEN lower(trim(coalesce(k.storage_details_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.storage_details_id) END,
  CASE WHEN lower(trim(coalesce(k.storage_details_position_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.storage_details_position_id) END,
  CASE WHEN lower(trim(coalesce(k.storage_details_rack_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.storage_details_rack_id) END,
  CASE WHEN lower(trim(coalesce(k.storage_details_location,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.storage_details_location) END,
  CASE WHEN lower(trim(coalesce(k.storage_details_temperature_c,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.storage_details_temperature_c) END,
  -- numeric extraction: keep digits, dot, minus
  CASE
    WHEN lower(trim(coalesce(k.storage_details_temperature_c,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.storage_details_temperature_c), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.storage_details_medium,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.storage_details_medium) END,

  CASE WHEN lower(trim(coalesce(k.sampling_metadata_country,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.sampling_metadata_country) END,
  CASE WHEN lower(trim(coalesce(k.sampling_metadata_latitude,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.sampling_metadata_latitude) END,
  CASE WHEN lower(trim(coalesce(k.sampling_metadata_longitude,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.sampling_metadata_longitude) END,
  -- numeric lat/long only if already decimal; DMS stays NULL for now
  CASE
    WHEN k.sampling_metadata_latitude ~ '^\s*-?\d+(\.\d+)?\s*$'
      THEN trim(k.sampling_metadata_latitude)::numeric
    ELSE NULL
  END,
  CASE
    WHEN k.sampling_metadata_longitude ~ '^\s*-?\d+(\.\d+)?\s*$'
      THEN trim(k.sampling_metadata_longitude)::numeric
    ELSE NULL
  END,
  CASE WHEN lower(trim(coalesce(k.sampling_metadata_collection_date,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.sampling_metadata_collection_date) END,
  -- date parsing attempts: DD-Mon-YY / DD-Mon-YYYY / M/D/YYYY
  CASE
    WHEN lower(trim(coalesce(k.sampling_metadata_collection_date,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    WHEN trim(k.sampling_metadata_collection_date) ~ '^\d{1,2}-[A-Za-z]{3}-\d{2,4}$'
      THEN to_date(trim(k.sampling_metadata_collection_date), CASE WHEN length(trim(k.sampling_metadata_collection_date)) = 9 THEN 'DD-Mon-YY' ELSE 'DD-Mon-YYYY' END)
    WHEN trim(k.sampling_metadata_collection_date) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
      THEN to_date(trim(k.sampling_metadata_collection_date), 'MM/DD/YYYY')
    ELSE NULL
  END,
  CASE WHEN lower(trim(coalesce(k.sampling_metadata_personnel_collected,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.sampling_metadata_personnel_collected) END,
  CASE WHEN lower(trim(coalesce(k.sampling_metadata_isolation_date,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.sampling_metadata_isolation_date) END,
  CASE
    WHEN lower(trim(coalesce(k.sampling_metadata_isolation_date,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    WHEN trim(k.sampling_metadata_isolation_date) ~ '^\d{1,2}-[A-Za-z]{3}-\d{2,4}$'
      THEN to_date(trim(k.sampling_metadata_isolation_date), CASE WHEN length(trim(k.sampling_metadata_isolation_date)) = 9 THEN 'DD-Mon-YY' ELSE 'DD-Mon-YYYY' END)
    WHEN trim(k.sampling_metadata_isolation_date) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
      THEN to_date(trim(k.sampling_metadata_isolation_date), 'MM/DD/YYYY')
    ELSE NULL
  END,
  CASE WHEN lower(trim(coalesce(k.sampling_metadata_deposit_date,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.sampling_metadata_deposit_date) END,
  CASE
    WHEN lower(trim(coalesce(k.sampling_metadata_deposit_date,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    WHEN trim(k.sampling_metadata_deposit_date) ~ '^\d{1,2}-[A-Za-z]{3}-\d{2,4}$'
      THEN to_date(trim(k.sampling_metadata_deposit_date), CASE WHEN length(trim(k.sampling_metadata_deposit_date)) = 9 THEN 'DD-Mon-YY' ELSE 'DD-Mon-YYYY' END)
    WHEN trim(k.sampling_metadata_deposit_date) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
      THEN to_date(trim(k.sampling_metadata_deposit_date), 'MM/DD/YYYY')
    ELSE NULL
  END,
  CASE WHEN lower(trim(coalesce(k.sampling_metadata_deposited_by,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.sampling_metadata_deposited_by) END,
  CASE WHEN lower(trim(coalesce(k.sampling_metadata_permit,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.sampling_metadata_permit) END,
  CASE WHEN lower(trim(coalesce(k.sampling_metadata_collection_site,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.sampling_metadata_collection_site) END,

  CASE WHEN lower(trim(coalesce(k.other_previously_housed_location,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.other_previously_housed_location) END,
  CASE WHEN lower(trim(coalesce(k.sponsorship_strain_sponsorship_status,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.sponsorship_strain_sponsorship_status) END,
  CASE WHEN lower(trim(coalesce(k.sponsorship_code,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.sponsorship_code) END,

  CASE WHEN lower(trim(coalesce(k.phenotypic_data_growth_rate,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.phenotypic_data_growth_rate) END,
  CASE
    WHEN lower(trim(coalesce(k.phenotypic_data_growth_rate,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.phenotypic_data_growth_rate), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.phenotypic_data_optimal_growth_conditions,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.phenotypic_data_optimal_growth_conditions) END,
  CASE WHEN lower(trim(coalesce(k.phenotypic_data_percent_viability,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.phenotypic_data_percent_viability) END,
  CASE
    WHEN lower(trim(coalesce(k.phenotypic_data_percent_viability,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.phenotypic_data_percent_viability), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.phenotypic_data_lifespan,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.phenotypic_data_lifespan) END,
  CASE WHEN lower(trim(coalesce(k.phenotypic_data_tolerance_to_thermal_stressor,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.phenotypic_data_tolerance_to_thermal_stressor) END,
  CASE WHEN lower(trim(coalesce(k.phenotypic_data_tolerance_to_water_quality_stressors,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.phenotypic_data_tolerance_to_water_quality_stressors) END,

  CASE WHEN lower(trim(coalesce(k.inaturalist,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.inaturalist) END,
  CASE WHEN lower(trim(coalesce(k.ecological_role_primary_producer,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.ecological_role_primary_producer) END,
  CASE WHEN lower(trim(coalesce(k.ecological_role_carbon_sink,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.ecological_role_carbon_sink) END,
  CASE WHEN lower(trim(coalesce(k.ecological_role_habitat_former,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.ecological_role_habitat_former) END,

  CASE WHEN lower(trim(coalesce(k.metabolic_pathways_kegg_pathway_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.metabolic_pathways_kegg_pathway_id) END,
  CASE WHEN lower(trim(coalesce(k.metabolic_pathways_metacyc_pathway_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.metabolic_pathways_metacyc_pathway_id) END,
  CASE WHEN lower(trim(coalesce(k.functional_annotation_gene_function_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.functional_annotation_gene_function_id) END,
  CASE WHEN lower(trim(coalesce(k.functional_annotation_protein_function_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.functional_annotation_protein_function_id) END,

  CASE WHEN lower(trim(coalesce(k.genetic_variation_data_variant_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_variation_data_variant_id) END,
  CASE WHEN lower(trim(coalesce(k.genetic_variation_data_gene_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_variation_data_gene_id) END,
  CASE WHEN lower(trim(coalesce(k.genetic_variation_data_chromosome,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_variation_data_chromosome) END,
  CASE WHEN lower(trim(coalesce(k.genetic_variation_data_reference_allele,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_variation_data_reference_allele) END,
  CASE WHEN lower(trim(coalesce(k.genetic_variation_data_alternate_allele,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_variation_data_alternate_allele) END,
  CASE WHEN lower(trim(coalesce(k.genetic_variation_data_variant_type,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_variation_data_variant_type) END,
  CASE WHEN lower(trim(coalesce(k.genetic_variation_data_allele_frequency,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_variation_data_allele_frequency) END,
  CASE
    WHEN lower(trim(coalesce(k.genetic_variation_data_allele_frequency,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.genetic_variation_data_allele_frequency), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.genetic_variation_data_read_depth,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_variation_data_read_depth) END,
  CASE
    WHEN lower(trim(coalesce(k.genetic_variation_data_read_depth,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.genetic_variation_data_read_depth), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.genetic_variation_data_quality_score,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_variation_data_quality_score) END,
  CASE
    WHEN lower(trim(coalesce(k.genetic_variation_data_quality_score,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.genetic_variation_data_quality_score), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.genetic_variation_data_genotype,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_variation_data_genotype) END,

  -- Diversity: in raw these may be very long column names; your staging table likely has the truncated versions
  CASE WHEN lower(trim(coalesce(k.genetic_diversity_within_geography_sample_sets_fst,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_diversity_within_geography_sample_sets_fst) END,
  CASE
    WHEN lower(trim(coalesce(k.genetic_diversity_within_geography_sample_sets_fst,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.genetic_diversity_within_geography_sample_sets_fst), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.genetic_diversity_within_geography_sample_sets_observed_heteroz,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_diversity_within_geography_sample_sets_observed_heteroz) END,
  CASE
    WHEN lower(trim(coalesce(k.genetic_diversity_within_geography_sample_sets_observed_heteroz,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.genetic_diversity_within_geography_sample_sets_observed_heteroz), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.genetic_diversity_within_geography_sample_sets_observed_homozyg,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_diversity_within_geography_sample_sets_observed_homozyg) END,
  CASE
    WHEN lower(trim(coalesce(k.genetic_diversity_within_geography_sample_sets_observed_homozyg,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.genetic_diversity_within_geography_sample_sets_observed_homozyg), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.genetic_diversity_within_geography_sample_sets_allele_count,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_diversity_within_geography_sample_sets_allele_count) END,
  CASE
    WHEN lower(trim(coalesce(k.genetic_diversity_within_geography_sample_sets_allele_count,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.genetic_diversity_within_geography_sample_sets_allele_count), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.genetic_diversity_within_geography_sample_sets_nucleotide_diver,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.genetic_diversity_within_geography_sample_sets_nucleotide_diver) END,
  CASE
    WHEN lower(trim(coalesce(k.genetic_diversity_within_geography_sample_sets_nucleotide_diver,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.genetic_diversity_within_geography_sample_sets_nucleotide_diver), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,

  CASE WHEN lower(trim(coalesce(k.phenotypic_diversity_within_geography_sample_sets_trait_id_name,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.phenotypic_diversity_within_geography_sample_sets_trait_id_name) END,
  CASE WHEN lower(trim(coalesce(k.phenotypic_diversity_within_geography_sample_sets_trait_varianc,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.phenotypic_diversity_within_geography_sample_sets_trait_varianc) END,
  CASE
    WHEN lower(trim(coalesce(k.phenotypic_diversity_within_geography_sample_sets_trait_varianc,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.phenotypic_diversity_within_geography_sample_sets_trait_varianc), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.phenotypic_diversity_within_geography_sample_sets_trait_mean,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.phenotypic_diversity_within_geography_sample_sets_trait_mean) END,
  CASE
    WHEN lower(trim(coalesce(k.phenotypic_diversity_within_geography_sample_sets_trait_mean,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.phenotypic_diversity_within_geography_sample_sets_trait_mean), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.phenotypic_diversity_within_geography_sample_sets_trait_standar,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.phenotypic_diversity_within_geography_sample_sets_trait_standar) END,
  CASE
    WHEN lower(trim(coalesce(k.phenotypic_diversity_within_geography_sample_sets_trait_standar,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.phenotypic_diversity_within_geography_sample_sets_trait_standar), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.phenotypic_diversity_within_geography_sample_sets_trait_range,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.phenotypic_diversity_within_geography_sample_sets_trait_range) END,

  -- IUCN flags: interpret yes/true/1 as true; no/false/0 as false; otherwise NULL (including Not_Yet_Assessed)
  CASE
    WHEN lower(trim(coalesce(k.iucn_red_list_extinct_ex,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(k.iucn_red_list_extinct_ex,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,
  CASE
    WHEN lower(trim(coalesce(k.iucn_red_list_extinct_in_the_wild_ew,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(k.iucn_red_list_extinct_in_the_wild_ew,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,
  CASE
    WHEN lower(trim(coalesce(k.iucn_red_list_critically_endangered_cr,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(k.iucn_red_list_critically_endangered_cr,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,
  CASE
    WHEN lower(trim(coalesce(k.iucn_red_list_endangered_en,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(k.iucn_red_list_endangered_en,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,
  CASE
    WHEN lower(trim(coalesce(k.iucn_red_list_vulnerable_vu,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(k.iucn_red_list_vulnerable_vu,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,
  CASE
    WHEN lower(trim(coalesce(k.iucn_red_list_least_concern,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(k.iucn_red_list_least_concern,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,
  CASE
    WHEN lower(trim(coalesce(k.iucn_red_list_data_deficient_dd,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(k.iucn_red_list_data_deficient_dd,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,
  CASE
    WHEN lower(trim(coalesce(k.iucn_red_list_not_evaluated_ne,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(k.iucn_red_list_not_evaluated_ne,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,

  -- Ecosystem flags
  CASE WHEN lower(trim(coalesce(k.ecosystem_endemic,''))) IN ('yes','true','1') THEN true
       WHEN lower(trim(coalesce(k.ecosystem_endemic,''))) IN ('no','false','0') THEN false
       ELSE NULL END,
  CASE WHEN lower(trim(coalesce(k.ecosystem_naturalized,''))) IN ('yes','true','1') THEN true
       WHEN lower(trim(coalesce(k.ecosystem_naturalized,''))) IN ('no','false','0') THEN false
       ELSE NULL END,
  CASE WHEN lower(trim(coalesce(k.ecosystem_invasive,''))) IN ('yes','true','1') THEN true
       WHEN lower(trim(coalesce(k.ecosystem_invasive,''))) IN ('no','false','0') THEN false
       ELSE NULL END,
  CASE WHEN lower(trim(coalesce(k.ecosystem_adventive,''))) IN ('yes','true','1') THEN true
       WHEN lower(trim(coalesce(k.ecosystem_adventive,''))) IN ('no','false','0') THEN false
       ELSE NULL END,
  CASE WHEN lower(trim(coalesce(k.ecosystem_extirpated,''))) IN ('yes','true','1') THEN true
       WHEN lower(trim(coalesce(k.ecosystem_extirpated,''))) IN ('no','false','0') THEN false
       ELSE NULL END,
  CASE WHEN lower(trim(coalesce(k.ecosystem_weed,''))) IN ('yes','true','1') THEN true
       WHEN lower(trim(coalesce(k.ecosystem_weed,''))) IN ('no','false','0') THEN false
       ELSE NULL END,
  CASE WHEN lower(trim(coalesce(k.ecosystem_cultivated_horticultural,''))) IN ('yes','true','1') THEN true
       WHEN lower(trim(coalesce(k.ecosystem_cultivated_horticultural,''))) IN ('no','false','0') THEN false
       ELSE NULL END,
  CASE WHEN lower(trim(coalesce(k.ecosystem_ruderal,''))) IN ('yes','true','1') THEN true
       WHEN lower(trim(coalesce(k.ecosystem_ruderal,''))) IN ('no','false','0') THEN false
       ELSE NULL END,
  CASE WHEN lower(trim(coalesce(k.ecosystem_pioneer,''))) IN ('yes','true','1') THEN true
       WHEN lower(trim(coalesce(k.ecosystem_pioneer,''))) IN ('no','false','0') THEN false
       ELSE NULL END,

  -- Commercial numerics (best-effort)
  CASE WHEN lower(trim(coalesce(k.commercial_bio_variables_harvestable_yield_per_cycle,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_bio_variables_harvestable_yield_per_cycle) END,
  CASE
    WHEN lower(trim(coalesce(k.commercial_bio_variables_harvestable_yield_per_cycle,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.commercial_bio_variables_harvestable_yield_per_cycle), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.commercial_bio_variables_harvest_season,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_bio_variables_harvest_season) END,
  CASE WHEN lower(trim(coalesce(k.commercial_bio_variables_light_needed,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_bio_variables_light_needed) END,
  CASE WHEN lower(trim(coalesce(k.commercial_production_spoilage_rate,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_production_spoilage_rate) END,
  CASE
    WHEN lower(trim(coalesce(k.commercial_production_spoilage_rate,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.commercial_production_spoilage_rate), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.commercial_production_operational_cost_per_cycle,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_production_operational_cost_per_cycle) END,
  CASE
    WHEN lower(trim(coalesce(k.commercial_production_operational_cost_per_cycle,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.commercial_production_operational_cost_per_cycle), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.commercial_production_gross_margin,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_production_gross_margin) END,
  CASE
    WHEN lower(trim(coalesce(k.commercial_production_gross_margin,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.commercial_production_gross_margin), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.commercial_market_price_volatility_index,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_market_price_volatility_index) END,
  CASE
    WHEN lower(trim(coalesce(k.commercial_market_price_volatility_index,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.commercial_market_price_volatility_index), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.commercial_market_demand_index_sector,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_market_demand_index_sector) END,
  CASE WHEN lower(trim(coalesce(k.commercial_market_market_growth_rate,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_market_market_growth_rate) END,
  CASE
    WHEN lower(trim(coalesce(k.commercial_market_market_growth_rate,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.commercial_market_market_growth_rate), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.commercial_processing_moisture_content,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_processing_moisture_content) END,
  CASE
    WHEN lower(trim(coalesce(k.commercial_processing_moisture_content,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.commercial_processing_moisture_content), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.commercial_processing_protein_content,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_processing_protein_content) END,
  CASE
    WHEN lower(trim(coalesce(k.commercial_processing_protein_content,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.commercial_processing_protein_content), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.commercial_processing_alginate_or_carrageenan_content,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_processing_alginate_or_carrageenan_content) END,
  CASE
    WHEN lower(trim(coalesce(k.commercial_processing_alginate_or_carrageenan_content,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.commercial_processing_alginate_or_carrageenan_content), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.commercial_processing_contaminants,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_processing_contaminants) END,
  CASE WHEN lower(trim(coalesce(k.commercial_processing_shelf_life,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_processing_shelf_life) END,
  CASE WHEN lower(trim(coalesce(k.commercial_processing_grade_quality_score,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_processing_grade_quality_score) END,
  CASE
    WHEN lower(trim(coalesce(k.commercial_processing_grade_quality_score,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.commercial_processing_grade_quality_score), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.commercial_supply_logistics_transport_cost,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_supply_logistics_transport_cost) END,
  CASE
    WHEN lower(trim(coalesce(k.commercial_supply_logistics_transport_cost,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.commercial_supply_logistics_transport_cost), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(k.commercial_supply_logistics_distribution_channel,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_supply_logistics_distribution_channel) END,
  CASE WHEN lower(trim(coalesce(k.commercial_supply_logistics_carbon_footprint_transport,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(k.commercial_supply_logistics_carbon_footprint_transport) END,
  CASE
    WHEN lower(trim(coalesce(k.commercial_supply_logistics_carbon_footprint_transport,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(k.commercial_supply_logistics_carbon_footprint_transport), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END
FROM staging.kelps_raw k;

CREATE INDEX IF NOT EXISTS idx_kelps_typed_batch ON staging.kelps_typed (ingest_batch_id);
CREATE INDEX IF NOT EXISTS idx_kelps_typed_storage_id ON staging.kelps_typed (storage_details_id);
CREATE INDEX IF NOT EXISTS idx_kelps_typed_taxon ON staging.kelps_typed (taxonomy_genus, taxonomy_species);

-- =========================
-- MICROBES_TYPED
-- =========================
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
  high_quality_malditof_data TEXT NULL, -- may be yes/no or quality label; keep text

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

INSERT INTO staging.microbes_typed (
  staging_id, ingest_batch_id, source_filename, source_row_num, loaded_at,
  microbe_id, original_code, institution_isolation_physically_conducted, isolated_year,
  isolated_by, maintained_by, maintained_at,
  kelp_host, kelp_ka_sample_id, source_if_ka_id, source_if_no_ka_id, kelp_location,
  kelp_collection_temp_raw, kelp_collection_temp_num, kelp_collection_month, kelp_collection_season,
  kelp_thallus_collection, kelp_collection_approach, kelp_collection_method,
  microbe_isolation_methods, microbe_isolation_protocol, isolation_media,
  location_stored1, location_1_temperature, location_stored2, location_2_temperature,
  cryopreservation_date_raw, cryopreservation_date, cryo_storage_medium, cryo_storage_preservative,
  cryo_revival_tested, cryo_backups_created, cryopreservation_protocol,
  malditof_procedure, malditof_dataanalysis_complete, high_quality_malditof_data,
  s16_pcr_completed, pcr_conducted_by, sanger_sequencing_completed, sequencing_date_raw, sequencing_date,
  primers_used, sequencing_notes, sequencing_conducted_by,
  total_bp_length_after_trimming, closest_ncbi_blast_tax_id, ncbi_blast_query_cover, percent_identity,
  accession, taxonomy_kingdom, s16_sequence, its2_sequence,
  pathogen_activity_kelp, pathogen_activity_humans, pathogen_activity_plants, pathogen_activity_animals,
  growth_temperature_c_range, growth_salinity_range, growth_ph_range, growth_optimal_media,
  morphology_colony_color, morphology_colony_size, morphology_colony_shape, morphology_colony_texture,
  gram_stain, morphology_cell_shape, probiotic_activity, probiotic_known_host
)
SELECT
  m.staging_id, m.ingest_batch_id, m.source_filename, m.source_row_num, m.loaded_at,

  CASE WHEN lower(trim(coalesce(m.microbe_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.microbe_id) END,
  CASE WHEN lower(trim(coalesce(m.original_code,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.original_code) END,
  CASE WHEN lower(trim(coalesce(m.institution_isolation_physically_conducted,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.institution_isolation_physically_conducted) END,

  CASE
    WHEN lower(trim(coalesce(m.isolated_year::text, trim(coalesce(m.isolated_year,''))))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    WHEN trim(m.isolated_year) ~ '^\d{4}$' THEN trim(m.isolated_year)::int
    ELSE NULL
  END,

  CASE WHEN lower(trim(coalesce(m.isolated_by,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.isolated_by) END,
  CASE WHEN lower(trim(coalesce(m.maintained_by,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.maintained_by) END,
  CASE WHEN lower(trim(coalesce(m.maintained_at,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.maintained_at) END,

  CASE WHEN lower(trim(coalesce(m.kelp_host,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.kelp_host) END,
  CASE WHEN lower(trim(coalesce(m.kelp_ka_sample_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.kelp_ka_sample_id) END,
  CASE WHEN lower(trim(coalesce(m.source_if_ka_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.source_if_ka_id) END,
  CASE WHEN lower(trim(coalesce(m.source_if_no_ka_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.source_if_no_ka_id) END,
  CASE WHEN lower(trim(coalesce(m.kelp_location,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.kelp_location) END,

  CASE WHEN lower(trim(coalesce(m.kelp_collection_temp,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.kelp_collection_temp) END,
  CASE
    WHEN lower(trim(coalesce(m.kelp_collection_temp,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(m.kelp_collection_temp), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(m.kelp_collection_month,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.kelp_collection_month) END,
  CASE WHEN lower(trim(coalesce(m.kelp_collection_season,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.kelp_collection_season) END,

  CASE WHEN lower(trim(coalesce(m.kelp_thallus_collection,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.kelp_thallus_collection) END,
  CASE WHEN lower(trim(coalesce(m.kelp_collection_approach,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.kelp_collection_approach) END,
  CASE WHEN lower(trim(coalesce(m.kelp_collection_method,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.kelp_collection_method) END,

  CASE WHEN lower(trim(coalesce(m.microbe_isolation_methods,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.microbe_isolation_methods) END,
  CASE WHEN lower(trim(coalesce(m.microbe_isolation_protocol,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.microbe_isolation_protocol) END,
  CASE WHEN lower(trim(coalesce(m.isolation_media,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.isolation_media) END,

  CASE WHEN lower(trim(coalesce(m.location_stored1,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.location_stored1) END,
  CASE WHEN lower(trim(coalesce(m.location_1_temperature,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.location_1_temperature) END,
  CASE WHEN lower(trim(coalesce(m.location_stored2,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.location_stored2) END,
  CASE WHEN lower(trim(coalesce(m.location_2_temperature,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.location_2_temperature) END,

  CASE WHEN lower(trim(coalesce(m.cryopreservation_date,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.cryopreservation_date) END,
  CASE
    WHEN lower(trim(coalesce(m.cryopreservation_date,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    WHEN trim(m.cryopreservation_date) ~ '^\d{1,2}-[A-Za-z]{3}-\d{2,4}$'
      THEN to_date(trim(m.cryopreservation_date), CASE WHEN length(trim(m.cryopreservation_date)) = 9 THEN 'DD-Mon-YY' ELSE 'DD-Mon-YYYY' END)
    WHEN trim(m.cryopreservation_date) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
      THEN to_date(trim(m.cryopreservation_date), 'MM/DD/YYYY')
    ELSE NULL
  END,
  CASE WHEN lower(trim(coalesce(m.cryo_storage_medium,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.cryo_storage_medium) END,
  CASE WHEN lower(trim(coalesce(m.cryo_storage_preservative,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.cryo_storage_preservative) END,

  CASE
    WHEN lower(trim(coalesce(m.cryo_revival_tested,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(m.cryo_revival_tested,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,
  CASE
    WHEN lower(trim(coalesce(m.cryo_backups_created,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(m.cryo_backups_created,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,
  CASE WHEN lower(trim(coalesce(m.cryopreservation_protocol,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.cryopreservation_protocol) END,

  CASE
    WHEN lower(trim(coalesce(m.malditof_procedure,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(m.malditof_procedure,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,
  CASE
    WHEN lower(trim(coalesce(m.malditof_dataanalysis_complete,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(m.malditof_dataanalysis_complete,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,
  CASE WHEN lower(trim(coalesce(m.high_quality_malditof_data,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.high_quality_malditof_data) END,

  CASE
    WHEN lower(trim(coalesce(m.s16_pcr_completed,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(m.s16_pcr_completed,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,
  CASE WHEN lower(trim(coalesce(m.pcr_conducted_by,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.pcr_conducted_by) END,
  CASE
    WHEN lower(trim(coalesce(m.sanger_sequencing_completed,''))) IN ('yes','true','1') THEN true
    WHEN lower(trim(coalesce(m.sanger_sequencing_completed,''))) IN ('no','false','0') THEN false
    ELSE NULL
  END,
  CASE WHEN lower(trim(coalesce(m.sequencing_date,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.sequencing_date) END,
  CASE
    WHEN lower(trim(coalesce(m.sequencing_date,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    WHEN trim(m.sequencing_date) ~ '^\d{1,2}-[A-Za-z]{3}-\d{2,4}$'
      THEN to_date(trim(m.sequencing_date), CASE WHEN length(trim(m.sequencing_date)) = 9 THEN 'DD-Mon-YY' ELSE 'DD-Mon-YYYY' END)
    WHEN trim(m.sequencing_date) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
      THEN to_date(trim(m.sequencing_date), 'MM/DD/YYYY')
    ELSE NULL
  END,
  CASE WHEN lower(trim(coalesce(m.primers_used,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.primers_used) END,
  CASE WHEN lower(trim(coalesce(m.sequencing_notes,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.sequencing_notes) END,
  CASE WHEN lower(trim(coalesce(m.sequencing_conducted_by,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.sequencing_conducted_by) END,

  CASE
    WHEN lower(trim(coalesce(m.total_bp_length_after_trimming,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    WHEN trim(m.total_bp_length_after_trimming) ~ '^\d+$' THEN trim(m.total_bp_length_after_trimming)::int
    ELSE NULL
  END,
  CASE WHEN lower(trim(coalesce(m.closest_ncbi_blast_tax_id,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.closest_ncbi_blast_tax_id) END,
  CASE
    WHEN lower(trim(coalesce(m.ncbi_blast_query_cover,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(m.ncbi_blast_query_cover), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE
    WHEN lower(trim(coalesce(m.percent_identity,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL
    ELSE NULLIF(regexp_replace(trim(m.percent_identity), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END,
  CASE WHEN lower(trim(coalesce(m.accession,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.accession) END,
  CASE WHEN lower(trim(coalesce(m.taxonomy_kingdom,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.taxonomy_kingdom) END,

  CASE WHEN lower(trim(coalesce(m.s16_sequence,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.s16_sequence) END,
  CASE WHEN lower(trim(coalesce(m.its2_sequence,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.its2_sequence) END,

  CASE WHEN lower(trim(coalesce(m.pathogen_activity_kelp,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.pathogen_activity_kelp) END,
  CASE WHEN lower(trim(coalesce(m.pathogen_activity_humans,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.pathogen_activity_humans) END,
  CASE WHEN lower(trim(coalesce(m.pathogen_activity_plants,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.pathogen_activity_plants) END,
  CASE WHEN lower(trim(coalesce(m.pathogen_activity_animals,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.pathogen_activity_animals) END,

  CASE WHEN lower(trim(coalesce(m.growth_temperature_c_range,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.growth_temperature_c_range) END,
  CASE WHEN lower(trim(coalesce(m.growth_salinity_range,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.growth_salinity_range) END,
  CASE WHEN lower(trim(coalesce(m.growth_ph_range,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.growth_ph_range) END,
  CASE WHEN lower(trim(coalesce(m.growth_optimal_media,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.growth_optimal_media) END,

  CASE WHEN lower(trim(coalesce(m.morphology_colony_color,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.morphology_colony_color) END,
  CASE WHEN lower(trim(coalesce(m.morphology_colony_size,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.morphology_colony_size) END,
  CASE WHEN lower(trim(coalesce(m.morphology_colony_shape,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.morphology_colony_shape) END,
  CASE WHEN lower(trim(coalesce(m.morphology_colony_texture,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.morphology_colony_texture) END,
  CASE WHEN lower(trim(coalesce(m.gram_stain,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.gram_stain) END,
  CASE WHEN lower(trim(coalesce(m.morphology_cell_shape,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.morphology_cell_shape) END,

  CASE WHEN lower(trim(coalesce(m.probiotic_activity,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.probiotic_activity) END,
  CASE WHEN lower(trim(coalesce(m.probiotic_known_host,''))) IN ('', 'na','n/a','none','null','not_yet_assessed','not yet assessed','tbd','to_be_analyzed') THEN NULL ELSE trim(m.probiotic_known_host) END
FROM staging.microbes_raw m;

CREATE INDEX IF NOT EXISTS idx_microbes_typed_batch ON staging.microbes_typed (ingest_batch_id);
CREATE INDEX IF NOT EXISTS idx_microbes_typed_microbe_id ON staging.microbes_typed (microbe_id);
CREATE INDEX IF NOT EXISTS idx_microbes_typed_kelp_sample ON staging.microbes_typed (kelp_ka_sample_id);

-- =========================
-- Quick sanity checks
-- =========================
-- You can run these after COMMIT if you want:
-- SELECT count(*) FROM staging.kelps_typed;
-- SELECT count(*) FROM staging.microbes_typed;

COMMIT;
```
