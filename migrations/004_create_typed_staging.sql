-- 004_create_typed_staging.sql
-- Purpose:
--   Create typed staging tables from staging.*_raw, normalizing placeholders
--   (NA, None, Not_Yet_Assessed, TBD, empty) to NULL, and casting where safe.
--
-- Assumptions:
--   - Raw tables exist: staging.kelps_raw, staging.microbes_raw
--   - Extension pgcrypto exists (for gen_random_uuid elsewhere)
--
-- Notes:
--   - We DO NOT drop raw tables.
--   - We cast conservatively (dates, numerics, booleans). Anything ambiguous stays TEXT.
--   - If your raw table has any auto-truncated column names (63 char limit), you MUST
--     edit the few long raw-column references in the kelps SELECT section to match
--     your actual staging.kelps_raw column names.

BEGIN;

CREATE SCHEMA IF NOT EXISTS staging;

-- ============================================================
-- Helper functions (small + deterministic + reusable in SELECT)
-- ============================================================
-- Normalizes common placeholders to NULL and trims whitespace.
CREATE OR REPLACE FUNCTION staging.nullify_placeholder(p_text TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN p_text IS NULL THEN NULL
    WHEN lower(btrim(p_text)) IN (
      '', 'na','n/a','none','null',
      'not_yet_assessed','not yet assessed',
      'tbd','to_be_analyzed','to be analyzed'
    ) THEN NULL
    ELSE btrim(p_text)
  END;
$$;

-- Extract numeric best-effort (keeps digits, dot, minus). Returns NULL if nothing parseable.
CREATE OR REPLACE FUNCTION staging.parse_numeric(p_text TEXT)
RETURNS NUMERIC
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN staging.nullify_placeholder(p_text) IS NULL THEN NULL
    ELSE NULLIF(regexp_replace(staging.nullify_placeholder(p_text), '[^0-9\.\-]+', '', 'g'), '')::numeric
  END;
$$;

-- Parse integer (best-effort). Returns NULL if not clean integer.
CREATE OR REPLACE FUNCTION staging.parse_int(p_text TEXT)
RETURNS INTEGER
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN staging.nullify_placeholder(p_text) IS NULL THEN NULL
    WHEN staging.nullify_placeholder(p_text) ~ '^\-?\d+$' THEN staging.nullify_placeholder(p_text)::int
    ELSE NULL
  END;
$$;

-- Parse boolean from yes/true/1 and no/false/0. Returns NULL otherwise.
CREATE OR REPLACE FUNCTION staging.parse_bool(p_text TEXT)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN staging.nullify_placeholder(p_text) IS NULL THEN NULL
    WHEN lower(staging.nullify_placeholder(p_text)) IN ('yes','true','1') THEN true
    WHEN lower(staging.nullify_placeholder(p_text)) IN ('no','false','0') THEN false
    ELSE NULL
  END;
$$;

-- Parse DATE from common formats:
--   - DD-Mon-YY / DD-Mon-YYYY  (e.g., 05-Jan-24 / 05-Jan-2024)
--   - MM/DD/YYYY              (e.g., 1/5/2024)
CREATE OR REPLACE FUNCTION staging.parse_date(p_text TEXT)
RETURNS DATE
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v TEXT;
BEGIN
  v := staging.nullify_placeholder(p_text);
  IF v IS NULL THEN
    RETURN NULL;
  END IF;

  IF v ~ '^\d{1,2}-[A-Za-z]{3}-\d{2,4}$' THEN
    IF length(v) = 9 THEN
      RETURN to_date(v, 'DD-Mon-YY');
    ELSE
      RETURN to_date(v, 'DD-Mon-YYYY');
    END IF;
  ELSIF v ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
    RETURN to_date(v, 'MM/DD/YYYY');
  ELSE
    RETURN NULL;
  END IF;
END;
$$;

-- Parse decimal degrees latitude/longitude ONLY if already decimal format (DMS stays NULL)
CREATE OR REPLACE FUNCTION staging.parse_decimal_degrees(p_text TEXT)
RETURNS NUMERIC
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN staging.nullify_placeholder(p_text) IS NULL THEN NULL
    WHEN staging.nullify_placeholder(p_text) ~ '^\s*-?\d+(\.\d+)?\s*$'
      THEN staging.nullify_placeholder(p_text)::numeric
    ELSE NULL
  END;
$$;

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

INSERT INTO staging.kelps_typed (
  staging_id, ingest_batch_id, source_filename, source_row_num, loaded_at,
  taxonomy_genus, taxonomy_species, taxonomy_sex, taxonomy_variety_or_form,
  storage_details_id, storage_details_position_id, storage_details_rack_id, storage_details_location,
  storage_details_temperature_c_raw, storage_details_temperature_c_num, storage_details_medium,
  sampling_metadata_country, sampling_metadata_latitude_raw, sampling_metadata_longitude_raw,
  sampling_metadata_latitude_num, sampling_metadata_longitude_num,
  sampling_metadata_collection_date_raw, sampling_metadata_collection_date,
  sampling_metadata_personnel_collected,
  sampling_metadata_isolation_date_raw, sampling_metadata_isolation_date,
  sampling_metadata_deposit_date_raw, sampling_metadata_deposit_date,
  sampling_metadata_deposited_by, sampling_metadata_permit, sampling_metadata_collection_site,
  other_previously_housed_location, sponsorship_strain_sponsorship_status, sponsorship_code,
  phenotypic_data_growth_rate_raw, phenotypic_data_growth_rate_num,
  phenotypic_data_optimal_growth_conditions,
  phenotypic_data_percent_viability_raw, phenotypic_data_percent_viability_num,
  phenotypic_data_lifespan,
  phenotypic_data_tolerance_to_thermal_stressor,
  phenotypic_data_tolerance_to_water_quality_stressors,
  inaturalist,
  ecological_role_primary_producer, ecological_role_carbon_sink, ecological_role_habitat_former,
  metabolic_pathways_kegg_pathway_id, metabolic_pathways_metacyc_pathway_id,
  functional_annotation_gene_function_id, functional_annotation_protein_function_id,
  genetic_variation_data_variant_id, genetic_variation_data_gene_id, genetic_variation_data_chromosome,
  genetic_variation_data_reference_allele, genetic_variation_data_alternate_allele,
  genetic_variation_data_variant_type,
  genetic_variation_data_allele_frequency_raw, genetic_variation_data_allele_frequency_num,
  genetic_variation_data_read_depth_raw, genetic_variation_data_read_depth_num,
  genetic_variation_data_quality_score_raw, genetic_variation_data_quality_score_num,
  genetic_variation_data_genotype,
  genetic_diversity_fst_raw, genetic_diversity_fst_num,
  genetic_diversity_observed_heterozygosity_raw, genetic_diversity_observed_heterozygosity_num,
  genetic_diversity_observed_homozygosity_raw, genetic_diversity_observed_homozygosity_num,
  genetic_diversity_allele_count_raw, genetic_diversity_allele_count_num,
  genetic_diversity_nucleotide_diversity_raw, genetic_diversity_nucleotide_diversity_num,
  phenotypic_diversity_trait_id_name,
  phenotypic_diversity_trait_variance_raw, phenotypic_diversity_trait_variance_num,
  phenotypic_diversity_trait_mean_raw, phenotypic_diversity_trait_mean_num,
  phenotypic_diversity_trait_standard_deviation_raw, phenotypic_diversity_trait_standard_deviation_num,
  phenotypic_diversity_trait_range,
  iucn_red_list_extinct_ex, iucn_red_list_extinct_in_the_wild_ew,
  iucn_red_list_critically_endangered_cr, iucn_red_list_endangered_en,
  iucn_red_list_vulnerable_vu, iucn_red_list_least_concern,
  iucn_red_list_data_deficient_dd, iucn_red_list_not_evaluated_ne,
  ecosystem_endemic, ecosystem_naturalized, ecosystem_invasive, ecosystem_adventive,
  ecosystem_extirpated, ecosystem_weed, ecosystem_cultivated_horticultural,
  ecosystem_ruderal, ecosystem_pioneer,
  commercial_bio_variables_harvestable_yield_per_cycle_raw,
  commercial_bio_variables_harvestable_yield_per_cycle_num,
  commercial_bio_variables_harvest_season, commercial_bio_variables_light_needed,
  commercial_production_spoilage_rate_raw, commercial_production_spoilage_rate_num,
  commercial_production_operational_cost_per_cycle_raw, commercial_production_operational_cost_per_cycle_num,
  commercial_production_gross_margin_raw, commercial_production_gross_margin_num,
  commercial_market_price_volatility_index_raw, commercial_market_price_volatility_index_num,
  commercial_market_demand_index_sector,
  commercial_market_market_growth_rate_raw, commercial_market_market_growth_rate_num,
  commercial_processing_moisture_content_raw, commercial_processing_moisture_content_num,
  commercial_processing_protein_content_raw, commercial_processing_protein_content_num,
  commercial_processing_alginate_or_carrageenan_content_raw, commercial_processing_alginate_or_carrageenan_content_num,
  commercial_processing_contaminants, commercial_processing_shelf_life,
  commercial_processing_grade_quality_score_raw, commercial_processing_grade_quality_score_num,
  commercial_supply_logistics_transport_cost_raw, commercial_supply_logistics_transport_cost_num,
  commercial_supply_logistics_distribution_channel,
  commercial_supply_logistics_carbon_footprint_transport_raw, commercial_supply_logistics_carbon_footprint_transport_num
)
SELECT
  k.staging_id,
  k.ingest_batch_id,
  k.source_filename,
  k.source_row_num,
  k.loaded_at,

  staging.nullify_placeholder(k.taxonomy_genus),
  staging.nullify_placeholder(k.taxonomy_species),
  staging.nullify_placeholder(k.taxonomy_sex),
  staging.nullify_placeholder(k.taxonomy_variety_or_form),

  staging.nullify_placeholder(k.storage_details_id),
  staging.nullify_placeholder(k.storage_details_position_id),
  staging.nullify_placeholder(k.storage_details_rack_id),
  staging.nullify_placeholder(k.storage_details_location),
  staging.nullify_placeholder(k.storage_details_temperature_c)                               AS storage_details_temperature_c_raw,
  staging.parse_numeric(k.storage_details_temperature_c)                                     AS storage_details_temperature_c_num,
  staging.nullify_placeholder(k.storage_details_medium),

  staging.nullify_placeholder(k.sampling_metadata_country),
  staging.nullify_placeholder(k.sampling_metadata_latitude)                                  AS sampling_metadata_latitude_raw,
  staging.nullify_placeholder(k.sampling_metadata_longitude)                                 AS sampling_metadata_longitude_raw,
  staging.parse_decimal_degrees(k.sampling_metadata_latitude)                                AS sampling_metadata_latitude_num,
  staging.parse_decimal_degrees(k.sampling_metadata_longitude)                               AS sampling_metadata_longitude_num,
  staging.nullify_placeholder(k.sampling_metadata_collection_date)                           AS sampling_metadata_collection_date_raw,
  staging.parse_date(k.sampling_metadata_collection_date)                                    AS sampling_metadata_collection_date,
  staging.nullify_placeholder(k.sampling_metadata_personnel_collected),
  staging.nullify_placeholder(k.sampling_metadata_isolation_date)                            AS sampling_metadata_isolation_date_raw,
  staging.parse_date(k.sampling_metadata_isolation_date)                                     AS sampling_metadata_isolation_date,
  staging.nullify_placeholder(k.sampling_metadata_deposit_date)                              AS sampling_metadata_deposit_date_raw,
  staging.parse_date(k.sampling_metadata_deposit_date)                                       AS sampling_metadata_deposit_date,
  staging.nullify_placeholder(k.sampling_metadata_deposited_by),
  staging.nullify_placeholder(k.sampling_metadata_permit),
  staging.nullify_placeholder(k.sampling_metadata_collection_site),

  staging.nullify_placeholder(k.other_previously_housed_location),
  staging.nullify_placeholder(k.sponsorship_strain_sponsorship_status),
  staging.nullify_placeholder(k.sponsorship_code),

  staging.nullify_placeholder(k.phenotypic_data_growth_rate)                                 AS phenotypic_data_growth_rate_raw,
  staging.parse_numeric(k.phenotypic_data_growth_rate)                                       AS phenotypic_data_growth_rate_num,
  staging.nullify_placeholder(k.phenotypic_data_optimal_growth_conditions),
  staging.nullify_placeholder(k.phenotypic_data_percent_viability)                           AS phenotypic_data_percent_viability_raw,
  staging.parse_numeric(k.phenotypic_data_percent_viability)                                 AS phenotypic_data_percent_viability_num,
  staging.nullify_placeholder(k.phenotypic_data_lifespan),
  staging.nullify_placeholder(k.phenotypic_data_tolerance_to_thermal_stressor),
  staging.nullify_placeholder(k.phenotypic_data_tolerance_to_water_quality_stressors),

  staging.nullify_placeholder(k.inaturalist),

  staging.nullify_placeholder(k.ecological_role_primary_producer),
  staging.nullify_placeholder(k.ecological_role_carbon_sink),
  staging.nullify_placeholder(k.ecological_role_habitat_former),

  staging.nullify_placeholder(k.metabolic_pathways_kegg_pathway_id),
  staging.nullify_placeholder(k.metabolic_pathways_metacyc_pathway_id),
  staging.nullify_placeholder(k.functional_annotation_gene_function_id),
  staging.nullify_placeholder(k.functional_annotation_protein_function_id),

  staging.nullify_placeholder(k.genetic_variation_data_variant_id),
  staging.nullify_placeholder(k.genetic_variation_data_gene_id),
  staging.nullify_placeholder(k.genetic_variation_data_chromosome),
  staging.nullify_placeholder(k.genetic_variation_data_reference_allele),
  staging.nullify_placeholder(k.genetic_variation_data_alternate_allele),
  staging.nullify_placeholder(k.genetic_variation_data_variant_type),
  staging.nullify_placeholder(k.genetic_variation_data_allele_frequency)                      AS genetic_variation_data_allele_frequency_raw,
  staging.parse_numeric(k.genetic_variation_data_allele_frequency)                            AS genetic_variation_data_allele_frequency_num,
  staging.nullify_placeholder(k.genetic_variation_data_read_depth)                            AS genetic_variation_data_read_depth_raw,
  staging.parse_numeric(k.genetic_variation_data_read_depth)                                  AS genetic_variation_data_read_depth_num,
  staging.nullify_placeholder(k.genetic_variation_data_quality_score)                         AS genetic_variation_data_quality_score_raw,
  staging.parse_numeric(k.genetic_variation_data_quality_score)                               AS genetic_variation_data_quality_score_num,
  staging.nullify_placeholder(k.genetic_variation_data_genotype),

  -- =========================
  -- Diversity raw column names
  -- =========================
  -- IMPORTANT:
  -- The next 4 groups are the ones most likely to differ in YOUR staging.kelps_raw
  -- if Postgres truncated column names when you created the raw table.
  -- If you get "column does not exist", edit these references to match \d staging.kelps_raw.

  staging.nullify_placeholder(k.genetic_diversity_within_geography_sample_sets_fst)           AS genetic_diversity_fst_raw,
  staging.parse_numeric(k.genetic_diversity_within_geography_sample_sets_fst)                 AS genetic_diversity_fst_num,

  staging.nullify_placeholder(k.genetic_diversity_within_geography_sample_sets_observed_heteroz)
                                                                                              AS genetic_diversity_observed_heterozygosity_raw,
  staging.parse_numeric(k.genetic_diversity_within_geography_sample_sets_observed_heteroz)
                                                                                              AS genetic_diversity_observed_heterozygosity_num,

  staging.nullify_placeholder(k.genetic_diversity_within_geography_sample_sets_observed_homozyg)
                                                                                              AS genetic_diversity_observed_homozygosity_raw,
  staging.parse_numeric(k.genetic_diversity_within_geography_sample_sets_observed_homozyg)
                                                                                              AS genetic_diversity_observed_homozygosity_num,

  staging.nullify_placeholder(k.genetic_diversity_within_geography_sample_sets_allele_count)  AS genetic_diversity_allele_count_raw,
  staging.parse_numeric(k.genetic_diversity_within_geography_sample_sets_allele_count)        AS genetic_diversity_allele_count_num,

  staging.nullify_placeholder(k.genetic_diversity_within_geography_sample_sets_nucleotide_diver)
                                                                                              AS genetic_diversity_nucleotide_diversity_raw,
  staging.parse_numeric(k.genetic_diversity_within_geography_sample_sets_nucleotide_diver)
                                                                                              AS genetic_diversity_nucleotide_diversity_num,

  staging.nullify_placeholder(k.phenotypic_diversity_within_geography_sample_sets_trait_id_name)
                                                                                              AS phenotypic_diversity_trait_id_name,
  staging.nullify_placeholder(k.phenotypic_diversity_within_geography_sample_sets_trait_varianc)
                                                                                              AS phenotypic_diversity_trait_variance_raw,
  staging.parse_numeric(k.phenotypic_diversity_within_geography_sample_sets_trait_varianc)
                                                                                              AS phenotypic_diversity_trait_variance_num,
  staging.nullify_placeholder(k.phenotypic_diversity_within_geography_sample_sets_trait_mean)  AS phenotypic_diversity_trait_mean_raw,
  staging.parse_numeric(k.phenotypic_diversity_within_geography_sample_sets_trait_mean)        AS phenotypic_diversity_trait_mean_num,
  staging.nullify_placeholder(k.phenotypic_diversity_within_geography_sample_sets_trait_standar)
                                                                                              AS phenotypic_diversity_trait_standard_deviation_raw,
  staging.parse_numeric(k.phenotypic_diversity_within_geography_sample_sets_trait_standar)
                                                                                              AS phenotypic_diversity_trait_standard_deviation_num,
  staging.nullify_placeholder(k.phenotypic_diversity_within_geography_sample_sets_trait_range) AS phenotypic_diversity_trait_range,

  staging.parse_bool(k.iucn_red_list_extinct_ex),
  staging.parse_bool(k.iucn_red_list_extinct_in_the_wild_ew),
  staging.parse_bool(k.iucn_red_list_critically_endangered_cr),
  staging.parse_bool(k.iucn_red_list_endangered_en),
  staging.parse_bool(k.iucn_red_list_vulnerable_vu),
  staging.parse_bool(k.iucn_red_list_least_concern),
  staging.parse_bool(k.iucn_red_list_data_deficient_dd),
  staging.parse_bool(k.iucn_red_list_not_evaluated_ne),

  staging.parse_bool(k.ecosystem_endemic),
  staging.parse_bool(k.ecosystem_naturalized),
  staging.parse_bool(k.ecosystem_invasive),
  staging.parse_bool(k.ecosystem_adventive),
  staging.parse_bool(k.ecosystem_extirpated),
  staging.parse_bool(k.ecosystem_weed),
  staging.parse_bool(k.ecosystem_cultivated_horticultural),
  staging.parse_bool(k.ecosystem_ruderal),
  staging.parse_bool(k.ecosystem_pioneer),

  staging.nullify_placeholder(k.commercial_bio_variables_harvestable_yield_per_cycle)         AS commercial_bio_variables_harvestable_yield_per_cycle_raw,
  staging.parse_numeric(k.commercial_bio_variables_harvestable_yield_per_cycle)               AS commercial_bio_variables_harvestable_yield_per_cycle_num,
  staging.nullify_placeholder(k.commercial_bio_variables_harvest_season),
  staging.nullify_placeholder(k.commercial_bio_variables_light_needed),

  staging.nullify_placeholder(k.commercial_production_spoilage_rate)                          AS commercial_production_spoilage_rate_raw,
  staging.parse_numeric(k.commercial_production_spoilage_rate)                                AS commercial_production_spoilage_rate_num,
  staging.nullify_placeholder(k.commercial_production_operational_cost_per_cycle)             AS commercial_production_operational_cost_per_cycle_raw,
  staging.parse_numeric(k.commercial_production_operational_cost_per_cycle)                   AS commercial_production_operational_cost_per_cycle_num,
  staging.nullify_placeholder(k.commercial_production_gross_margin)                           AS commercial_production_gross_margin_raw,
  staging.parse_numeric(k.commercial_production_gross_margin)                                 AS commercial_production_gross_margin_num,

  staging.nullify_placeholder(k.commercial_market_price_volatility_index)                     AS commercial_market_price_volatility_index_raw,
  staging.parse_numeric(k.commercial_market_price_volatility_index)                           AS commercial_market_price_volatility_index_num,
  staging.nullify_placeholder(k.commercial_market_demand_index_sector),
  staging.nullify_placeholder(k.commercial_market_market_growth_rate)                         AS commercial_market_market_growth_rate_raw,
  staging.parse_numeric(k.commercial_market_market_growth_rate)                               AS commercial_market_market_growth_rate_num,

  staging.nullify_placeholder(k.commercial_processing_moisture_content)                       AS commercial_processing_moisture_content_raw,
  staging.parse_numeric(k.commercial_processing_moisture_content)                             AS commercial_processing_moisture_content_num,
  staging.nullify_placeholder(k.commercial_processing_protein_content)                        AS commercial_processing_protein_content_raw,
  staging.parse_numeric(k.commercial_processing_protein_content)                              AS commercial_processing_protein_content_num,
  staging.nullify_placeholder(k.commercial_processing_alginate_or_carrageenan_content)        AS commercial_processing_alginate_or_carrageenan_content_raw,
  staging.parse_numeric(k.commercial_processing_alginate_or_carrageenan_content)              AS commercial_processing_alginate_or_carrageenan_content_num,

  staging.nullify_placeholder(k.commercial_processing_contaminants),
  staging.nullify_placeholder(k.commercial_processing_shelf_life),

  staging.nullify_placeholder(k.commercial_processing_grade_quality_score)                    AS commercial_processing_grade_quality_score_raw,
  staging.parse_numeric(k.commercial_processing_grade_quality_score)                          AS commercial_processing_grade_quality_score_num,

  staging.nullify_placeholder(k.commercial_supply_logistics_transport_cost)                   AS commercial_supply_logistics_transport_cost_raw,
  staging.parse_numeric(k.commercial_supply_logistics_transport_cost)                         AS commercial_supply_logistics_transport_cost_num,
  staging.nullify_placeholder(k.commercial_supply_logistics_distribution_channel),

  staging.nullify_placeholder(k.commercial_supply_logistics_carbon_footprint_transport)       AS commercial_supply_logistics_carbon_footprint_transport_raw,
  staging.parse_numeric(k.commercial_supply_logistics_carbon_footprint_transport)             AS commercial_supply_logistics_carbon_footprint_transport_num
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

  staging.nullify_placeholder(m.microbe_id),
  staging.nullify_placeholder(m.original_code),
  staging.nullify_placeholder(m.institution_isolation_physically_conducted),

  -- FIXED: isolated_year parsing (your previous version mixed casts and would error)
  staging.parse_int(m.isolated_year),

  staging.nullify_placeholder(m.isolated_by),
  staging.nullify_placeholder(m.maintained_by),
  staging.nullify_placeholder(m.maintained_at),

  staging.nullify_placeholder(m.kelp_host),
  staging.nullify_placeholder(m.kelp_ka_sample_id),
  staging.nullify_placeholder(m.source_if_ka_id),
  staging.nullify_placeholder(m.source_if_no_ka_id),
  staging.nullify_placeholder(m.kelp_location),

  staging.nullify_placeholder(m.kelp_collection_temp) AS kelp_collection_temp_raw,
  staging.parse_numeric(m.kelp_collection_temp)       AS kelp_collection_temp_num,
  staging.nullify_placeholder(m.kelp_collection_month),
  staging.nullify_placeholder(m.kelp_collection_season),

  staging.nullify_placeholder(m.kelp_thallus_collection),
  staging.nullify_placeholder(m.kelp_collection_approach),
  staging.nullify_placeholder(m.kelp_collection_method),

  staging.nullify_placeholder(m.microbe_isolation_methods),
  staging.nullify_placeholder(m.microbe_isolation_protocol),
  staging.nullify_placeholder(m.isolation_media),

  staging.nullify_placeholder(m.location_stored1),
  staging.nullify_placeholder(m.location_1_temperature),
  staging.nullify_placeholder(m.location_stored2),
  staging.nullify_placeholder(m.location_2_temperature),

  staging.nullify_placeholder(m.cryopreservation_date) AS cryopreservation_date_raw,
  staging.parse_date(m.cryopreservation_date)          AS cryopreservation_date,
  staging.nullify_placeholder(m.cryo_storage_medium),
  staging.nullify_placeholder(m.cryo_storage_preservative),
  staging.parse_bool(m.cryo_revival_tested),
  staging.parse_bool(m.cryo_backups_created),
  staging.nullify_placeholder(m.cryopreservation_protocol),

  staging.parse_bool(m.malditof_procedure),
  staging.parse_bool(m.malditof_dataanalysis_complete),
  staging.nullify_placeholder(m.high_quality_malditof_data),

  staging.parse_bool(m.s16_pcr_completed),
  staging.nullify_placeholder(m.pcr_conducted_by),
  staging.parse_bool(m.sanger_sequencing_completed),
  staging.nullify_placeholder(m.sequencing_date) AS sequencing_date_raw,
  staging.parse_date(m.sequencing_date)          AS sequencing_date,
  staging.nullify_placeholder(m.primers_used),
  staging.nullify_placeholder(m.sequencing_notes),
  staging.nullify_placeholder(m.sequencing_conducted_by),

  staging.parse_int(m.total_bp_length_after_trimming),

  staging.nullify_placeholder(m.closest_ncbi_blast_tax_id),
  staging.parse_numeric(m.ncbi_blast_query_cover),
  staging.parse_numeric(m.percent_identity),
  staging.nullify_placeholder(m.accession),
  staging.nullify_placeholder(m.taxonomy_kingdom),

  staging.nullify_placeholder(m.s16_sequence),
  staging.nullify_placeholder(m.its2_sequence),

  staging.nullify_placeholder(m.pathogen_activity_kelp),
  staging.nullify_placeholder(m.pathogen_activity_humans),
  staging.nullify_placeholder(m.pathogen_activity_plants),
  staging.nullify_placeholder(m.pathogen_activity_animals),

  staging.nullify_placeholder(m.growth_temperature_c_range),
  staging.nullify_placeholder(m.growth_salinity_range),
  staging.nullify_placeholder(m.growth_ph_range),
  staging.nullify_placeholder(m.growth_optimal_media),

  staging.nullify_placeholder(m.morphology_colony_color),
  staging.nullify_placeholder(m.morphology_colony_size),
  staging.nullify_placeholder(m.morphology_colony_shape),
  staging.nullify_placeholder(m.morphology_colony_texture),
  staging.nullify_placeholder(m.gram_stain),
  staging.nullify_placeholder(m.morphology_cell_shape),

  staging.nullify_placeholder(m.probiotic_activity),
  staging.nullify_placeholder(m.probiotic_known_host)
FROM staging.microbes_raw m;

CREATE INDEX IF NOT EXISTS idx_microbes_typed_batch ON staging.microbes_typed (ingest_batch_id);
CREATE INDEX IF NOT EXISTS idx_microbes_typed_microbe_id ON staging.microbes_typed (microbe_id);
CREATE INDEX IF NOT EXISTS idx_microbes_typed_kelp_sample ON staging.microbes_typed (kelp_ka_sample_id);

-- =========================
-- Quick sanity checks
-- =========================
-- SELECT count(*) FROM staging.kelps_typed;
-- SELECT count(*) FROM staging.microbes_typed;

COMMIT;
