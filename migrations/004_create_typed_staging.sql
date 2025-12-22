BEGIN;

-- ============================================================
-- 004_staging_typed.sql
-- Create typed + normalized staging tables for:
--   - staging.kelps_raw    -> staging.kelps_typed
--   - staging.microbes_raw -> staging.microbes_typed
--
-- Design goals:
--   1) Never lose raw values
--   2) Normalize obvious placeholders to NULL
--   3) Parse key numeric/date fields defensively
-- ============================================================

-- For UUIDs if you want deterministic keys later (not required here)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ----------------------------
-- 1) Normalization helpers
-- ----------------------------

CREATE OR REPLACE FUNCTION staging.norm_text(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT
    CASE
      WHEN p_text IS NULL THEN NULL
      WHEN btrim(p_text) = '' THEN NULL
      WHEN lower(btrim(p_text)) IN ('na','n/a','none','null','unknown') THEN NULL
      WHEN lower(btrim(p_text)) IN ('legacy') THEN NULL
      WHEN lower(btrim(p_text)) IN ('will_be_added','will be added') THEN NULL
      WHEN lower(btrim(p_text)) IN ('not_yet_assessed','not yet assessed') THEN NULL
      ELSE btrim(p_text)
    END;
$$;

-- Extract a numeric value from common temperature strings:
--   '14C', '15 C', '-80C' -> numeric
-- Also handles the kelps oddity: '12-Oct' -> 12
CREATE OR REPLACE FUNCTION staging.parse_temp_c(p_text text)
RETURNS numeric
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT
    CASE
      WHEN staging.norm_text(p_text) IS NULL THEN NULL
      -- pure integer/decimal
      WHEN staging.norm_text(p_text) ~ '^[+-]?\d+(\.\d+)?$'
        THEN staging.norm_text(p_text)::numeric
      -- common patterns like '14C', '15 C', '-80C'
      WHEN staging.norm_text(p_text) ~ '^[+-]?\d+(\.\d+)?\s*[cC]$'
        THEN regexp_replace(staging.norm_text(p_text), '\s*[cC]$', '', 'g')::numeric
      -- odd excel-ish pattern '12-Oct' -> take leading number
      WHEN staging.norm_text(p_text) ~ '^[+-]?\d{1,3}\s*-\s*[A-Za-z]{3,}$'
        THEN regexp_replace(staging.norm_text(p_text), '^([+-]?\d{1,3}).*$', '\1')::numeric
      ELSE NULL
    END;
$$;

-- Convert degrees/minutes/seconds strings to decimal degrees.
-- Handles:
--   34°02'34.6"N
--   33° 43.212' N   (deg + decimal minutes)
--   118° 20.230' W
--
-- NOTE: We make two variants for lat and lon so we can enforce sign.
CREATE OR REPLACE FUNCTION staging.parse_dms_to_decimal(p_text text)
RETURNS numeric
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  s text;
  deg numeric;
  min numeric;
  sec numeric;
  hemi text;
  m text[];
BEGIN
  s := staging.norm_text(p_text);
  IF s IS NULL THEN
    RETURN NULL;
  END IF;

  -- Normalize spacing
  s := regexp_replace(s, '\s+', ' ', 'g');
  s := btrim(s);

  -- Extract hemisphere if present
  hemi := NULL;
  IF s ~* '[NSEW]$' THEN
    hemi := upper(right(s, 1));
    s := btrim(left(s, length(s) - 1));
  END IF;

  -- Case A: DMS -> deg°min'sec"
  -- Example: 34°02'34.6"
  IF s ~ '°' AND s ~ '''' THEN
    -- Try (deg)(min)(sec optional)
    -- Capture numbers around ° and '
    m := regexp_match(s, '^\s*([+-]?\d+(?:\.\d+)?)\s*°\s*([0-9]+(?:\.\d+)?)\s*''\s*([0-9]+(?:\.\d+)?)?');
    IF m IS NOT NULL THEN
      deg := m[1]::numeric;
      min := m[2]::numeric;
      sec := COALESCE(NULLIF(m[3], '')::numeric, 0);
      RETURN deg + (min/60) + (sec/3600);
    END IF;

    -- Case B: deg° decimal_minutes'
    -- Example: 33° 43.212'
    m := regexp_match(s, '^\s*([+-]?\d+(?:\.\d+)?)\s*°\s*([0-9]+(?:\.\d+)?)\s*''\s*$');
    IF m IS NOT NULL THEN
      deg := m[1]::numeric;
      min := m[2]::numeric;
      RETURN deg + (min/60);
    END IF;
  END IF;

  -- If it doesn't match known DMS patterns, return NULL
  RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION staging.parse_dms_lat(p_text text)
RETURNS numeric
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  raw text;
  hemi text;
  val numeric;
BEGIN
  raw := staging.norm_text(p_text);
  IF raw IS NULL THEN RETURN NULL; END IF;

  hemi := NULL;
  IF raw ~* '[NSEW]\s*$' THEN
    hemi := upper(regexp_replace(raw, '^.*([NSEW])\s*$', '\1'));
  END IF;

  val := staging.parse_dms_to_decimal(raw);
  IF val IS NULL THEN
    RETURN NULL;
  END IF;

  -- Apply sign by hemisphere if we have it
  IF hemi = 'S' THEN
    RETURN -abs(val);
  ELSIF hemi = 'N' THEN
    RETURN abs(val);
  ELSE
    -- no hemi; assume already signed
    RETURN val;
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION staging.parse_dms_lon(p_text text)
RETURNS numeric
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  raw text;
  hemi text;
  val numeric;
BEGIN
  raw := staging.norm_text(p_text);
  IF raw IS NULL THEN RETURN NULL; END IF;

  hemi := NULL;
  IF raw ~* '[NSEW]\s*$' THEN
    hemi := upper(regexp_replace(raw, '^.*([NSEW])\s*$', '\1'));
  END IF;

  val := staging.parse_dms_to_decimal(raw);
  IF val IS NULL THEN
    RETURN NULL;
  END IF;

  IF hemi = 'W' THEN
    RETURN -abs(val);
  ELSIF hemi = 'E' THEN
    RETURN abs(val);
  ELSE
    RETURN val;
  END IF;
END;
$$;

-- Flexible date parsing for your observed formats.
-- Returns NULL for 'Legacy', 'Winter 2020', 'Feb-2018 to Apr-2018', 'NA', etc.
CREATE OR REPLACE FUNCTION staging.parse_date_flexible(p_text text)
RETURNS date
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  s text;
  d date;
BEGIN
  s := staging.norm_text(p_text);
  IF s IS NULL THEN
    RETURN NULL;
  END IF;

  -- Reject known non-dates seen in kelps_raw
  IF s ~* 'winter\s+\d{4}' THEN RETURN NULL; END IF;
  IF s ~* 'to' THEN RETURN NULL; END IF; -- e.g., 'Feb-2018 to Apr-2018'

  -- 1) MM/DD/YYYY or M/D/YYYY
  IF s ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
    RETURN to_date(s, 'MM/DD/YYYY');
  END IF;

  -- 2) M/D/YYYY (still covered by MM/DD/YYYY above because to_date is lenient)

  -- 3) D-Mon-YY or D-Mon-YYYY (e.g. 2-Feb-18, 27-Aug-18, 20-Jun-01, 14-Oct-83)
  IF s ~ '^\d{1,2}-[A-Za-z]{3}-\d{2,4}$' THEN
    -- Try 4-digit year first
    BEGIN
      d := to_date(s, 'DD-Mon-YYYY');
      RETURN d;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    BEGIN
      d := to_date(s, 'DD-Mon-YY');
      RETURN d;
    EXCEPTION WHEN others THEN
      NULL;
    END;
  END IF;

  -- 4) Month DD,YYYY (e.g. June 4,1990)
  IF s ~* '^[A-Za-z]+\s+\d{1,2},\s*\d{4}$' THEN
    BEGIN
      d := to_date(regexp_replace(s, '\s+', ' ', 'g'), 'Month DD, YYYY');
      RETURN d;
    EXCEPTION WHEN others THEN
      NULL;
    END;
  END IF;

  -- 5) D-Mon (no year) -> cannot safely infer year -> NULL
  IF s ~ '^\d{1,2}-[A-Za-z]{3}$' THEN
    RETURN NULL;
  END IF;

  -- 6) Mon-YY (e.g. Sep-24 in microbes) -> interpret as first of month
  IF s ~ '^[A-Za-z]{3}-\d{2}$' THEN
    BEGIN
      d := to_date('01-' || s, 'DD-Mon-YY');
      RETURN d;
    EXCEPTION WHEN others THEN
      NULL;
    END;
  END IF;

  -- If nothing matched, return NULL
  RETURN NULL;
END;
$$;

-- ----------------------------
-- 2) Create typed tables
-- ----------------------------

DROP TABLE IF EXISTS staging.kelps_typed CASCADE;
CREATE TABLE staging.kelps_typed (
  kelps_typed_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- lineage / ingestion metadata
  staging_id BIGINT NOT NULL UNIQUE,
  ingest_batch_id UUID NULL,
  source_filename TEXT NULL,
  source_row_num INTEGER NULL,
  loaded_at TIMESTAMPTZ NOT NULL,

  -- taxonomy
  taxonomy_genus TEXT NULL,
  taxonomy_species TEXT NULL,
  taxonomy_sex TEXT NULL,
  taxonomy_variety_or_form TEXT NULL,

  -- storage (raw + parsed)
  storage_details_id TEXT NULL,
  storage_details_position_id TEXT NULL,
  storage_details_rack_id TEXT NULL,
  storage_details_location TEXT NULL,
  storage_details_temperature_c_raw TEXT NULL,
  storage_details_temperature_c NUMERIC NULL,
  storage_details_medium TEXT NULL,

  -- sampling (raw + parsed)
  sampling_metadata_country TEXT NULL,
  sampling_metadata_latitude_raw TEXT NULL,
  sampling_metadata_longitude_raw TEXT NULL,
  sampling_metadata_latitude_dd NUMERIC NULL,
  sampling_metadata_longitude_dd NUMERIC NULL,
  sampling_metadata_collection_date_raw TEXT NULL,
  sampling_metadata_collection_date DATE NULL,
  sampling_metadata_personnel_collected TEXT NULL,
  sampling_metadata_isolation_date_raw TEXT NULL,
  sampling_metadata_isolation_date DATE NULL,
  sampling_metadata_deposit_date_raw TEXT NULL,
  sampling_metadata_deposit_date DATE NULL,
  sampling_metadata_deposited_by TEXT NULL,
  sampling_metadata_permit TEXT NULL,
  sampling_metadata_collection_site TEXT NULL,

  other_previously_housed_location TEXT NULL,
  sponsorship_strain_sponsorship_status TEXT NULL,
  sponsorship_code TEXT NULL,

  -- phenotypic (raw; currently always Not_Yet_Assessed in your sample)
  phenotypic_data_growth_rate_raw TEXT NULL,
  phenotypic_data_growth_rate NUMERIC NULL,
  phenotypic_data_optimal_growth_conditions TEXT NULL,
  phenotypic_data_percent_viability_raw TEXT NULL,
  phenotypic_data_percent_viability NUMERIC NULL,
  phenotypic_data_lifespan TEXT NULL,
  phenotypic_data_tolerance_to_thermal_stressor TEXT NULL,
  phenotypic_data_tolerance_to_water_quality_stressors TEXT NULL,

  inaturalist TEXT NULL,

  ecological_role_primary_producer TEXT NULL,
  ecological_role_carbon_sink TEXT NULL,
  ecological_role_habitat_former TEXT NULL,

  metabolic_pathways_kegg_pathway_id TEXT NULL,
  metabolic_pathways_metacyc_pathway_id TEXT NULL,
  functional_annotation_gene_function_id TEXT NULL,
  functional_annotation_protein_function_id TEXT NULL,

  genetic_variation_data_variant_id TEXT NULL,
  genetic_variation_data_gene_id TEXT NULL,
  genetic_variation_data_chromosome TEXT NULL,
  genetic_variation_data_reference_allele TEXT NULL,
  genetic_variation_data_alternate_allele TEXT NULL,
  genetic_variation_data_variant_type TEXT NULL,
  genetic_variation_data_allele_frequency TEXT NULL,
  genetic_variation_data_read_depth TEXT NULL,
  genetic_variation_data_quality_score TEXT NULL,
  genetic_variation_data_genotype TEXT NULL,

  genetic_diversity_within_geography_sample_sets_fst TEXT NULL,
  genetic_diversity_within_geography_sample_sets_observed_heteroz TEXT NULL,
  genetic_diversity_within_geography_sample_sets_observed_homozyg TEXT NULL,
  genetic_diversity_within_geography_sample_sets_allele_count TEXT NULL,
  genetic_diversity_within_geography_sample_sets_nucleotide_diver TEXT NULL,

  phenotypic_diversity_within_geography_sample_sets_trait_id_name TEXT NULL,
  phenotypic_diversity_within_geography_sample_sets_trait_varianc TEXT NULL,
  phenotypic_diversity_within_geography_sample_sets_trait_mean TEXT NULL,
  phenotypic_diversity_within_geography_sample_sets_trait_standar TEXT NULL,
  phenotypic_diversity_within_geography_sample_sets_trait_range TEXT NULL,

  iucn_red_list_extinct_ex TEXT NULL,
  iucn_red_list_extinct_in_the_wild_ew TEXT NULL,
  iucn_red_list_critically_endangered_cr TEXT NULL,
  iucn_red_list_endangered_en TEXT NULL,
  iucn_red_list_vulnerable_vu TEXT NULL,
  iucn_red_list_least_concern TEXT NULL,
  iucn_red_list_data_deficient_dd TEXT NULL,
  iucn_red_list_not_evaluated_ne TEXT NULL,

  ecosystem_endemic TEXT NULL,
  ecosystem_naturalized TEXT NULL,
  ecosystem_invasive TEXT NULL,
  ecosystem_adventive TEXT NULL,
  ecosystem_extirpated TEXT NULL,
  ecosystem_weed TEXT NULL,
  ecosystem_cultivated_horticultural TEXT NULL,
  ecosystem_ruderal TEXT NULL,
  ecosystem_pioneer TEXT NULL,

  commercial_bio_variables_harvestable_yield_per_cycle TEXT NULL,
  commercial_bio_variables_harvest_season TEXT NULL,
  commercial_bio_variables_light_needed TEXT NULL,
  commercial_production_spoilage_rate TEXT NULL,
  commercial_production_operational_cost_per_cycle TEXT NULL,
  commercial_production_gross_margin TEXT NULL,
  commercial_market_price_volatility_index TEXT NULL,
  commercial_market_demand_index_sector TEXT NULL,
  commercial_market_market_growth_rate TEXT NULL,
  commercial_processing_moisture_content TEXT NULL,
  commercial_processing_protein_content TEXT NULL,
  commercial_processing_alginate_or_carrageenan_content TEXT NULL,
  commercial_processing_contaminants TEXT NULL,
  commercial_processing_shelf_life TEXT NULL,
  commercial_processing_grade_quality_score TEXT NULL,
  commercial_supply_logistics_transport_cost TEXT NULL,
  commercial_supply_logistics_distribution_channel TEXT NULL,
  commercial_supply_logistics_carbon_footprint_transport TEXT NULL
);

DROP TABLE IF EXISTS staging.microbes_typed CASCADE;
CREATE TABLE staging.microbes_typed (
  microbes_typed_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- lineage / ingestion metadata
  staging_id BIGINT NOT NULL UNIQUE,
  ingest_batch_id UUID NULL,
  source_filename TEXT NULL,
  source_row_num INTEGER NULL,
  loaded_at TIMESTAMPTZ NOT NULL,

  microbe_id TEXT NULL,
  original_code TEXT NULL,
  institution_isolation_physically_conducted TEXT NULL,

  isolated_year_raw TEXT NULL,
  isolated_year INTEGER NULL,

  isolated_by TEXT NULL,
  maintained_by TEXT NULL,
  maintained_at TEXT NULL,

  kelp_host TEXT NULL,
  kelp_ka_sample_id TEXT NULL,
  source_if_ka_id TEXT NULL,
  source_if_no_ka_id TEXT NULL,
  kelp_location TEXT NULL,

  kelp_collection_temp_raw TEXT NULL,
  kelp_collection_temp_c NUMERIC NULL,

  kelp_collection_month TEXT NULL,
  kelp_collection_season TEXT NULL,
  kelp_thallus_collection TEXT NULL,
  kelp_collection_approach TEXT NULL,
  kelp_collection_method TEXT NULL,

  microbe_isolation_methods TEXT NULL,
  microbe_isolation_protocol TEXT NULL,
  isolation_media TEXT NULL,

  location_stored1 TEXT NULL,
  location_1_temperature_raw TEXT NULL,
  location_1_temperature_c NUMERIC NULL,

  location_stored2 TEXT NULL,
  location_2_temperature_raw TEXT NULL,
  location_2_temperature_c NUMERIC NULL,

  cryopreservation_date_raw TEXT NULL,
  cryopreservation_date DATE NULL,

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

  sequencing_date_raw TEXT NULL,
  sequencing_date DATE NULL,

  primers_used TEXT NULL,
  sequencing_notes TEXT NULL,
  sequencing_conducted_by TEXT NULL,

  total_bp_length_after_trimming TEXT NULL,
  closest_ncbi_blast_tax_id TEXT NULL,
  ncbi_blast_query_cover TEXT NULL,

  percent_identity_raw TEXT NULL,
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

-- ----------------------------
-- 3) Insert (raw -> typed)
-- ----------------------------

INSERT INTO staging.kelps_typed (
  staging_id, ingest_batch_id, source_filename, source_row_num, loaded_at,

  taxonomy_genus, taxonomy_species, taxonomy_sex, taxonomy_variety_or_form,

  storage_details_id, storage_details_position_id, storage_details_rack_id,
  storage_details_location, storage_details_temperature_c_raw, storage_details_temperature_c,
  storage_details_medium,

  sampling_metadata_country,
  sampling_metadata_latitude_raw, sampling_metadata_longitude_raw,
  sampling_metadata_latitude_dd, sampling_metadata_longitude_dd,
  sampling_metadata_collection_date_raw, sampling_metadata_collection_date,
  sampling_metadata_personnel_collected,
  sampling_metadata_isolation_date_raw, sampling_metadata_isolation_date,
  sampling_metadata_deposit_date_raw, sampling_metadata_deposit_date,
  sampling_metadata_deposited_by, sampling_metadata_permit, sampling_metadata_collection_site,

  other_previously_housed_location,
  sponsorship_strain_sponsorship_status, sponsorship_code,

  phenotypic_data_growth_rate_raw, phenotypic_data_growth_rate,
  phenotypic_data_optimal_growth_conditions,
  phenotypic_data_percent_viability_raw, phenotypic_data_percent_viability,
  phenotypic_data_lifespan,
  phenotypic_data_tolerance_to_thermal_stressor,
  phenotypic_data_tolerance_to_water_quality_stressors,

  inaturalist,

  ecological_role_primary_producer, ecological_role_carbon_sink, ecological_role_habitat_former,

  metabolic_pathways_kegg_pathway_id, metabolic_pathways_metacyc_pathway_id,
  functional_annotation_gene_function_id, functional_annotation_protein_function_id,

  genetic_variation_data_variant_id, genetic_variation_data_gene_id, genetic_variation_data_chromosome,
  genetic_variation_data_reference_allele, genetic_variation_data_alternate_allele,
  genetic_variation_data_variant_type, genetic_variation_data_allele_frequency,
  genetic_variation_data_read_depth, genetic_variation_data_quality_score, genetic_variation_data_genotype,

  genetic_diversity_within_geography_sample_sets_fst,
  genetic_diversity_within_geography_sample_sets_observed_heteroz,
  genetic_diversity_within_geography_sample_sets_observed_homozyg,
  genetic_diversity_within_geography_sample_sets_allele_count,
  genetic_diversity_within_geography_sample_sets_nucleotide_diver,

  phenotypic_diversity_within_geography_sample_sets_trait_id_name,
  phenotypic_diversity_within_geography_sample_sets_trait_varianc,
  phenotypic_diversity_within_geography_sample_sets_trait_mean,
  phenotypic_diversity_within_geography_sample_sets_trait_standar,
  phenotypic_diversity_within_geography_sample_sets_trait_range,

  iucn_red_list_extinct_ex,
  iucn_red_list_extinct_in_the_wild_ew,
  iucn_red_list_critically_endangered_cr,
  iucn_red_list_endangered_en,
  iucn_red_list_vulnerable_vu,
  iucn_red_list_least_concern,
  iucn_red_list_data_deficient_dd,
  iucn_red_list_not_evaluated_ne,

  ecosystem_endemic, ecosystem_naturalized, ecosystem_invasive, ecosystem_adventive,
  ecosystem_extirpated, ecosystem_weed, ecosystem_cultivated_horticultural,
  ecosystem_ruderal, ecosystem_pioneer,

  commercial_bio_variables_harvestable_yield_per_cycle,
  commercial_bio_variables_harvest_season,
  commercial_bio_variables_light_needed,
  commercial_production_spoilage_rate,
  commercial_production_operational_cost_per_cycle,
  commercial_production_gross_margin,
  commercial_market_price_volatility_index,
  commercial_market_demand_index_sector,
  commercial_market_market_growth_rate,
  commercial_processing_moisture_content,
  commercial_processing_protein_content,
  commercial_processing_alginate_or_carrageenan_content,
  commercial_processing_contaminants,
  commercial_processing_shelf_life,
  commercial_processing_grade_quality_score,
  commercial_supply_logistics_transport_cost,
  commercial_supply_logistics_distribution_channel,
  commercial_supply_logistics_carbon_footprint_transport
)
SELECT
  r.staging_id,
  r.ingest_batch_id,
  staging.norm_text(r.source_filename),
  r.source_row_num,
  r.loaded_at,

  staging.norm_text(r.taxonomy_genus),
  staging.norm_text(r.taxonomy_species),
  staging.norm_text(r.taxonomy_sex),
  staging.norm_text(r.taxonomy_variety_or_form),

  staging.norm_text(r.storage_details_id),
  staging.norm_text(r.storage_details_position_id),
  staging.norm_text(r.storage_details_rack_id),
  staging.norm_text(r.storage_details_location),
  r.storage_details_temperature_c,
  staging.parse_temp_c(r.storage_details_temperature_c),
  staging.norm_text(r.storage_details_medium),

  staging.norm_text(r.sampling_metadata_country),

  r.sampling_metadata_latitude,
  r.sampling_metadata_longitude,
  staging.parse_dms_lat(r.sampling_metadata_latitude),
  staging.parse_dms_lon(r.sampling_metadata_longitude),

  r.sampling_metadata_collection_date,
  staging.parse_date_flexible(r.sampling_metadata_collection_date),

  staging.norm_text(r.sampling_metadata_personnel_collected),

  r.sampling_metadata_isolation_date,
  staging.parse_date_flexible(r.sampling_metadata_isolation_date),

  r.sampling_metadata_deposit_date,
  staging.parse_date_flexible(r.sampling_metadata_deposit_date),

  staging.norm_text(r.sampling_metadata_deposited_by),
  staging.norm_text(r.sampling_metadata_permit),
  staging.norm_text(r.sampling_metadata_collection_site),

  staging.norm_text(r.other_previously_housed_location),
  staging.norm_text(r.sponsorship_strain_sponsorship_status),
  staging.norm_text(r.sponsorship_code),

  r.phenotypic_data_growth_rate,
  NULL::numeric, -- currently all Not_Yet_Assessed; keep NULL until real numeric appears
  staging.norm_text(r.phenotypic_data_optimal_growth_conditions),

  r.phenotypic_data_percent_viability,
  NULL::numeric, -- currently all Not_Yet_Assessed; keep NULL until real numeric appears
  staging.norm_text(r.phenotypic_data_lifespan),
  staging.norm_text(r.phenotypic_data_tolerance_to_thermal_stressor),
  staging.norm_text(r.phenotypic_data_tolerance_to_water_quality_stressors),

  staging.norm_text(r.inaturalist),

  staging.norm_text(r.ecological_role_primary_producer),
  staging.norm_text(r.ecological_role_carbon_sink),
  staging.norm_text(r.ecological_role_habitat_former),

  staging.norm_text(r.metabolic_pathways_kegg_pathway_id),
  staging.norm_text(r.metabolic_pathways_metacyc_pathway_id),
  staging.norm_text(r.functional_annotation_gene_function_id),
  staging.norm_text(r.functional_annotation_protein_function_id),

  staging.norm_text(r.genetic_variation_data_variant_id),
  staging.norm_text(r.genetic_variation_data_gene_id),
  staging.norm_text(r.genetic_variation_data_chromosome),
  staging.norm_text(r.genetic_variation_data_reference_allele),
  staging.norm_text(r.genetic_variation_data_alternate_allele),
  staging.norm_text(r.genetic_variation_data_variant_type),
  staging.norm_text(r.genetic_variation_data_allele_frequency),
  staging.norm_text(r.genetic_variation_data_read_depth),
  staging.norm_text(r.genetic_variation_data_quality_score),
  staging.norm_text(r.genetic_variation_data_genotype),

  staging.norm_text(r.genetic_diversity_within_geography_sample_sets_fst),
  staging.norm_text(r.genetic_diversity_within_geography_sample_sets_observed_heteroz),
  staging.norm_text(r.genetic_diversity_within_geography_sample_sets_observed_homozyg),
  staging.norm_text(r.genetic_diversity_within_geography_sample_sets_allele_count),
  staging.norm_text(r.genetic_diversity_within_geography_sample_sets_nucleotide_diver),

  staging.norm_text(r.phenotypic_diversity_within_geography_sample_sets_trait_id_name),
  staging.norm_text(r.phenotypic_diversity_within_geography_sample_sets_trait_varianc),
  staging.norm_text(r.phenotypic_diversity_within_geography_sample_sets_trait_mean),
  staging.norm_text(r.phenotypic_diversity_within_geography_sample_sets_trait_standar),
  staging.norm_text(r.phenotypic_diversity_within_geography_sample_sets_trait_range),

  staging.norm_text(r.iucn_red_list_extinct_ex),
  staging.norm_text(r.iucn_red_list_extinct_in_the_wild_ew),
  staging.norm_text(r.iucn_red_list_critically_endangered_cr),
  staging.norm_text(r.iucn_red_list_endangered_en),
  staging.norm_text(r.iucn_red_list_vulnerable_vu),
  staging.norm_text(r.iucn_red_list_least_concern),
  staging.norm_text(r.iucn_red_list_data_deficient_dd),
  staging.norm_text(r.iucn_red_list_not_evaluated_ne),

  staging.norm_text(r.ecosystem_endemic),
  staging.norm_text(r.ecosystem_naturalized),
  staging.norm_text(r.ecosystem_invasive),
  staging.norm_text(r.ecosystem_adventive),
  staging.norm_text(r.ecosystem_extirpated),
  staging.norm_text(r.ecosystem_weed),
  staging.norm_text(r.ecosystem_cultivated_horticultural),
  staging.norm_text(r.ecosystem_ruderal),
  staging.norm_text(r.ecosystem_pioneer),

  staging.norm_text(r.commercial_bio_variables_harvestable_yield_per_cycle),
  staging.norm_text(r.commercial_bio_variables_harvest_season),
  staging.norm_text(r.commercial_bio_variables_light_needed),
  staging.norm_text(r.commercial_production_spoilage_rate),
  staging.norm_text(r.commercial_production_operational_cost_per_cycle),
  staging.norm_text(r.commercial_production_gross_margin),
  staging.norm_text(r.commercial_market_price_volatility_index),
  staging.norm_text(r.commercial_market_demand_index_sector),
  staging.norm_text(r.commercial_market_market_growth_rate),
  staging.norm_text(r.commercial_processing_moisture_content),
  staging.norm_text(r.commercial_processing_protein_content),
  staging.norm_text(r.commercial_processing_alginate_or_carrageenan_content),
  staging.norm_text(r.commercial_processing_contaminants),
  staging.norm_text(r.commercial_processing_shelf_life),
  staging.norm_text(r.commercial_processing_grade_quality_score),
  staging.norm_text(r.commercial_supply_logistics_transport_cost),
  staging.norm_text(r.commercial_supply_logistics_distribution_channel),
  staging.norm_text(r.commercial_supply_logistics_carbon_footprint_transport)
FROM staging.kelps_raw r;

INSERT INTO staging.microbes_typed (
  staging_id, ingest_batch_id, source_filename, source_row_num, loaded_at,
  microbe_id, original_code, institution_isolation_physically_conducted,
  isolated_year_raw, isolated_year,
  isolated_by, maintained_by, maintained_at,
  kelp_host, kelp_ka_sample_id, source_if_ka_id, source_if_no_ka_id,
  kelp_location,
  kelp_collection_temp_raw, kelp_collection_temp_c,
  kelp_collection_month, kelp_collection_season, kelp_thallus_collection,
  kelp_collection_approach, kelp_collection_method,
  microbe_isolation_methods, microbe_isolation_protocol, isolation_media,
  location_stored1, location_1_temperature_raw, location_1_temperature_c,
  location_stored2, location_2_temperature_raw, location_2_temperature_c,
  cryopreservation_date_raw, cryopreservation_date,
  cryo_storage_medium, cryo_storage_preservative, cryo_revival_tested,
  cryo_backups_created, cryopreservation_protocol,
  malditof_procedure, malditof_dataanalysis_complete, high_quality_malditof_data,
  s16_pcr_completed, pcr_conducted_by, sanger_sequencing_completed,
  sequencing_date_raw, sequencing_date,
  primers_used, sequencing_notes, sequencing_conducted_by,
  total_bp_length_after_trimming, closest_ncbi_blast_tax_id, ncbi_blast_query_cover,
  percent_identity_raw, percent_identity,
  accession, taxonomy_kingdom, s16_sequence, its2_sequence,
  pathogen_activity_kelp, pathogen_activity_humans, pathogen_activity_plants, pathogen_activity_animals,
  growth_temperature_c_range, growth_salinity_range, growth_ph_range, growth_optimal_media,
  morphology_colony_color, morphology_colony_size, morphology_colony_shape, morphology_colony_texture,
  gram_stain, morphology_cell_shape,
  probiotic_activity, probiotic_known_host
)
SELECT
  r.staging_id,
  r.ingest_batch_id,
  staging.norm_text(r.source_filename),
  r.source_row_num,
  r.loaded_at,

  staging.norm_text(r.microbe_id),
  staging.norm_text(r.original_code),
  staging.norm_text(r.institution_isolation_physically_conducted),

  r.isolated_year,
  CASE
    WHEN staging.norm_text(r.isolated_year) ~ '^\d{4}$' THEN staging.norm_text(r.isolated_year)::int
    ELSE NULL
  END,

  staging.norm_text(r.isolated_by),
  staging.norm_text(r.maintained_by),
  staging.norm_text(r.maintained_at),

  staging.norm_text(r.kelp_host),
  staging.norm_text(r.kelp_ka_sample_id),
  staging.norm_text(r.source_if_ka_id),
  staging.norm_text(r.source_if_no_ka_id),
  staging.norm_text(r.kelp_location),

  r.kelp_collection_temp,
  staging.parse_temp_c(r.kelp_collection_temp),

  staging.norm_text(r.kelp_collection_month),
  staging.norm_text(r.kelp_collection_season),
  staging.norm_text(r.kelp_thallus_collection),
  staging.norm_text(r.kelp_collection_approach),
  staging.norm_text(r.kelp_collection_method),

  staging.norm_text(r.microbe_isolation_methods),
  staging.norm_text(r.microbe_isolation_protocol),
  staging.norm_text(r.isolation_media),

  staging.norm_text(r.location_stored1),
  r.location_1_temperature,
  staging.parse_temp_c(r.location_1_temperature),

  staging.norm_text(r.location_stored2),
  r.location_2_temperature,
  staging.parse_temp_c(r.location_2_temperature),

  r.cryopreservation_date,
  staging.parse_date_flexible(r.cryopreservation_date),

  staging.norm_text(r.cryo_storage_medium),
  staging.norm_text(r.cryo_storage_preservative),
  staging.norm_text(r.cryo_revival_tested),
  staging.norm_text(r.cryo_backups_created),
  staging.norm_text(r.cryopreservation_protocol),

  staging.norm_text(r.malditof_procedure),
  staging.norm_text(r.malditof_dataanalysis_complete),
  staging.norm_text(r.high_quality_malditof_data),

  staging.norm_text(r.s16_pcr_completed),
  staging.norm_text(r.pcr_conducted_by),
  staging.norm_text(r.sanger_sequencing_completed),

  r.sequencing_date,
  staging.parse_date_flexible(r.sequencing_date),

  staging.norm_text(r.primers_used),
  staging.norm_text(r.sequencing_notes),
  staging.norm_text(r.sequencing_conducted_by),

  staging.norm_text(r.total_bp_length_after_trimming),
  staging.norm_text(r.closest_ncbi_blast_tax_id),
  staging.norm_text(r.ncbi_blast_query_cover),

  r.percent_identity,
  CASE
    WHEN staging.norm_text(r.percent_identity) ~ '^[0-9]+(\.[0-9]+)?$' THEN staging.norm_text(r.percent_identity)::numeric
    ELSE NULL
  END,

  staging.norm_text(r.accession),
  staging.norm_text(r.taxonomy_kingdom),
  staging.norm_text(r.s16_sequence),
  staging.norm_text(r.its2_sequence),

  staging.norm_text(r.pathogen_activity_kelp),
  staging.norm_text(r.pathogen_activity_humans),
  staging.norm_text(r.pathogen_activity_plants),
  staging.norm_text(r.pathogen_activity_animals),

  staging.norm_text(r.growth_temperature_c_range),
  staging.norm_text(r.growth_salinity_range),
  staging.norm_text(r.growth_ph_range),
  staging.norm_text(r.growth_optimal_media),

  staging.norm_text(r.morphology_colony_color),
  staging.norm_text(r.morphology_colony_size),
  staging.norm_text(r.morphology_colony_shape),
  staging.norm_text(r.morphology_colony_texture),

  staging.norm_text(r.gram_stain),
  staging.norm_text(r.morphology_cell_shape),

  staging.norm_text(r.probiotic_activity),
  staging.norm_text(r.probiotic_known_host)
FROM staging.microbes_raw r;

-- ----------------------------
-- 4) Quick sanity checks (optional)
-- ----------------------------

-- How many temps parsed?
-- SELECT
--   count(*) AS rows,
--   count(*) FILTER (WHERE storage_details_temperature_c IS NOT NULL) AS parsed_temp_rows
-- FROM staging.kelps_typed;

-- How many lat/lon parsed?
-- SELECT
--   count(*) AS rows,
--   count(*) FILTER (WHERE sampling_metadata_latitude_dd IS NOT NULL) AS parsed_lat_rows,
--   count(*) FILTER (WHERE sampling_metadata_longitude_dd IS NOT NULL) AS parsed_lon_rows
-- FROM staging.kelps_typed;

-- How many dates parsed?
-- SELECT
--   count(*) AS rows,
--   count(*) FILTER (WHERE sampling_metadata_collection_date IS NOT NULL) AS parsed_collection_date_rows
-- FROM staging.kelps_typed;

COMMIT;
