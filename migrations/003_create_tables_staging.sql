BEGIN;

-- =========================
-- Schema safety
-- =========================
CREATE SCHEMA IF NOT EXISTS staging;

-- Optional: nice for case-insensitive comparisons later
-- CREATE EXTENSION IF NOT EXISTS citext;

-- =========================
-- Raw staging: KELPS
-- =========================
DROP TABLE IF EXISTS staging.kelps_raw;

CREATE TABLE staging.kelps_raw (
  -- Ingest metadata (not in CSV)
  staging_id        BIGSERIAL PRIMARY KEY,
  ingest_batch_id   UUID NULL,
  source_filename   TEXT NULL,
  source_row_num    INTEGER NULL,
  loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- CSV columns (all TEXT)
  taxonomy_genus TEXT,
  taxonomy_species TEXT,
  taxonomy_sex TEXT,
  taxonomy_variety_or_form TEXT,
  storage_details_id TEXT,
  storage_details_position_id TEXT,
  storage_details_rack_id TEXT,
  storage_details_location TEXT,
  storage_details_temperature_c TEXT,
  storage_details_medium TEXT,
  sampling_metadata_country TEXT,
  sampling_metadata_latitude TEXT,
  sampling_metadata_longitude TEXT,
  sampling_metadata_collection_date TEXT,
  sampling_metadata_personnel_collected TEXT,
  sampling_metadata_isolation_date TEXT,
  sampling_metadata_deposit_date TEXT,
  sampling_metadata_deposited_by TEXT,
  sampling_metadata_permit TEXT,
  sampling_metadata_collection_site TEXT,
  other_previously_housed_location TEXT,
  sponsorship_strain_sponsorship_status TEXT,
  sponsorship_code TEXT,
  phenotypic_data_growth_rate TEXT,
  phenotypic_data_optimal_growth_conditions TEXT,
  phenotypic_data_percent_viability TEXT,
  phenotypic_data_lifespan TEXT,
  phenotypic_data_tolerance_to_thermal_stressor TEXT,
  phenotypic_data_tolerance_to_water_quality_stressors TEXT,
  inaturalist TEXT,
  ecological_role_primary_producer TEXT,
  ecological_role_carbon_sink TEXT,
  ecological_role_habitat_former TEXT,
  metabolic_pathways_kegg_pathway_id TEXT,
  metabolic_pathways_metacyc_pathway_id TEXT,
  functional_annotation_gene_function_id TEXT,
  functional_annotation_protein_function_id TEXT,
  genetic_variation_data_variant_id TEXT,
  genetic_variation_data_gene_id TEXT,
  genetic_variation_data_chromosome TEXT,
  genetic_variation_data_reference_allele TEXT,
  genetic_variation_data_alternate_allele TEXT,
  genetic_variation_data_variant_type TEXT,
  genetic_variation_data_allele_frequency TEXT,
  genetic_variation_data_read_depth TEXT,
  genetic_variation_data_quality_score TEXT,
  genetic_variation_data_genotype TEXT,
  genetic_diversity_within_geography_sample_sets_fst TEXT,
  genetic_diversity_within_geography_sample_sets_observed_heterozygosity TEXT,
  genetic_diversity_within_geography_sample_sets_observed_homozygosity TEXT,
  genetic_diversity_within_geography_sample_sets_allele_count TEXT,
  genetic_diversity_within_geography_sample_sets_nucleotide_diversity TEXT,
  phenotypic_diversity_within_geography_sample_sets_trait_id_name TEXT,
  phenotypic_diversity_within_geography_sample_sets_trait_variance TEXT,
  phenotypic_diversity_within_geography_sample_sets_trait_mean TEXT,
  phenotypic_diversity_within_geography_sample_sets_trait_standard_deviation TEXT,
  phenotypic_diversity_within_geography_sample_sets_trait_range TEXT,
  iucn_red_list_extinct_ex TEXT,
  iucn_red_list_extinct_in_the_wild_ew TEXT,
  iucn_red_list_critically_endangered_cr TEXT,
  iucn_red_list_endangered_en TEXT,
  iucn_red_list_vulnerable_vu TEXT,
  iucn_red_list_least_concern TEXT,
  iucn_red_list_data_deficient_dd TEXT,
  iucn_red_list_not_evaluated_ne TEXT,
  ecosystem_endemic TEXT,
  ecosystem_naturalized TEXT,
  ecosystem_invasive TEXT,
  ecosystem_adventive TEXT,
  ecosystem_extirpated TEXT,
  ecosystem_weed TEXT,
  ecosystem_cultivated_horticultural TEXT,
  ecosystem_ruderal TEXT,
  ecosystem_pioneer TEXT,
  commercial_bio_variables_harvestable_yield_per_cycle TEXT,
  commercial_bio_variables_harvest_season TEXT,
  commercial_bio_variables_light_needed TEXT,
  commercial_production_spoilage_rate TEXT,
  commercial_production_operational_cost_per_cycle TEXT,
  commercial_production_gross_margin TEXT,
  commercial_market_price_volatility_index TEXT,
  commercial_market_demand_index_sector TEXT,
  commercial_market_market_growth_rate TEXT,
  commercial_processing_moisture_content TEXT,
  commercial_processing_protein_content TEXT,
  commercial_processing_alginate_or_carrageenan_content TEXT,
  commercial_processing_contaminants TEXT,
  commercial_processing_shelf_life TEXT,
  commercial_processing_grade_quality_score TEXT,
  commercial_supply_logistics_transport_cost TEXT,
  commercial_supply_logistics_distribution_channel TEXT,
  commercial_supply_logistics_carbon_footprint_transport TEXT
);

CREATE INDEX IF NOT EXISTS idx_kelps_raw_batch
  ON staging.kelps_raw (ingest_batch_id);

CREATE INDEX IF NOT EXISTS idx_kelps_raw_storage_details_id
  ON staging.kelps_raw (storage_details_id);

-- =========================
-- Raw staging: MICROBES
-- (header set from the microbes CSV you pasted earlier)
-- =========================
DROP TABLE IF EXISTS staging.microbes_raw;

CREATE TABLE staging.microbes_raw (
  -- Ingest metadata (not in CSV)
  staging_id        BIGSERIAL PRIMARY KEY,
  ingest_batch_id   UUID NULL,
  source_filename   TEXT NULL,
  source_row_num    INTEGER NULL,
  loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- CSV columns (all TEXT)
  microbe_id TEXT,
  original_code TEXT,
  institution_isolation_physically_conducted TEXT,
  isolated_year TEXT,
  isolated_by TEXT,
  maintained_by TEXT,
  maintained_at TEXT,
  kelp_host TEXT,
  kelp_ka_sample_id TEXT,
  source_if_ka_id TEXT,
  source_if_no_ka_id TEXT,
  kelp_location TEXT,
  kelp_collection_temp TEXT,
  kelp_collection_month TEXT,
  kelp_collection_season TEXT,
  kelp_thallus_collection TEXT,
  kelp_collection_approach TEXT,
  kelp_collection_method TEXT,
  microbe_isolation_methods TEXT,
  microbe_isolation_protocol TEXT,
  isolation_media TEXT,
  location_stored1 TEXT,
  location_1_temperature TEXT,
  location_stored2 TEXT,
  location_2_temperature TEXT,
  cryopreservation_date TEXT,
  cryo_storage_medium TEXT,
  cryo_storage_preservative TEXT,
  cryo_revival_tested TEXT,
  cryo_backups_created TEXT,
  cryopreservation_protocol TEXT,
  malditof_procedure TEXT,
  malditof_dataanalysis_complete TEXT,
  high_quality_malditof_data TEXT,
  s16_pcr_completed TEXT,
  pcr_conducted_by TEXT,
  sanger_sequencing_completed TEXT,
  sequencing_date TEXT,
  primers_used TEXT,
  sequencing_notes TEXT,
  sequencing_conducted_by TEXT,
  total_bp_length_after_trimming TEXT,
  closest_ncbi_blast_tax_id TEXT,
  ncbi_blast_query_cover TEXT,
  percent_identity TEXT,
  accession TEXT,
  taxonomy_kingdom TEXT,
  s16_sequence TEXT,
  its2_sequence TEXT,
  pathogen_activity_kelp TEXT,
  pathogen_activity_humans TEXT,
  pathogen_activity_plants TEXT,
  pathogen_activity_animals TEXT,
  growth_temperature_c_range TEXT,
  growth_salinity_range TEXT,
  growth_ph_range TEXT,
  growth_optimal_media TEXT,
  morphology_colony_color TEXT,
  morphology_colony_size TEXT,
  morphology_colony_shape TEXT,
  morphology_colony_texture TEXT,
  gram_stain TEXT,
  morphology_cell_shape TEXT,
  probiotic_activity TEXT,
  probiotic_known_host TEXT
);

CREATE INDEX IF NOT EXISTS idx_microbes_raw_batch
  ON staging.microbes_raw (ingest_batch_id);

CREATE INDEX IF NOT EXISTS idx_microbes_raw_microbe_id
  ON staging.microbes_raw (microbe_id);

CREATE INDEX IF NOT EXISTS idx_microbes_raw_kelp_ka_sample_id
  ON staging.microbes_raw (kelp_ka_sample_id);

COMMIT;
