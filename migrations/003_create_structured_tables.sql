-- 003_create_structured_tables.sql
BEGIN;

-- =========================
-- KELPS (structured)
-- =========================

CREATE TABLE IF NOT EXISTS structured.kelp_specimen (
  storage_details_id TEXT PRIMARY KEY,
  genus TEXT NOT NULL,
  species TEXT NOT NULL,
  sex TEXT NULL,
  variety_or_form TEXT NULL,
  inaturalist_url TEXT NULL,

  -- ingestion metadata
  ingest_batch_id UUID NULL REFERENCES structured.ingest_batch(ingest_batch_id),
  source_filename TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS structured.kelp_sampling (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,

  country TEXT NULL,
  latitude_raw TEXT NULL,
  longitude_raw TEXT NULL,
  latitude_dd DOUBLE PRECISION NULL,
  longitude_dd DOUBLE PRECISION NULL,

  collection_date DATE NULL,
  collection_date_raw TEXT NULL,

  collector_source TEXT NULL,  -- internal
  permit_type TEXT NULL,
  deposited_by TEXT NULL,      -- internal
  collection_site TEXT NULL
);

CREATE TABLE IF NOT EXISTS structured.kelp_storage_current (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,

  location_site TEXT NULL,  -- internal
  position_id TEXT NULL,    -- internal
  rack_id TEXT NULL,        -- internal

  medium TEXT NULL,         -- internal
  temperature_c_min NUMERIC NULL,
  temperature_c_max NUMERIC NULL,
  temperature_c_text TEXT NULL,

  previously_housed_location TEXT NULL
);

CREATE TABLE IF NOT EXISTS structured.kelp_sponsorship (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  sponsorship_status TEXT NULL,
  sponsorship_code TEXT NULL  -- internal
);

CREATE TABLE IF NOT EXISTS structured.kelp_phenotype_current (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,

  growth_rate TEXT NULL,
  optimal_growth_conditions TEXT NULL,
  viability_percent NUMERIC NULL,
  lifespan TEXT NULL,
  tolerance_thermal_stressors TEXT NULL,
  tolerance_water_quality_stressors TEXT NULL
);

CREATE TABLE IF NOT EXISTS structured.kelp_ecological_role (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  primary_producer TEXT NULL,
  carbon_sink TEXT NULL,
  habitat_former TEXT NULL
);

-- Pathways (1-to-many safe)
CREATE TABLE IF NOT EXISTS structured.kelp_kegg_pathway (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  storage_details_id TEXT NOT NULL REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  kegg_pathway_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_kelp_kegg_storage ON structured.kelp_kegg_pathway(storage_details_id);

CREATE TABLE IF NOT EXISTS structured.kelp_metacyc_pathway (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  storage_details_id TEXT NOT NULL REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  metacyc_pathway_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_kelp_metacyc_storage ON structured.kelp_metacyc_pathway(storage_details_id);

-- Functional annotations (1-to-many)
CREATE TABLE IF NOT EXISTS structured.kelp_gene_function (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  storage_details_id TEXT NOT NULL REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  gene_function_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS structured.kelp_protein_function (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  storage_details_id TEXT NOT NULL REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  protein_function_id TEXT NOT NULL
);

-- Variants (1-to-many)
CREATE TABLE IF NOT EXISTS structured.kelp_variant (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  storage_details_id TEXT NOT NULL REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,

  variant_id TEXT NULL,
  gene_id TEXT NULL,
  chromosome TEXT NULL,

  reference_allele TEXT NULL,
  alternate_allele TEXT NULL,
  variant_type TEXT NULL,

  allele_frequency NUMERIC NULL,
  read_depth NUMERIC NULL,
  quality_score NUMERIC NULL,
  genotype TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_kelp_variant_storage ON structured.kelp_variant(storage_details_id);

-- Diversity metrics (per specimen in Phase 1)
CREATE TABLE IF NOT EXISTS structured.kelp_genetic_diversity_metrics (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  fst NUMERIC NULL,
  observed_heterozygosity NUMERIC NULL,
  observed_homozygosity NUMERIC NULL,
  allele_count NUMERIC NULL,
  nucleotide_diversity NUMERIC NULL
);

CREATE TABLE IF NOT EXISTS structured.kelp_phenotypic_diversity_metrics (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  trait_id_name TEXT NULL,
  trait_variance NUMERIC NULL,
  trait_mean NUMERIC NULL,
  trait_std_dev NUMERIC NULL,
  trait_range TEXT NULL
);

-- IUCN + Ecosystem flags
CREATE TABLE IF NOT EXISTS structured.kelp_iucn_status (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  extinct_ex BOOLEAN NULL,
  extinct_in_wild_ew BOOLEAN NULL,
  critically_endangered_cr BOOLEAN NULL,
  endangered_en BOOLEAN NULL,
  vulnerable_vu BOOLEAN NULL,
  near_threatened_nt BOOLEAN NULL,
  least_concern_lc BOOLEAN NULL,
  data_deficient_dd BOOLEAN NULL,
  not_evaluated_ne BOOLEAN NULL
);

CREATE TABLE IF NOT EXISTS structured.kelp_ecosystem_flags (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  endemic BOOLEAN NULL,
  naturalized BOOLEAN NULL,
  invasive BOOLEAN NULL,
  adventive BOOLEAN NULL,
  extirpated BOOLEAN NULL,
  weed BOOLEAN NULL,
  cultivated_horticultural BOOLEAN NULL,
  ruderal BOOLEAN NULL,
  pioneer_species BOOLEAN NULL
);

-- Commercial tables
CREATE TABLE IF NOT EXISTS structured.kelp_commercial_bio (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  harvestable_yield_per_cycle NUMERIC NULL,
  harvest_season TEXT NULL,
  light_availability_needed TEXT NULL
);

CREATE TABLE IF NOT EXISTS structured.kelp_commercial_production (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  spoilage_rate_percent NUMERIC NULL,
  operational_cost_per_cycle NUMERIC NULL,
  gross_margin_percent NUMERIC NULL
);

CREATE TABLE IF NOT EXISTS structured.kelp_commercial_market (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  price_volatility_index NUMERIC NULL,
  demand_index_sector TEXT NULL,
  market_growth_rate_percent NUMERIC NULL
);

CREATE TABLE IF NOT EXISTS structured.kelp_commercial_processing (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  moisture_content_percent NUMERIC NULL,
  protein_content_percent NUMERIC NULL,
  alginate_or_carrageenan_content_percent NUMERIC NULL,
  contaminants TEXT NULL,
  shelf_life TEXT NULL,
  grade_quality_score NUMERIC NULL
);

CREATE TABLE IF NOT EXISTS structured.kelp_commercial_logistics (
  storage_details_id TEXT PRIMARY KEY REFERENCES structured.kelp_specimen(storage_details_id) ON DELETE CASCADE,
  transport_cost_per_kg NUMERIC NULL,
  distribution_channel TEXT NULL,
  carbon_footprint_transport NUMERIC NULL
);

-- Indexes for common filters
CREATE INDEX IF NOT EXISTS idx_kelp_specimen_tax ON structured.kelp_specimen(genus, species);


-- =========================
-- MICROBES (structured)
-- =========================

CREATE TABLE IF NOT EXISTS structured.microbe_isolate (
  microbe_id TEXT PRIMARY KEY,
  original_code TEXT NULL,

  institution_isolation_conducted TEXT NULL,
  isolated_year INTEGER NULL,
  isolated_by TEXT NULL,

  maintained_by TEXT NULL,
  maintained_at TEXT NULL,

  kelp_host TEXT NULL,
  kelp_ka_sample_id TEXT NULL,
  source_if_ka_id TEXT NULL,
  source_if_no_ka_id TEXT NULL,

  ingest_batch_id UUID NULL REFERENCES structured.ingest_batch(ingest_batch_id),
  source_filename TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS structured.microbe_collection_context (
  microbe_id TEXT PRIMARY KEY REFERENCES structured.microbe_isolate(microbe_id) ON DELETE CASCADE,

  kelp_location TEXT NULL,
  kelp_collection_temp_raw TEXT NULL,
  kelp_collection_temp_c NUMERIC NULL,

  collection_month TEXT NULL,
  collection_season TEXT NULL,

  thallus_collection TEXT NULL,
  collection_approach TEXT NULL,
  collection_method TEXT NULL
);

CREATE TABLE IF NOT EXISTS structured.microbe_isolation (
  microbe_id TEXT PRIMARY KEY REFERENCES structured.microbe_isolate(microbe_id) ON DELETE CASCADE,
  isolation_methods TEXT NULL,
  isolation_protocol TEXT NULL,
  isolation_media TEXT NULL
);

CREATE TABLE IF NOT EXISTS structured.microbe_storage_current (
  microbe_id TEXT PRIMARY KEY REFERENCES structured.microbe_isolate(microbe_id) ON DELETE CASCADE,

  location_stored1 TEXT NULL,
  location_1_temperature TEXT NULL,
  location_stored2 TEXT NULL,
  location_2_temperature TEXT NULL,

  cryopreservation_date DATE NULL,
  cryopreservation_date_raw TEXT NULL,

  cryo_storage_medium TEXT NULL,
  cryo_storage_preservative TEXT NULL,

  cryo_revival_tested BOOLEAN NULL,
  cryo_backups_created BOOLEAN NULL,

  cryopreservation_protocol TEXT NULL
);

CREATE TABLE IF NOT EXISTS structured.microbe_maldi (
  microbe_id TEXT PRIMARY KEY REFERENCES structured.microbe_isolate(microbe_id) ON DELETE CASCADE,
  malditof_procedure TEXT NULL,
  data_analysis_complete BOOLEAN NULL,
  high_quality_data TEXT NULL
);

CREATE TABLE IF NOT EXISTS structured.microbe_sequencing (
  microbe_id TEXT PRIMARY KEY REFERENCES structured.microbe_isolate(microbe_id) ON DELETE CASCADE,

  pcr_16s_completed BOOLEAN NULL,
  pcr_conducted_by TEXT NULL,

  sanger_sequencing_completed BOOLEAN NULL,
  sequencing_date DATE NULL,
  sequencing_date_raw TEXT NULL,

  primers_used TEXT NULL,
  sequencing_notes TEXT NULL,
  sequencing_conducted_by TEXT NULL,

  total_bp_length_after_trimming INTEGER NULL,

  closest_ncbi_blast_tax_id TEXT NULL,
  ncbi_blast_query_cover NUMERIC NULL,
  percent_identity NUMERIC NULL,
  accession TEXT NULL,

  taxonomy_kingdom TEXT NULL,

  sequence_16s TEXT NULL,
  sequence_its2 TEXT NULL
);

CREATE TABLE IF NOT EXISTS structured.microbe_traits (
  microbe_id TEXT PRIMARY KEY REFERENCES structured.microbe_isolate(microbe_id) ON DELETE CASCADE,

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

CREATE INDEX IF NOT EXISTS idx_microbe_host ON structured.microbe_isolate(kelp_host);
CREATE INDEX IF NOT EXISTS idx_microbe_maintained_at ON structured.microbe_isolate(maintained_at);

COMMIT;
