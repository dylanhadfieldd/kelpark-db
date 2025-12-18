-- 004_create_ai_views.sql
BEGIN;

-- =====================================================
-- KELP VIEWS
-- =====================================================

-- PUBLIC: taxonomy + ID + public phenotypes + sponsorship_status + iNaturalist + (optional) restoration-visible ecology fields
CREATE OR REPLACE VIEW ai_public.v_kelp_catalog AS
SELECT
  s.storage_details_id,
  s.genus,
  s.species,
  s.variety_or_form,
  s.sex,
  sp.sponsorship_status,
  s.inaturalist_url,

  -- Public phenotypes
  p.growth_rate,
  p.optimal_growth_conditions,
  p.viability_percent,
  p.lifespan,
  p.tolerance_thermal_stressors,
  p.tolerance_water_quality_stressors
FROM structured.kelp_specimen s
LEFT JOIN structured.kelp_sponsorship sp ON sp.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_phenotype_current p ON p.storage_details_id = s.storage_details_id;

-- INTERNAL: includes storage, personnel, deposited_by, sponsorship_code (internal)
CREATE OR REPLACE VIEW ai_internal.v_kelp_inventory AS
SELECT
  s.storage_details_id,
  s.genus,
  s.species,
  s.variety_or_form,
  s.sex,
  s.inaturalist_url,

  -- Sampling internal fields
  sm.country,
  sm.latitude_dd,
  sm.longitude_dd,
  sm.collection_date,
  sm.collection_date_raw,
  sm.collector_source,
  sm.deposited_by,
  sm.permit_type,
  sm.collection_site,

  -- Storage internal fields (current only)
  st.location_site,
  st.position_id,
  st.rack_id,
  st.medium,
  st.temperature_c_min,
  st.temperature_c_max,
  st.temperature_c_text,
  st.previously_housed_location,

  -- Sponsorship internal
  sp.sponsorship_status,
  sp.sponsorship_code
FROM structured.kelp_specimen s
LEFT JOIN structured.kelp_sampling sm ON sm.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_storage_current st ON st.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_sponsorship sp ON sp.storage_details_id = s.storage_details_id;

-- GENETIC: pathways + annotations + variants + diversity
CREATE OR REPLACE VIEW ai_genetic.v_kelp_genetics AS
SELECT
  s.storage_details_id,
  s.genus,
  s.species,

  -- Diversity metrics (per specimen)
  gdm.fst,
  gdm.observed_heterozygosity,
  gdm.observed_homozygosity,
  gdm.allele_count,
  gdm.nucleotide_diversity
FROM structured.kelp_specimen s
LEFT JOIN structured.kelp_genetic_diversity_metrics gdm
  ON gdm.storage_details_id = s.storage_details_id;

-- Detailed child tables are queried separately by the SQL tool when needed:
-- ai_genetic can also expose them via separate views for simplicity:

CREATE OR REPLACE VIEW ai_genetic.v_kelp_kegg_pathways AS
SELECT storage_details_id, kegg_pathway_id
FROM structured.kelp_kegg_pathway;

CREATE OR REPLACE VIEW ai_genetic.v_kelp_metacyc_pathways AS
SELECT storage_details_id, metacyc_pathway_id
FROM structured.kelp_metacyc_pathway;

CREATE OR REPLACE VIEW ai_genetic.v_kelp_gene_functions AS
SELECT storage_details_id, gene_function_id
FROM structured.kelp_gene_function;

CREATE OR REPLACE VIEW ai_genetic.v_kelp_protein_functions AS
SELECT storage_details_id, protein_function_id
FROM structured.kelp_protein_function;

CREATE OR REPLACE VIEW ai_genetic.v_kelp_variants AS
SELECT
  storage_details_id,
  variant_id, gene_id, chromosome,
  reference_allele, alternate_allele,
  variant_type, allele_frequency, read_depth, quality_score, genotype
FROM structured.kelp_variant;

-- RESTORATION: sampling geo/date + ecology + conservation flags + diversity
CREATE OR REPLACE VIEW ai_restoration.v_kelp_restoration AS
SELECT
  s.storage_details_id,
  s.genus,
  s.species,
  s.sex,

  sm.country,
  sm.latitude_dd,
  sm.longitude_dd,
  sm.collection_date,
  sm.collection_site,
  sm.permit_type,

  er.primary_producer,
  er.carbon_sink,
  er.habitat_former,

  -- IUCN status
  i.extinct_ex,
  i.extinct_in_wild_ew,
  i.critically_endangered_cr,
  i.endangered_en,
  i.vulnerable_vu,
  i.near_threatened_nt,
  i.least_concern_lc,
  i.data_deficient_dd,
  i.not_evaluated_ne,

  -- Ecosystem flags
  ef.endemic,
  ef.naturalized,
  ef.invasive,
  ef.adventive,
  ef.extirpated,
  ef.weed,
  ef.cultivated_horticultural,
  ef.ruderal,
  ef.pioneer_species,

  -- Diversity metrics
  gdm.fst,
  gdm.observed_heterozygosity,
  gdm.nucleotide_diversity
FROM structured.kelp_specimen s
LEFT JOIN structured.kelp_sampling sm ON sm.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_ecological_role er ON er.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_iucn_status i ON i.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_ecosystem_flags ef ON ef.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_genetic_diversity_metrics gdm ON gdm.storage_details_id = s.storage_details_id;

-- FARMING & BREEDING: phenotypes + commercial variables (+ taxonomy)
CREATE OR REPLACE VIEW ai_farming.v_kelp_farming AS
SELECT
  s.storage_details_id,
  s.genus,
  s.species,
  s.variety_or_form,
  s.sex,

  p.growth_rate,
  p.optimal_growth_conditions,
  p.viability_percent,
  p.lifespan,
  p.tolerance_thermal_stressors,
  p.tolerance_water_quality_stressors,

  cb.harvestable_yield_per_cycle,
  cb.harvest_season,
  cb.light_availability_needed,

  cp.spoilage_rate_percent,
  cp.operational_cost_per_cycle,
  cp.gross_margin_percent,

  cm.price_volatility_index,
  cm.demand_index_sector,
  cm.market_growth_rate_percent,

  cpr.moisture_content_percent,
  cpr.protein_content_percent,
  cpr.alginate_or_carrageenan_content_percent,
  cpr.contaminants,
  cpr.shelf_life,
  cpr.grade_quality_score,

  cl.transport_cost_per_kg,
  cl.distribution_channel,
  cl.carbon_footprint_transport
FROM structured.kelp_specimen s
LEFT JOIN structured.kelp_phenotype_current p ON p.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_commercial_bio cb ON cb.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_commercial_production cp ON cp.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_commercial_market cm ON cm.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_commercial_processing cpr ON cpr.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_commercial_logistics cl ON cl.storage_details_id = s.storage_details_id;

-- Non-genetic research: taxonomy + sampling + phenotype + ecology (no variants/sequences)
CREATE OR REPLACE VIEW ai_nongenetic.v_kelp_nongenetic AS
SELECT
  s.storage_details_id,
  s.genus, s.species, s.sex, s.variety_or_form,
  sm.country, sm.latitude_dd, sm.longitude_dd, sm.collection_date, sm.collection_site,
  p.growth_rate, p.optimal_growth_conditions, p.viability_percent, p.lifespan,
  er.primary_producer, er.carbon_sink, er.habitat_former,
  s.inaturalist_url
FROM structured.kelp_specimen s
LEFT JOIN structured.kelp_sampling sm ON sm.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_phenotype_current p ON p.storage_details_id = s.storage_details_id
LEFT JOIN structured.kelp_ecological_role er ON er.storage_details_id = s.storage_details_id;


-- =====================================================
-- MICROBE VIEWS (translated governance)
-- =====================================================

-- PUBLIC: isolate ID + host + collection context + non-sensitive summary (no storage location, no sequences by default)
CREATE OR REPLACE VIEW ai_public.v_microbe_catalog AS
SELECT
  m.microbe_id,
  m.original_code,
  m.kelp_host,
  cc.kelp_location,
  cc.kelp_collection_temp_c,
  cc.collection_month,
  cc.collection_season
FROM structured.microbe_isolate m
LEFT JOIN structured.microbe_collection_context cc ON cc.microbe_id = m.microbe_id;

-- INTERNAL: includes storage + protocols + maintenance
CREATE OR REPLACE VIEW ai_internal.v_microbe_inventory AS
SELECT
  m.microbe_id,
  m.original_code,
  m.institution_isolation_conducted,
  m.isolated_year,
  m.isolated_by,
  m.maintained_by,
  m.maintained_at,
  m.kelp_host,
  m.kelp_ka_sample_id,
  m.source_if_ka_id,
  m.source_if_no_ka_id,

  cc.kelp_location,
  cc.kelp_collection_temp_c,
  cc.collection_month,
  cc.collection_season,
  cc.thallus_collection,
  cc.collection_approach,
  cc.collection_method,

  iso.isolation_methods,
  iso.isolation_protocol,
  iso.isolation_media,

  st.location_stored1,
  st.location_1_temperature,
  st.location_stored2,
  st.location_2_temperature,
  st.cryopreservation_date,
  st.cryo_storage_medium,
  st.cryo_storage_preservative,
  st.cryo_revival_tested,
  st.cryo_backups_created,
  st.cryopreservation_protocol,

  mal.malditof_procedure,
  mal.data_analysis_complete,
  mal.high_quality_data
FROM structured.microbe_isolate m
LEFT JOIN structured.microbe_collection_context cc ON cc.microbe_id = m.microbe_id
LEFT JOIN structured.microbe_isolation iso ON iso.microbe_id = m.microbe_id
LEFT JOIN structured.microbe_storage_current st ON st.microbe_id = m.microbe_id
LEFT JOIN structured.microbe_maldi mal ON mal.microbe_id = m.microbe_id;

-- GENETIC: sequencing + BLAST + accession + sequences (restricted)
CREATE OR REPLACE VIEW ai_genetic.v_microbe_genetics AS
SELECT
  m.microbe_id,
  m.kelp_host,
  seq.taxonomy_kingdom,
  seq.closest_ncbi_blast_tax_id,
  seq.ncbi_blast_query_cover,
  seq.percent_identity,
  seq.accession,
  seq.sequence_16s,
  seq.sequence_its2,
  seq.sequencing_date
FROM structured.microbe_isolate m
LEFT JOIN structured.microbe_sequencing seq ON seq.microbe_id = m.microbe_id;

-- FARMING / RESTORATION / NONGENETIC equivalents (optional; start minimal)
CREATE OR REPLACE VIEW ai_nongenetic.v_microbe_nongenetic AS
SELECT
  m.microbe_id,
  m.kelp_host,
  cc.kelp_location,
  t.probiotic_activity,
  t.probiotic_known_host,
  t.growth_temperature_c_range,
  t.growth_salinity_range,
  t.growth_ph_range,
  t.growth_optimal_media
FROM structured.microbe_isolate m
LEFT JOIN structured.microbe_collection_context cc ON cc.microbe_id = m.microbe_id
LEFT JOIN structured.microbe_traits t ON t.microbe_id = m.microbe_id;

COMMIT;
