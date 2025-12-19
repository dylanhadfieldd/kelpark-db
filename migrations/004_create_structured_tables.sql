-- ============================================================
-- 004_create_tables_staging.sql
-- Kara Platform – Staging Tables (Raw CSV Landing)
--
-- Goal: 1:1 landing tables that preserve the source data.
-- Everything is TEXT (MVP). We'll type/clean into structured later.
-- ============================================================

BEGIN;

-- ============================================================
-- staging.kelp_raw
-- Primary key in structured will be Storage_Details_ID
-- Staging has no PK (raw landing).
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.kelp_raw (
  -- Ingestion metadata
  ingest_batch_id UUID NULL,
  source_filename TEXT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- CSV columns (raw)
  Taxonomy_Genus TEXT NULL,
  Taxonomy_Species TEXT NULL,
  Taxonomy_Sex TEXT NULL,
  Taxonomy_Variety_or_Form TEXT NULL,

  Storage_Details_ID TEXT NULL,
  Storage_Details_Position_ID TEXT NULL,
  Storage_Details_Rack_ID TEXT NULL,
  Storage_Details_Location TEXT NULL,
  Storage_Details_Temperature_C TEXT NULL,
  Storage_Details_Medium TEXT NULL,

  Sampling_Metadata_Country TEXT NULL,
  Sampling_Metadata_Latitude TEXT NULL,
  Sampling_Metadata_Longitude TEXT NULL,
  Sampling_Metadata_Collection_Date TEXT NULL,
  Sampling_Metadata_Personnel_Collected TEXT NULL,
  Sampling_Metadata_Isolation_Date TEXT NULL,
  Sampling_Metadata_Deposit_Date TEXT NULL,
  Sampling_Metadata_Deposited_By TEXT NULL,
  Sampling_Metadata_Permit TEXT NULL,
  Sampling_Metadata_Collection_Site TEXT NULL,

  Other_Previously_Housed_Location TEXT NULL,

  Sponsorship_Strain_Sponsorship_Status TEXT NULL,
  Sponsorship_Code TEXT NULL,

  Phenotypic_Data_Growth_Rate TEXT NULL,
  Phenotypic_Data_Optimal_Growth_Conditions TEXT NULL,
  Phenotypic_Data_Percent_Viability TEXT NULL,
  Phenotypic_Data_Lifespan TEXT NULL,
  Phenotypic_Data_Tolerance_to_Thermal_Stressor TEXT NULL,
  Phenotypic_Data_Tolerance_to_Water_Quality_Stressors TEXT NULL,

  iNaturalist TEXT NULL,

  Ecological_Role_Primary_Producer TEXT NULL,
  Ecological_Role_Carbon_Sink TEXT NULL,
  Ecological_Role_Habitat_Former TEXT NULL,

  Metabolic_Pathways_KEGG_Pathway_ID TEXT NULL,
  Metabolic_Pathways_MetaCyc_Pathway_ID TEXT NULL,

  Functional_Annotation_Gene_Function_ID TEXT NULL,
  Functional_Annotation_Protein_Function_ID TEXT NULL,

  Genetic_Variation_Data_Variant_ID TEXT NULL,
  Genetic_Variation_Data_Gene_ID TEXT NULL,
  Genetic_Variation_Data_Chromosome TEXT NULL,
  Genetic_Variation_Data_Reference_Allele TEXT NULL,
  Genetic_Variation_Data_Alternate_Allele TEXT NULL,
  Genetic_Variation_Data_Variant_Type TEXT NULL,
  Genetic_Variation_Data_Allele_Frequency TEXT NULL,
  Genetic_Variation_Data_Read_Depth TEXT NULL,
  Genetic_Variation_Data_Quality_Score TEXT NULL,
  Genetic_Variation_Data_Genotype TEXT NULL,

  Genetic_Diversity_Within_Geography_Sample_Sets_FST TEXT NULL,
  Genetic_Diversity_Within_Geography_Sample_Sets_Observed_Heterozygosity TEXT NULL,
  Genetic_Diversity_Within_Geography_Sample_Sets_Observed_Homozygosity TEXT NULL,
  Genetic_Diversity_Within_Geography_Sample_Sets_Allele_Count TEXT NULL,
  Genetic_Diversity_Within_Geography_Sample_Sets_Nucleotide_Diversity TEXT NULL,

  Phenotypic_Diversity_Within_Geography_Sample_Sets_Trait_ID_Name TEXT NULL,
  Phenotypic_Diversity_Within_Geography_Sample_Sets_Trait_Variance TEXT NULL,
  Phenotypic_Diversity_Within_Geography_Sample_Sets_Trait_Mean TEXT NULL,
  Phenotypic_Diversity_Within_Geography_Sample_Sets_Trait_Standard_Deviation TEXT NULL,
  Phenotypic_Diversity_Within_Geography_Sample_Sets_Trait_Range TEXT NULL,

  IUCN_Red_List_Extinct_EX TEXT NULL,
  IUCN_Red_List_Extinct_In_The_Wild_EW TEXT NULL,
  IUCN_Red_List_Critically_Endangered_CR TEXT NULL,
  IUCN_Red_List_Endangered_EN TEXT NULL,
  IUCN_Red_List_Vulnerable_VU TEXT NULL,
  IUCN_Red_List_Least_Concern TEXT NULL,
  IUCN_Red_List_Data_Deficient_DD TEXT NULL,
  IUCN_Red_List_Not_Evaluated_NE TEXT NULL,

  Ecosystem_Endemic TEXT NULL,
  Ecosystem_Naturalized TEXT NULL,
  Ecosystem_Invasive TEXT NULL,
  Ecosystem_Adventive TEXT NULL,
  Ecosystem_Extirpated TEXT NULL,
  Ecosystem_Weed TEXT NULL,
  Ecosystem_Cultivated_Horticultural TEXT NULL,
  Ecosystem_Ruderal TEXT NULL,
  Ecosystem_Pioneer TEXT NULL,

  Commercial_Bio_Variables_Harvestable_Yield_Per_Cycle TEXT NULL,
  Commercial_Bio_Variables_Harvest_Season TEXT NULL,
  Commercial_Bio_Variables_Light_Needed TEXT NULL,

  Commercial_Production_Spoilage_Rate TEXT NULL,
  Commercial_Production_Operational_Cost_Per_Cycle TEXT NULL,
  Commercial_Production_Gross_Margin TEXT NULL,

  Commercial_Market_Price_Volatility_Index TEXT NULL,
  Commercial_Market_Demand_Index_Sector TEXT NULL,
  Commercial_Market_Market_Growth_Rate TEXT NULL,

  Commercial_Processing_Moisture_Content TEXT NULL,
  Commercial_Processing_Protein_Content TEXT NULL,
  Commercial_Processing_Alginate_or_Carrageenan_Content TEXT NULL,
  Commercial_Processing_Contaminants TEXT NULL,
  Commercial_Processing_Shelf_Life TEXT NULL,
  Commercial_Processing_Grade_Quality_Score TEXT NULL,

  Commercial_Supply_Logistics_Transport_Cost TEXT NULL,
  Commercial_Supply_Logistics_Distribution_Channel TEXT NULL,
  Commercial_Supply_Logistics_Carbon_Footprint_Transport TEXT NULL
);

-- Helpful index for later transforms into structured
CREATE INDEX IF NOT EXISTS idx_kelp_raw_storage_details_id
  ON staging.kelp_raw (Storage_Details_ID);

-- ============================================================
-- staging.microbe_raw
-- Primary key in structured will be Microbe_ID
-- Note: identifiers like 16S_* must be quoted in SQL.
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.microbe_raw (
  -- Ingestion metadata
  ingest_batch_id UUID NULL,
  source_filename TEXT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- CSV columns (raw)
  Microbe_ID TEXT NULL,
  Original_Code TEXT NULL,
  Institution_Isolation_Physically_Conducted TEXT NULL,
  Isolated_Year TEXT NULL,
  Isolated_by TEXT NULL,
  Maintained_by TEXT NULL,
  Maintained_at TEXT NULL,
  Kelp_Host TEXT NULL,
  Kelp_KA_Sample_ID TEXT NULL,
  Source_if_KA_ID TEXT NULL,
  Source_If_no_KA_ID TEXT NULL,
  Kelp_Location TEXT NULL,
  Kelp_Collection_Temp TEXT NULL,
  Kelp_Collection_Month TEXT NULL,
  Kelp_Collection_Season TEXT NULL,
  Kelp_thallus_collection TEXT NULL,
  Kelp_Collection_Approach TEXT NULL,
  Kelp_Collection_method TEXT NULL,
  Microbe_Isolation_Methods TEXT NULL,
  MIcrobe_Isolation_Protocol TEXT NULL,
  Isolation_Media TEXT NULL,
  Location_Stored1 TEXT NULL,
  Location_1_Temperature TEXT NULL,
  Location_Stored2 TEXT NULL,
  Location_2_Temperature TEXT NULL,
  Cryopreservation_Date TEXT NULL,
  Cryo_Storage_Medium TEXT NULL,
  Cryo_Storage_Preservative TEXT NULL,
  Cryo_Revival_Tested TEXT NULL,
  Cryo_backups_created TEXT NULL,
  Cryopreservation_protocol TEXT NULL,
  MALDITOF_Procedure TEXT NULL,
  MALDITOF_DataAnalysis_Complete TEXT NULL,
  High_Quality_MALDITOF_Data TEXT NULL,

  "16S_PCR_Completed" TEXT NULL,
  PCR_Conducted_By TEXT NULL,
  Sanger_Sequencing_Completed TEXT NULL,
  Sequencing_Date TEXT NULL,
  Primers_Used TEXT NULL,
  Sequencing_Notes TEXT NULL,
  Sequencing_conducted_by TEXT NULL,
  Total_BP_Length_After_Trimming TEXT NULL,
  Closest_NCBI_BLAST_Tax_ID TEXT NULL,
  NCBI_BLAST_Query_Cover TEXT NULL,
  Percent_Identity TEXT NULL,
  Accession TEXT NULL,
  Taxonomy_Kingdom TEXT NULL,

  "16S_Sequence" TEXT NULL,
  ITS2_Sequence TEXT NULL,

  Pathogen_Activity_Kelp TEXT NULL,
  Pathogen_Activity_Humans TEXT NULL,
  Pathogen_Activity_Plants TEXT NULL,
  Pathogen_Activity_Animals TEXT NULL,

  Growth_Temperature_C_range TEXT NULL,
  Growth_salinity_range TEXT NULL,
  Growth_pH_range TEXT NULL,
  Growth_Optimal_Media TEXT NULL,

  Morphology_colony_color TEXT NULL,
  Morphology_colony_size TEXT NULL,
  Morphology_colony_shape TEXT NULL,
  Morphology_colony_texture TEXT NULL,
  Gram_Stain TEXT NULL,
  Morphology_cell_shape TEXT NULL,

  Probiotic_Activity TEXT NULL,
  Probiotic_Known_Host TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_microbe_raw_microbe_id
  ON staging.microbe_raw (Microbe_ID);

COMMIT;
