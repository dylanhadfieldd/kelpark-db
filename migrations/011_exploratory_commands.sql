-- ============================================================
-- DB_EXPLORATION_CHEATSHEET.sql
-- Kara DB exploration & "show the work" walkthrough
--
-- How to use:
--   - Run in psql
--   - Copy/paste sections as needed
--   - This is read-only (SELECTs + psql meta commands)
--
-- Goal:
--   Tell the story: raw -> typed -> structured -> dimensions -> relationships
-- ============================================================


-- ============================================================
-- 0) ORIENTATION: Where am I? What schemas exist?
-- ============================================================

-- psql meta commands (run in psql, not via drivers)
\conninfo
\l
\dn+

-- Tables by schema
\dt staging.*
\dt structured.*
-- Views layer (if/when you build them)
\dt ai_internal.*
\dt ai_public.*


-- ============================================================
-- 1) PIPELINE COUNTS: raw -> typed -> structured
-- Why: proves controlled transformations + MVP structured core
-- ============================================================

-- Kelps: raw vs typed vs structured
SELECT
  (SELECT count(*) FROM staging.kelps_raw)          AS kelps_raw,
  (SELECT count(*) FROM staging.kelps_typed)        AS kelps_typed,
  (SELECT count(*) FROM structured.kelp_sample)     AS kelps_structured;

-- Microbes: raw vs typed vs structured
SELECT
  (SELECT count(*) FROM staging.microbes_raw)       AS microbes_raw,
  (SELECT count(*) FROM staging.microbes_typed)     AS microbes_typed,
  (SELECT count(*) FROM structured.microbe_isolate) AS microbes_structured;


-- ============================================================
-- 2) TYPED PARSING COVERAGE (proof parsing worked)
-- Why: shows you converted messy strings into typed numeric/date fields
-- ============================================================

-- Kelps: temperature parse coverage
SELECT
  count(*) AS total,
  count(*) FILTER (WHERE storage_details_temperature_c IS NOT NULL) AS parsed_temp
FROM staging.kelps_typed;

-- Kelps: coordinate parse coverage (typed)
SELECT
  count(*) AS total,
  count(*) FILTER (WHERE sampling_metadata_latitude_dd IS NOT NULL
                AND sampling_metadata_longitude_dd IS NOT NULL) AS parsed_coords
FROM staging.kelps_typed;

-- Kelps: date parse coverage (typed)
SELECT
  count(*) FILTER (WHERE sampling_metadata_collection_date IS NOT NULL) AS parsed_collection_date,
  count(*) FILTER (WHERE sampling_metadata_isolation_date IS NOT NULL)  AS parsed_isolation_date,
  count(*) FILTER (WHERE sampling_metadata_deposit_date IS NOT NULL)    AS parsed_deposit_date
FROM staging.kelps_typed;

-- Microbes: typed date/number coverage
SELECT
  count(*) AS total,
  count(*) FILTER (WHERE cryopreservation_date IS NOT NULL) AS parsed_cryo_date,
  count(*) FILTER (WHERE sequencing_date IS NOT NULL)       AS parsed_seq_date,
  count(*) FILTER (WHERE percent_identity IS NOT NULL)      AS parsed_percent_identity
FROM staging.microbes_typed;


-- ============================================================
-- 3) STRUCTURED MVP COVERAGE CHECKS (FKs present?)
-- Why: shows structured tables are “wired” with dimensions
-- ============================================================

SELECT
  count(*) AS total_kelps,
  count(*) FILTER (WHERE kelp_taxonomy_id IS NOT NULL) AS with_taxonomy,
  count(*) FILTER (WHERE location_id IS NOT NULL)      AS with_location,
  count(*) FILTER (WHERE storage_id IS NOT NULL)       AS with_storage
FROM structured.kelp_sample;

SELECT
  count(*) AS total_microbes,
  count(*) FILTER (WHERE microbe_taxonomy_id IS NOT NULL) AS with_taxonomy
FROM structured.microbe_isolate;


-- ============================================================
-- 4) DIMENSIONS: cardinalities + “top” values
-- Why: proves dedupe + queryability for dashboards later
-- ============================================================

-- Taxonomy dims
SELECT count(*) AS kelp_taxa FROM structured.kelp_taxonomy_dim;
SELECT count(*) AS microbe_kingdoms FROM structured.microbe_taxonomy_dim;

-- Top kelp taxa by sample count
SELECT
  kt.genus,
  kt.species,
  count(*) AS n_samples
FROM structured.kelp_sample ks
JOIN structured.kelp_taxonomy_dim kt
  ON kt.kelp_taxonomy_id = ks.kelp_taxonomy_id
GROUP BY 1,2
ORDER BY n_samples DESC
LIMIT 15;

-- Storage dim size
SELECT count(*) AS storage_dim_rows FROM structured.storage_dim;

-- Storage: rollup by location (good for inventory dashboards)
SELECT
  sd.storage_location,
  count(*) AS samples
FROM structured.kelp_sample ks
JOIN structured.storage_dim sd
  ON sd.storage_id = ks.storage_id
GROUP BY 1
ORDER BY samples DESC
LIMIT 10;

-- Location dim: coordinate readiness (map prep)
SELECT
  count(*) AS locations_total,
  count(*) FILTER (WHERE latitude_dd IS NOT NULL AND longitude_dd IS NOT NULL) AS locations_with_coords
FROM structured.location_dim;


-- ============================================================
-- 5) RELATIONSHIPS: microbes <-> kelps and people attribution
-- Why: shows real relational modeling (many-to-many, role links)
-- ============================================================

-- Microbe-kelp link table coverage
SELECT count(*) AS microbe_kelp_links FROM structured.microbe_kelp_link;

-- What kelp_ka_sample_id has the most microbes?
SELECT
  kelp_ka_sample_id,
  count(*) AS n_microbes
FROM structured.microbe_kelp_link
GROUP BY 1
ORDER BY n_microbes DESC
LIMIT 10;

-- People dimension size
SELECT count(*) AS people FROM structured.person_dim;

-- Role link counts (microbes)
SELECT role, count(*) AS links
FROM structured.microbe_isolate_person_role
GROUP BY 1
ORDER BY links DESC;

-- Role link counts (kelps)
SELECT role, count(*) AS links
FROM structured.kelp_sample_person_role
GROUP BY 1
ORDER BY links DESC;

-- Top "isolated_by" contributors (microbes)
SELECT
  pd.person_name_raw,
  count(*) AS n
FROM structured.microbe_isolate_person_role pr
JOIN structured.person_dim pd
  ON pd.person_id = pr.person_id
WHERE pr.role = 'isolated_by'
GROUP BY 1
ORDER BY n DESC
LIMIT 20;

-- Top "collected" contributors (kelps)
SELECT
  pd.person_name_raw,
  count(*) AS n
FROM structured.kelp_sample_person_role pr
JOIN structured.person_dim pd
  ON pd.person_id = pr.person_id
WHERE pr.role = 'collected'
GROUP BY 1
ORDER BY n DESC
LIMIT 20;


-- ============================================================
-- 6) ONE “STORYTELLING” QUERY: samples with taxonomy + location + microbes + people
-- Why: a single table you can paste in a report to show integration
-- ============================================================

SELECT
  ks.kelp_sample_id,
  kt.genus,
  kt.species,
  ld.country,
  ld.collection_site,
  (ld.latitude_dd IS NOT NULL AND ld.longitude_dd IS NOT NULL) AS has_coords,
  count(DISTINCT mk.microbe_isolate_id) AS microbes_linked,
  count(DISTINCT kpr.person_id) AS people_involved
FROM structured.kelp_sample ks
LEFT JOIN structured.kelp_taxonomy_dim kt
  ON kt.kelp_taxonomy_id = ks.kelp_taxonomy_id
LEFT JOIN structured.location_dim ld
  ON ld.location_id = ks.location_id
LEFT JOIN structured.microbe_kelp_link mk
  ON mk.kelp_sample_id = ks.kelp_sample_id
LEFT JOIN structured.kelp_sample_person_role kpr
  ON kpr.kelp_sample_id = ks.kelp_sample_id
GROUP BY 1,2,3,4,5,6
ORDER BY microbes_linked DESC, people_involved DESC
LIMIT 15;


-- ============================================================
-- 7) OPTIONAL: quick "spot-check" individual rows
-- Why: sanity check fields without scrolling huge tables
-- ============================================================

-- One microbe isolate with key fields
SELECT
  microbe_isolate_id,
  microbe_id,
  isolated_year,
  isolated_by,
  maintained_by,
  pcr_conducted_by,
  sequencing_conducted_by,
  percent_identity,
  accession
FROM structured.microbe_isolate
LIMIT 5;

-- One kelp sample with key fields
SELECT
  kelp_sample_id,
  staging_id,
  taxonomy_genus,
  taxonomy_species,
  sampling_metadata_country,
  sampling_metadata_collection_site,
  storage_details_location,
  storage_details_temperature_c
FROM structured.kelp_sample
LIMIT 5;

-- ============================================================
-- End of cheat sheet
-- ============================================================
