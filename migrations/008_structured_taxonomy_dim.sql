BEGIN;

-- ============================================================
-- 008_structured_taxonomy_dim.sql
-- Taxonomy dimensions (kelps + microbes) + FK backfills
-- ============================================================

-- --------------------------------------------
-- 1) Kelps taxonomy dim
-- --------------------------------------------

ALTER TABLE structured.kelp_sample
  ADD COLUMN IF NOT EXISTS kelp_taxonomy_id UUID NULL;

DROP TABLE IF EXISTS structured.kelp_taxonomy_dim CASCADE;

CREATE TABLE structured.kelp_taxonomy_dim (
  kelp_taxonomy_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  genus TEXT NULL,
  species TEXT NULL,
  sex TEXT NULL,
  variety_or_form TEXT NULL,

  -- normalized dedupe key
  dedupe_key TEXT NOT NULL
);

INSERT INTO structured.kelp_taxonomy_dim (
  genus, species, sex, variety_or_form, dedupe_key
)
SELECT DISTINCT
  NULLIF(btrim(taxonomy_genus), '') AS genus,
  NULLIF(btrim(taxonomy_species), '') AS species,
  NULLIF(btrim(taxonomy_sex), '') AS sex,
  NULLIF(btrim(taxonomy_variety_or_form), '') AS variety_or_form,
  (
    COALESCE(lower(btrim(taxonomy_genus)), '') || '|' ||
    COALESCE(lower(btrim(taxonomy_species)), '') || '|' ||
    COALESCE(lower(regexp_replace(btrim(taxonomy_sex), '\s+', ' ', 'g')), '') || '|' ||
    COALESCE(lower(regexp_replace(btrim(taxonomy_variety_or_form), '\s+', ' ', 'g')), '')
  ) AS dedupe_key
FROM structured.kelp_sample;

-- Backfill kelp_sample.kelp_taxonomy_id
WITH ks_keys AS (
  SELECT
    kelp_sample_id,
    (
      COALESCE(lower(btrim(taxonomy_genus)), '') || '|' ||
      COALESCE(lower(btrim(taxonomy_species)), '') || '|' ||
      COALESCE(lower(regexp_replace(btrim(taxonomy_sex), '\s+', ' ', 'g')), '') || '|' ||
      COALESCE(lower(regexp_replace(btrim(taxonomy_variety_or_form), '\s+', ' ', 'g')), '')
    ) AS dedupe_key
  FROM structured.kelp_sample
)
UPDATE structured.kelp_sample ks
SET kelp_taxonomy_id = td.kelp_taxonomy_id
FROM ks_keys k
JOIN structured.kelp_taxonomy_dim td
  ON td.dedupe_key = k.dedupe_key
WHERE ks.kelp_sample_id = k.kelp_sample_id;

ALTER TABLE structured.kelp_sample
  ADD CONSTRAINT fk_kelp_sample_taxonomy
  FOREIGN KEY (kelp_taxonomy_id)
  REFERENCES structured.kelp_taxonomy_dim(kelp_taxonomy_id)
  ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_kelp_taxonomy_dim_genus_species
  ON structured.kelp_taxonomy_dim(genus, species);

CREATE INDEX IF NOT EXISTS idx_kelp_sample_taxonomy_id
  ON structured.kelp_sample(kelp_taxonomy_id);

-- --------------------------------------------
-- 2) Microbes taxonomy dim (kingdom)
-- --------------------------------------------

ALTER TABLE structured.microbe_isolate
  ADD COLUMN IF NOT EXISTS microbe_taxonomy_id UUID NULL;

DROP TABLE IF EXISTS structured.microbe_taxonomy_dim CASCADE;

CREATE TABLE structured.microbe_taxonomy_dim (
  microbe_taxonomy_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  taxonomy_kingdom TEXT NULL,
  dedupe_key TEXT NOT NULL
);

INSERT INTO structured.microbe_taxonomy_dim (taxonomy_kingdom, dedupe_key)
SELECT DISTINCT
  NULLIF(btrim(taxonomy_kingdom), '') AS taxonomy_kingdom,
  COALESCE(lower(btrim(taxonomy_kingdom)), '') AS dedupe_key
FROM structured.microbe_isolate;

WITH mi_keys AS (
  SELECT
    microbe_isolate_id,
    COALESCE(lower(btrim(taxonomy_kingdom)), '') AS dedupe_key
  FROM structured.microbe_isolate
)
UPDATE structured.microbe_isolate mi
SET microbe_taxonomy_id = td.microbe_taxonomy_id
FROM mi_keys k
JOIN structured.microbe_taxonomy_dim td
  ON td.dedupe_key = k.dedupe_key
WHERE mi.microbe_isolate_id = k.microbe_isolate_id;

ALTER TABLE structured.microbe_isolate
  ADD CONSTRAINT fk_microbe_isolate_taxonomy
  FOREIGN KEY (microbe_taxonomy_id)
  REFERENCES structured.microbe_taxonomy_dim(microbe_taxonomy_id)
  ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_microbe_taxonomy_dim_kingdom
  ON structured.microbe_taxonomy_dim(taxonomy_kingdom);

CREATE INDEX IF NOT EXISTS idx_microbe_isolate_taxonomy_id
  ON structured.microbe_isolate(microbe_taxonomy_id);

COMMIT;
