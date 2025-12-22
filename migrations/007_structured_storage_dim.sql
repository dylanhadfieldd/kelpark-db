BEGIN;

-- ============================================================
-- 007_structured_storage_dim.sql
-- Storage dimension for kelp samples (PostGIS-free)
--
-- Creates:
--   structured.storage_dim (deduped storage slots/contexts)
-- Adds:
--   structured.kelp_sample.storage_id FK
--   structured.kelp_sample.storage_status
-- ============================================================

-- --------------------------------------------
-- 1) Add storage fields to kelp_sample
-- --------------------------------------------

ALTER TABLE structured.kelp_sample
  ADD COLUMN IF NOT EXISTS storage_id UUID NULL;

ALTER TABLE structured.kelp_sample
  ADD COLUMN IF NOT EXISTS storage_status TEXT NULL; -- complete | partial | missing

-- Storage completeness tag (useful for dashboards)
UPDATE structured.kelp_sample
SET storage_status = CASE
  WHEN storage_details_location IS NOT NULL
   AND (storage_details_rack_id IS NOT NULL OR storage_details_position_id IS NOT NULL)
    THEN 'complete'
  WHEN storage_details_location IS NOT NULL
    OR storage_details_rack_id IS NOT NULL
    OR storage_details_position_id IS NOT NULL
    OR storage_details_id IS NOT NULL
    THEN 'partial'
  ELSE 'missing'
END;

-- --------------------------------------------
-- 2) Create storage_dim
-- --------------------------------------------

DROP TABLE IF EXISTS structured.storage_dim CASCADE;

CREATE TABLE structured.storage_dim (
  storage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  storage_details_id TEXT NULL,
  position_id TEXT NULL,
  rack_id TEXT NULL,
  storage_location TEXT NULL,

  temperature_c NUMERIC NULL,
  temperature_c_raw TEXT NULL,
  medium TEXT NULL,

  -- stable-ish dedupe key (not unique enforced because NULLs possible)
  dedupe_key TEXT NOT NULL
);

-- Populate storage_dim with dedupe key normalization
INSERT INTO structured.storage_dim (
  storage_details_id,
  position_id,
  rack_id,
  storage_location,
  temperature_c,
  temperature_c_raw,
  medium,
  dedupe_key
)
SELECT DISTINCT
  NULLIF(btrim(ks.storage_details_id), '') AS storage_details_id,
  NULLIF(btrim(ks.storage_details_position_id), '') AS position_id,
  NULLIF(btrim(ks.storage_details_rack_id), '') AS rack_id,
  NULLIF(btrim(ks.storage_details_location), '') AS storage_location,
  ks.storage_details_temperature_c AS temperature_c,
  NULLIF(btrim(ks.storage_details_temperature_c_raw), '') AS temperature_c_raw,
  NULLIF(btrim(ks.storage_details_medium), '') AS medium,
  (
    COALESCE(lower(btrim(ks.storage_details_id)), '') || '|' ||
    COALESCE(lower(btrim(ks.storage_details_position_id)), '') || '|' ||
    COALESCE(lower(btrim(ks.storage_details_rack_id)), '') || '|' ||
    COALESCE(lower(regexp_replace(btrim(ks.storage_details_location), '\s+', ' ', 'g')), '') || '|' ||
    COALESCE(ks.storage_details_temperature_c::text, '') || '|' ||
    COALESCE(lower(regexp_replace(btrim(ks.storage_details_medium), '\s+', ' ', 'g')), '')
  ) AS dedupe_key
FROM structured.kelp_sample ks
WHERE
  -- only make a storage_dim row if we have *some* storage signal
  COALESCE(
    NULLIF(btrim(ks.storage_details_id), ''),
    NULLIF(btrim(ks.storage_details_position_id), ''),
    NULLIF(btrim(ks.storage_details_rack_id), ''),
    NULLIF(btrim(ks.storage_details_location), ''),
    NULLIF(btrim(ks.storage_details_temperature_c_raw), ''),
    NULLIF(btrim(ks.storage_details_medium), '')
  ) IS NOT NULL;

-- --------------------------------------------
-- 3) Backfill kelp_sample.storage_id
-- --------------------------------------------

WITH ks_keys AS (
  SELECT
    ks.kelp_sample_id,
    (
      COALESCE(lower(btrim(ks.storage_details_id)), '') || '|' ||
      COALESCE(lower(btrim(ks.storage_details_position_id)), '') || '|' ||
      COALESCE(lower(btrim(ks.storage_details_rack_id)), '') || '|' ||
      COALESCE(lower(regexp_replace(btrim(ks.storage_details_location), '\s+', ' ', 'g')), '') || '|' ||
      COALESCE(ks.storage_details_temperature_c::text, '') || '|' ||
      COALESCE(lower(regexp_replace(btrim(ks.storage_details_medium), '\s+', ' ', 'g')), '')
    ) AS dedupe_key
  FROM structured.kelp_sample ks
)
UPDATE structured.kelp_sample ks
SET storage_id = sd.storage_id
FROM ks_keys k
JOIN structured.storage_dim sd
  ON sd.dedupe_key = k.dedupe_key
WHERE ks.kelp_sample_id = k.kelp_sample_id;

-- Add FK after backfill
ALTER TABLE structured.kelp_sample
  ADD CONSTRAINT fk_kelp_sample_storage
  FOREIGN KEY (storage_id)
  REFERENCES structured.storage_dim(storage_id)
  ON DELETE SET NULL;

-- --------------------------------------------
-- 4) Indexes for dashboard speed
-- --------------------------------------------

CREATE INDEX IF NOT EXISTS idx_storage_dim_location
  ON structured.storage_dim(storage_location);

CREATE INDEX IF NOT EXISTS idx_storage_dim_rack_pos
  ON structured.storage_dim(rack_id, position_id);

CREATE INDEX IF NOT EXISTS idx_kelp_sample_storage_id
  ON structured.kelp_sample(storage_id);

CREATE INDEX IF NOT EXISTS idx_kelp_sample_storage_status
  ON structured.kelp_sample(storage_status);

COMMIT;
