

BEGIN;

-- ============================================================
-- 007_structured_location_dim_nospatial.sql
-- PostGIS-free location dimension
-- Coordinates remain map-ready via lat/lon
-- ============================================================

-- --------------------------------------------
-- 1) Add coord fields to kelp_sample (safe, repeatable)
-- --------------------------------------------

ALTER TABLE structured.kelp_sample
  ADD COLUMN IF NOT EXISTS location_id UUID NULL;

ALTER TABLE structured.kelp_sample
  ADD COLUMN IF NOT EXISTS coord_status TEXT NULL,   -- parsed | legacy | invalid | missing
  ADD COLUMN IF NOT EXISTS coord_format TEXT NULL;   -- dms | decimal | unknown

-- Set coord_status / coord_format
UPDATE structured.kelp_sample
SET
  coord_status = CASE
    WHEN sampling_metadata_latitude_dd IS NOT NULL
     AND sampling_metadata_longitude_dd IS NOT NULL
     AND sampling_metadata_latitude_dd BETWEEN -90 AND 90
     AND sampling_metadata_longitude_dd BETWEEN -180 AND 180
      THEN 'parsed'
    WHEN sampling_metadata_latitude_raw ILIKE 'Legacy%'
      OR sampling_metadata_longitude_raw ILIKE 'Legacy%'
      THEN 'legacy'
    WHEN sampling_metadata_latitude_dd IS NULL
      OR sampling_metadata_longitude_dd IS NULL
      THEN 'missing'
    ELSE 'invalid'
  END,
  coord_format = CASE
    WHEN sampling_metadata_latitude_raw ~ '°'
      OR sampling_metadata_longitude_raw ~ '°'
      THEN 'dms'
    WHEN sampling_metadata_latitude_raw ~ '^[+-]?\d+(\.\d+)?$'
      OR sampling_metadata_longitude_raw ~ '^[+-]?\d+(\.\d+)?$'
      THEN 'decimal'
    ELSE 'unknown'
  END;

-- --------------------------------------------
-- 2) Create location dimension (no PostGIS)
-- --------------------------------------------

DROP TABLE IF EXISTS structured.location_dim CASCADE;

CREATE TABLE structured.location_dim (
  location_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  country TEXT NULL,
  collection_site TEXT NULL,

  latitude_dd NUMERIC NULL,
  longitude_dd NUMERIC NULL,

  coord_status TEXT NOT NULL,   -- parsed | legacy | invalid | missing
  coord_format TEXT NULL,       -- dms | decimal | unknown

  -- stable-ish dedupe key
  dedupe_key TEXT NOT NULL
);

-- --------------------------------------------
-- 3) Populate location_dim (deduped)
-- --------------------------------------------

INSERT INTO structured.location_dim (
  country,
  collection_site,
  latitude_dd,
  longitude_dd,
  coord_status,
  coord_format,
  dedupe_key
)
SELECT DISTINCT
  ks.sampling_metadata_country,
  ks.sampling_metadata_collection_site,
  ks.sampling_metadata_latitude_dd,
  ks.sampling_metadata_longitude_dd,
  COALESCE(ks.coord_status, 'missing'),
  ks.coord_format,
  (
    COALESCE(lower(btrim(ks.sampling_metadata_country)), '') || '|' ||
    COALESCE(lower(regexp_replace(btrim(ks.sampling_metadata_collection_site), '\s+', ' ', 'g')), '') || '|' ||
    COALESCE(ks.sampling_metadata_latitude_dd::text, '') || '|' ||
    COALESCE(ks.sampling_metadata_longitude_dd::text, '') || '|' ||
    COALESCE(ks.coord_status, '')
  ) AS dedupe_key
FROM structured.kelp_sample ks;

-- --------------------------------------------
-- 4) Backfill kelp_sample.location_id
-- --------------------------------------------

WITH ks_keys AS (
  SELECT
    ks.kelp_sample_id,
    (
      COALESCE(lower(btrim(ks.sampling_metadata_country)), '') || '|' ||
      COALESCE(lower(regexp_replace(btrim(ks.sampling_metadata_collection_site), '\s+', ' ', 'g')), '') || '|' ||
      COALESCE(ks.sampling_metadata_latitude_dd::text, '') || '|' ||
      COALESCE(ks.sampling_metadata_longitude_dd::text, '') || '|' ||
      COALESCE(ks.coord_status, '')
    ) AS dedupe_key
  FROM structured.kelp_sample ks
)
UPDATE structured.kelp_sample ks
SET location_id = ld.location_id
FROM ks_keys k
JOIN structured.location_dim ld
  ON ld.dedupe_key = k.dedupe_key
WHERE ks.kelp_sample_id = k.kelp_sample_id;

-- --------------------------------------------
-- 5) Add FK + indexes
-- --------------------------------------------

ALTER TABLE structured.kelp_sample
  ADD CONSTRAINT fk_kelp_sample_location
  FOREIGN KEY (location_id)
  REFERENCES structured.location_dim(location_id)
  ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_location_dim_status
  ON structured.location_dim(coord_status);

CREATE INDEX IF NOT EXISTS idx_location_dim_country_site
  ON structured.location_dim(country, collection_site);

CREATE INDEX IF NOT EXISTS idx_location_dim_lat_lon
  ON structured.location_dim(latitude_dd, longitude_dd);

CREATE INDEX IF NOT EXISTS idx_kelp_sample_location_id
  ON structured.kelp_sample(location_id);

COMMIT;
