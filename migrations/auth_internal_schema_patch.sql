BEGIN;

-- -----------------------------
-- 1) AUTH TABLES (DB-backed users)
-- -----------------------------
CREATE SCHEMA IF NOT EXISTS auth;

CREATE TABLE IF NOT EXISTS auth.user_account (
  user_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  username      text NOT NULL UNIQUE,
  password_hash text NOT NULL,
  is_active     boolean NOT NULL DEFAULT true,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now()
);

-- keep updated_at fresh
CREATE OR REPLACE FUNCTION auth.set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_user_account_updated_at ON auth.user_account;
CREATE TRIGGER trg_user_account_updated_at
BEFORE UPDATE ON auth.user_account
FOR EACH ROW EXECUTE FUNCTION auth.set_updated_at();

-- user roles (simple multi-role support)
CREATE TABLE IF NOT EXISTS auth.user_role (
  user_id uuid NOT NULL REFERENCES auth.user_account(user_id) ON DELETE CASCADE,
  role    text NOT NULL,
  PRIMARY KEY (user_id, role)
);

-- -----------------------------
-- 2) VIEW SCHEMAS
-- -----------------------------
CREATE SCHEMA IF NOT EXISTS internal_structured;
CREATE SCHEMA IF NOT EXISTS admin_structured;

-- -----------------------------
-- 2A) INTERNAL VIEW (your approved internal field list)
--     Source of truth = structured.kelp_sample + joins to dims
-- -----------------------------
CREATE OR REPLACE VIEW internal_structured.kelp_catalog_plus AS
SELECT
  -- stable keys for UI
  ks.storage_id,
  ks.kelp_taxonomy_id,
  ks.location_id,

  -- taxonomy (internal + public)
  kt.genus  AS taxonomy_genus_dim,
  kt.species AS taxonomy_species_dim,
  kt.sex    AS taxonomy_sex_dim,
  kt.variety_or_form AS taxonomy_variety_or_form_dim,

  -- sampling metadata (internal + public)
  ks.sampling_metadata_country,
  ks.sampling_metadata_latitude_dd,
  ks.sampling_metadata_longitude_dd,
  ks.sampling_metadata_collection_date,
  ks.sampling_metadata_permit,
  ks.sampling_metadata_collection_site,

  -- sampling metadata (internal-only)
  ks.sampling_metadata_personnel_collected,
  ks.sampling_metadata_deposited_by,

  -- storage details (internal-only, but ID is also public)
  sd.storage_details_id AS storage_details_id_dim,
  sd.storage_location   AS storage_location_dim,
  sd.position_id        AS storage_position_id_dim,
  sd.rack_id            AS storage_rack_id_dim,
  sd.temperature_c      AS storage_temperature_c_dim,
  sd.medium             AS storage_medium_dim,

  -- sponsorship (internal + public)
  ks.sponsorship_strain_sponsorship_status,
  ks.sponsorship_code

FROM structured.kelp_sample ks
LEFT JOIN structured.kelp_taxonomy_dim kt ON kt.kelp_taxonomy_id = ks.kelp_taxonomy_id
LEFT JOIN structured.storage_dim sd        ON sd.storage_id = ks.storage_id
LEFT JOIN structured.location_dim ld       ON ld.location_id = ks.location_id;

-- -----------------------------
-- 2B) ADMIN VIEW (everything)
--     Full structured.kelp_sample columns + joined dim “_dim” columns
-- -----------------------------
CREATE OR REPLACE VIEW admin_structured.kelp_catalog_plus AS
SELECT
  -- base table: EVERYTHING
  ks.*,

  -- taxonomy dim expansions (avoid collisions with ks.taxonomy_* raw columns)
  kt.genus AS taxonomy_genus_dim,
  kt.species AS taxonomy_species_dim,
  kt.sex AS taxonomy_sex_dim,
  kt.variety_or_form AS taxonomy_variety_or_form_dim,

  -- storage dim expansions
  sd.storage_details_id AS storage_details_id_dim,
  sd.position_id AS storage_details_position_id_dim,
  sd.rack_id AS storage_details_rack_id_dim,
  sd.storage_location AS storage_details_location_dim,
  sd.temperature_c AS storage_details_temperature_c_dim,
  sd.temperature_c_raw AS storage_details_temperature_c_raw_dim,
  sd.medium AS storage_details_medium_dim,

  -- location dim expansions
  ld.country AS location_country_dim,
  ld.collection_site AS location_collection_site_dim,
  ld.latitude_dd AS location_latitude_dd_dim,
  ld.longitude_dd AS location_longitude_dd_dim,
  ld.coord_status AS location_coord_status_dim,
  ld.coord_format AS location_coord_format_dim

FROM structured.kelp_sample ks
LEFT JOIN structured.kelp_taxonomy_dim kt ON kt.kelp_taxonomy_id = ks.kelp_taxonomy_id
LEFT JOIN structured.storage_dim sd        ON sd.storage_id = ks.storage_id
LEFT JOIN structured.location_dim ld       ON ld.location_id = ks.location_id;

COMMIT;




-- Example (replace with a real bcrypt hash):
-- INSERT INTO auth.user_account (username, password_hash) VALUES
-- ('alice', '$2b$12$REPLACE_ME_WITH_BCRYPT_HASH');

-- Assign a role:
-- INSERT INTO auth.user_role (user_id, role)
-- SELECT user_id, 'internal' FROM auth.user_account WHERE username='alice';
