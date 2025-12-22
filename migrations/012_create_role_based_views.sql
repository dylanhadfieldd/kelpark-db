BEGIN;

-- ---------------------------------------------------------------------
-- 010_create_role_based_views.sql
-- Role-based view layer for structured CSV-ingested datasets
--
-- Goal:
--   - structured.* is the canonical data
--   - role-facing schemas contain VIEWS only
--   - permissions are granted only on schemas + views
--   - column exposure controlled by a policy table
--
-- You will:
--   1) Register base tables (kelps vs microbes)
--   2) Populate column policy rows (or generate them)
--   3) Run refresh_views() to build/replace views
-- ---------------------------------------------------------------------

-- -----------------------------
-- 0) Safety: ensure schemas exist
-- -----------------------------
CREATE SCHEMA IF NOT EXISTS public_structured;
CREATE SCHEMA IF NOT EXISTS nongenresearch;
CREATE SCHEMA IF NOT EXISTS genresearch;
CREATE SCHEMA IF NOT EXISTS restoration;
CREATE SCHEMA IF NOT EXISTS farmbreed;

-- -----------------------------
-- 1) Table registry: what tables belong to which dataset family?
-- -----------------------------
CREATE TABLE IF NOT EXISTS admin.view_base_table_registry (
  base_schema     TEXT NOT NULL,
  base_table      TEXT NOT NULL,
  dataset_family  TEXT NOT NULL CHECK (dataset_family IN ('kelps','microbes')),
  is_active       BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (base_schema, base_table)
);

COMMENT ON TABLE admin.view_base_table_registry IS
'Lists which structured tables should get role-facing views, and whether they are kelps or microbes.';

-- -----------------------------
-- 2) Column policy: which columns are exposed per role-domain?
--
-- IMPORTANT:
--  - For microbes: only internal/admin should see them.
--  - For kelps: columns exposed according to your use-case matrix.
-- -----------------------------
CREATE TABLE IF NOT EXISTS admin.view_column_policy (
  base_schema   TEXT NOT NULL,
  base_table    TEXT NOT NULL,
  column_name   TEXT NOT NULL,

  allow_public          BOOLEAN NOT NULL DEFAULT FALSE,
  allow_nongenresearch  BOOLEAN NOT NULL DEFAULT FALSE,
  allow_genresearch     BOOLEAN NOT NULL DEFAULT FALSE,
  allow_restoration     BOOLEAN NOT NULL DEFAULT FALSE,
  allow_farmbreed       BOOLEAN NOT NULL DEFAULT FALSE,
  allow_internal        BOOLEAN NOT NULL DEFAULT FALSE,  -- internal_user / admin

  notes TEXT NULL,

  PRIMARY KEY (base_schema, base_table, column_name),
  FOREIGN KEY (base_schema, base_table)
    REFERENCES admin.view_base_table_registry(base_schema, base_table)
    ON DELETE CASCADE
);

COMMENT ON TABLE admin.view_column_policy IS
'Column-level allowlist for each role-facing domain. Views are generated from this.';

-- -----------------------------
-- 3) Helper: ensure internal always has access to all columns on kelp tables,
--    and only internal has access to microbes unless you explicitly allow otherwise.
-- -----------------------------
CREATE OR REPLACE FUNCTION admin.ensure_default_policies()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  r RECORD;
  c RECORD;
BEGIN
  FOR r IN
    SELECT base_schema, base_table, dataset_family
    FROM admin.view_base_table_registry
    WHERE is_active
  LOOP
    FOR c IN
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = r.base_schema
        AND table_name = r.base_table
      ORDER BY ordinal_position
    LOOP
      INSERT INTO admin.view_column_policy (
        base_schema, base_table, column_name,
        allow_internal,
        allow_public, allow_nongenresearch, allow_genresearch, allow_restoration, allow_farmbreed,
        notes
      )
      VALUES (
        r.base_schema, r.base_table, c.column_name,
        TRUE,
        FALSE, FALSE, FALSE, FALSE, FALSE,
        CASE
          WHEN r.dataset_family = 'microbes' THEN 'DEFAULT: microbes restricted to internal/admin'
          ELSE 'DEFAULT: internal/admin allowed'
        END
      )
      ON CONFLICT (base_schema, base_table, column_name) DO NOTHING;
    END LOOP;
  END LOOP;
END;
$$;

-- -----------------------------
-- 4) Core: view builder
--    Creates one view per base table per domain schema.
--
-- Naming convention:
--   <domain_schema>.<base_table>  (same name as base table)
-- -----------------------------
CREATE OR REPLACE FUNCTION admin.refresh_role_views()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  t RECORD;

  -- domain loop
  domain TEXT;
  target_schema TEXT;

  col_list TEXT;
  sql TEXT;

  -- mapping domain -> policy flag + schema
  domains TEXT[] := ARRAY[
    'public',
    'nongenresearch',
    'genresearch',
    'restoration',
    'farmbreed'
  ];
BEGIN
  -- Ensure policy table has at least defaults for existing columns
  PERFORM admin.ensure_default_policies();

  FOR t IN
    SELECT base_schema, base_table, dataset_family
    FROM admin.view_base_table_registry
    WHERE is_active
    ORDER BY dataset_family, base_schema, base_table
  LOOP
    FOREACH domain IN ARRAY domains LOOP
      target_schema :=
        CASE domain
          WHEN 'public' THEN 'public_structured'
          WHEN 'nongenresearch' THEN 'nongenresearch'
          WHEN 'genresearch' THEN 'genresearch'
          WHEN 'restoration' THEN 'restoration'
          WHEN 'farmbreed' THEN 'farmbreed'
        END;

      -- Build allowed column list for this domain
      SELECT string_agg(format('%I', p.column_name), ', ' ORDER BY p.column_name)
      INTO col_list
      FROM admin.view_column_policy p
      WHERE p.base_schema = t.base_schema
        AND p.base_table  = t.base_table
        AND (
          (domain = 'public'         AND p.allow_public)
          OR (domain = 'nongenresearch' AND p.allow_nongenresearch)
          OR (domain = 'genresearch'    AND p.allow_genresearch)
          OR (domain = 'restoration'    AND p.allow_restoration)
          OR (domain = 'farmbreed'      AND p.allow_farmbreed)
        );

      -- If nothing allowed, skip creating the view (keeps schemas cleaner)
      IF col_list IS NULL THEN
        CONTINUE;
      END IF;

      sql := format(
        'CREATE OR REPLACE VIEW %I.%I AS SELECT %s FROM %I.%I;',
        target_schema, t.base_table, col_list, t.base_schema, t.base_table
      );

      EXECUTE sql;
    END LOOP;

    -- Optional internal schema view layer:
    -- If you want internal_user to query via a view schema instead of structured directly,
    -- uncomment this block and create schema "admin" or "kara_internal_structured" etc.
    --
    -- EXECUTE format('CREATE OR REPLACE VIEW admin.%I AS SELECT * FROM %I.%I;',
    --                t.base_table, t.base_schema, t.base_table);

  END LOOP;
END;
$$;

-- -----------------------------
-- 5) Permissions: lock down structured tables, expose only views
--
-- NOTE: tailor this to your existing grants. This is the safe default pattern.
-- -----------------------------

-- Revoke public access broadly (optional; depends on your current posture)
REVOKE ALL ON SCHEMA structured FROM PUBLIC;

-- Ensure roles can use their schemas
GRANT USAGE ON SCHEMA public_structured TO public_user;
GRANT USAGE ON SCHEMA nongenresearch TO nongenresearch_user;
GRANT USAGE ON SCHEMA genresearch TO genresearch_user;
GRANT USAGE ON SCHEMA restoration TO restoration_user;
GRANT USAGE ON SCHEMA farmbreed TO farmbreed_user;

-- Allow selecting from views in those schemas (views created by refresh function)
-- We grant after creation; you can re-run this block any time.
-- (Alternatively, set default privileges, but explicit grants are simpler early.)
DO $$
DECLARE
  v RECORD;
BEGIN
  FOR v IN
    SELECT table_schema, table_name
    FROM information_schema.views
    WHERE table_schema IN ('public_structured','nongenresearch','genresearch','restoration','farmbreed')
  LOOP
    IF v.table_schema = 'public_structured' THEN
      EXECUTE format('GRANT SELECT ON %I.%I TO public_user;', v.table_schema, v.table_name);
    ELSIF v.table_schema = 'nongenresearch' THEN
      EXECUTE format('GRANT SELECT ON %I.%I TO nongenresearch_user;', v.table_schema, v.table_name);
    ELSIF v.table_schema = 'genresearch' THEN
      EXECUTE format('GRANT SELECT ON %I.%I TO genresearch_user;', v.table_schema, v.table_name);
    ELSIF v.table_schema = 'restoration' THEN
      EXECUTE format('GRANT SELECT ON %I.%I TO restoration_user;', v.table_schema, v.table_name);
    ELSIF v.table_schema = 'farmbreed' THEN
      EXECUTE format('GRANT SELECT ON %I.%I TO farmbreed_user;', v.table_schema, v.table_name);
    END IF;
  END LOOP;
END$$;

COMMIT;

-- After you register tables + set policy flags, run:
--   SELECT admin.refresh_role_views();
--
-- Then re-run the GRANT DO block (or just re-run the whole file safely).
