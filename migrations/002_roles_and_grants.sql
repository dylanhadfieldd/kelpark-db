-- ============================================================
-- 002_roles_and_grants.sql
-- Kara Platform – Roles & Permissions (Idempotent, View-Safe)
-- ============================================================

BEGIN;

-- ============================================================
-- Roles (idempotent)
-- ============================================================

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'internal_user') THEN
    CREATE ROLE internal_user NOINHERIT;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'nongenresearch_user') THEN
    CREATE ROLE nongenresearch_user NOINHERIT;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'genresearch_user') THEN
    CREATE ROLE genresearch_user NOINHERIT;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'restoration_user') THEN
    CREATE ROLE restoration_user NOINHERIT;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'farmbreed_user') THEN
    CREATE ROLE farmbreed_user NOINHERIT;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'public_user') THEN
    CREATE ROLE public_user NOINHERIT;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kara_app') THEN
    CREATE ROLE kara_app NOINHERIT;
  END IF;
END
$$;

-- ============================================================
-- Revoke dangerous defaults
-- ============================================================

REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- ============================================================
-- Schema USAGE grants
-- ============================================================

-- Vector search schemas
GRANT USAGE ON SCHEMA kara_public TO
  internal_user,
  nongenresearch_user,
  genresearch_user,
  restoration_user,
  farmbreed_user,
  public_user,
  kara_app;

GRANT USAGE ON SCHEMA kara_internal TO
  internal_user,
  kara_app;

-- Structured base schema (internal only)
GRANT USAGE ON SCHEMA structured TO internal_user;

-- Role-facing structured VIEW schemas
GRANT USAGE ON SCHEMA nongenresearch     TO nongenresearch_user;
GRANT USAGE ON SCHEMA genresearch        TO genresearch_user;
GRANT USAGE ON SCHEMA restoration        TO restoration_user;
GRANT USAGE ON SCHEMA farmbreed          TO farmbreed_user;
GRANT USAGE ON SCHEMA public_structured  TO public_user;

-- Internal + app can traverse all view schemas
GRANT USAGE ON SCHEMA
  nongenresearch,
  genresearch,
  restoration,
  farmbreed,
  public_structured
TO internal_user, kara_app;

-- ============================================================
-- SELECT privileges (existing tables/views)
-- ============================================================

-- Vector embeddings
GRANT SELECT ON ALL TABLES IN SCHEMA kara_public TO
  internal_user,
  nongenresearch_user,
  genresearch_user,
  restoration_user,
  farmbreed_user,
  public_user,
  kara_app;

GRANT SELECT ON ALL TABLES IN SCHEMA kara_internal TO
  internal_user,
  kara_app;

-- Structured base tables (internal only)
GRANT SELECT ON ALL TABLES IN SCHEMA structured TO internal_user;

-- Structured view schemas
GRANT SELECT ON ALL TABLES IN SCHEMA nongenresearch    TO nongenresearch_user;
GRANT SELECT ON ALL TABLES IN SCHEMA genresearch       TO genresearch_user;
GRANT SELECT ON ALL TABLES IN SCHEMA restoration       TO restoration_user;
GRANT SELECT ON ALL TABLES IN SCHEMA farmbreed         TO farmbreed_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public_structured TO public_user;

-- Internal + app can read all views
GRANT SELECT ON ALL TABLES IN SCHEMA
  nongenresearch,
  genresearch,
  restoration,
  farmbreed,
  public_structured
TO internal_user, kara_app;

-- ============================================================
-- Default privileges (CRITICAL – future-proofing)
-- ============================================================

-- Vector schemas
ALTER DEFAULT PRIVILEGES IN SCHEMA kara_public
  GRANT SELECT ON TABLES TO
    internal_user,
    nongenresearch_user,
    genresearch_user,
    restoration_user,
    farmbreed_user,
    public_user,
    kara_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA kara_internal
  GRANT SELECT ON TABLES TO internal_user, kara_app;

-- Structured base tables
ALTER DEFAULT PRIVILEGES IN SCHEMA structured
  GRANT SELECT ON TABLES TO internal_user;

-- View schemas
ALTER DEFAULT PRIVILEGES IN SCHEMA nongenresearch
  GRANT SELECT ON TABLES TO nongenresearch_user, internal_user, kara_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA genresearch
  GRANT SELECT ON TABLES TO genresearch_user, internal_user, kara_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA restoration
  GRANT SELECT ON TABLES TO restoration_user, internal_user, kara_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA farmbreed
  GRANT SELECT ON TABLES TO farmbreed_user, internal_user, kara_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA public_structured
  GRANT SELECT ON TABLES TO public_user, internal_user, kara_app;

-- ============================================================
-- Write access lockdown
-- ============================================================

REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA
  structured,
  staging,
  vector_store
FROM PUBLIC;

-- Admin (kelpark_admin) retains full rights implicitly

COMMIT;
