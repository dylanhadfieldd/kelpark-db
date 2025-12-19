-- ============================================================
-- 002_roles_and_grants.sql
-- Kara Platform – Roles & Permissions
-- ============================================================

BEGIN;

-- ============================================================
-- Roles
-- ============================================================

-- Admin (you) – already exists, assumed superuser
-- kelpark_admin

-- Internal full-access (read-only)
CREATE ROLE internal_user NOINHERIT;

-- Web-facing user roles (read-only)
CREATE ROLE nongenresearch_user NOINHERIT;
CREATE ROLE genresearch_user     NOINHERIT;
CREATE ROLE restoration_user     NOINHERIT;
CREATE ROLE farmbreed_user       NOINHERIT;
CREATE ROLE public_user          NOINHERIT;

-- Backend application role
CREATE ROLE kara_app NOINHERIT;

-- ============================================================
-- Revoke dangerous defaults
-- ============================================================

-- Lock down public schema
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- ============================================================
-- Schema USAGE grants
-- ============================================================

-- Everyone needs usage on their schemas
GRANT USAGE ON SCHEMA kara_public TO
  internal_user,
  nongenresearch_user,
  genresearch_user,
  restoration_user,
  farmbreed_user,
  public_user,
  kara_app;

-- Internal-only vector/document schema
GRANT USAGE ON SCHEMA kara_internal TO
  internal_user,
  kara_app;

-- Structured data
GRANT USAGE ON SCHEMA structured TO
  internal_user;

-- View schemas (role-specific)
GRANT USAGE ON SCHEMA nongenresearch TO nongenresearch_user;
GRANT USAGE ON SCHEMA genresearch     TO genresearch_user;
GRANT USAGE ON SCHEMA restoration     TO restoration_user;
GRANT USAGE ON SCHEMA farmbreed       TO farmbreed_user;
GRANT USAGE ON SCHEMA public_structured TO public_user;

-- Internal can see everything
GRANT USAGE ON SCHEMA
  nongenresearch,
  genresearch,
  restoration,
  farmbreed,
  public_structured
TO internal_user;

-- App can see all view schemas
GRANT USAGE ON SCHEMA
  nongenresearch,
  genresearch,
  restoration,
  farmbreed,
  public_structured
TO kara_app;

-- ============================================================
-- SELECT privileges (tables & views)
-- ============================================================

-- Public document embeddings
GRANT SELECT ON ALL TABLES IN SCHEMA kara_public TO
  internal_user,
  nongenresearch_user,
  genresearch_user,
  restoration_user,
  farmbreed_user,
  public_user,
  kara_app;

-- Internal document embeddings
GRANT SELECT ON ALL TABLES IN SCHEMA kara_internal TO
  internal_user,
  kara_app;

-- Structured base tables (read-only)
GRANT SELECT ON ALL TABLES IN SCHEMA structured TO
  internal_user;

-- Role-based view schemas
GRANT SELECT ON ALL TABLES IN SCHEMA nongenresearch TO nongenresearch_user;
GRANT SELECT ON ALL TABLES IN SCHEMA genresearch     TO genresearch_user;
GRANT SELECT ON ALL TABLES IN SCHEMA restoration     TO restoration_user;
GRANT SELECT ON ALL TABLES IN SCHEMA farmbreed       TO farmbreed_user;
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
-- Write access (admin only)
-- ============================================================

-- Explicitly revoke writes everywhere else
REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA
  structured,
  staging,
  vector_store
FROM PUBLIC;

-- Admin retains full rights implicitly

COMMIT;
