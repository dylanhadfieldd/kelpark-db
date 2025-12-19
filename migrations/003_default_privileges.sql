-- ============================================================
-- 003_default_privileges.sql
-- Kara Platform – Default Privileges for Future Objects
--
-- Why: GRANT ON ALL TABLES only affects existing tables.
-- This makes future tables/views automatically readable
-- by the intended roles without repeating GRANTs forever.
-- ============================================================

BEGIN;

-- Safety: ensure public doesn't get accidental access to new objects
ALTER DEFAULT PRIVILEGES FOR ROLE kelpark_admin
REVOKE ALL ON TABLES FROM PUBLIC;

ALTER DEFAULT PRIVILEGES FOR ROLE kelpark_admin
REVOKE ALL ON SEQUENCES FROM PUBLIC;

ALTER DEFAULT PRIVILEGES FOR ROLE kelpark_admin
REVOKE ALL ON FUNCTIONS FROM PUBLIC;

-- ============================================================
-- STAGING: admin-only (raw landing tables)
-- ============================================================
ALTER DEFAULT PRIVILEGES FOR ROLE kelpark_admin IN SCHEMA staging
REVOKE ALL ON TABLES FROM PUBLIC;

-- (Intentionally no GRANTs here — staging stays private to kelpark_admin)

-- ============================================================
-- STRUCTURED: canonical tables (MVP)
-- For MVP, allow internal_user + kara_app to read base tables.
-- Public/role access should go through views later.
-- ============================================================
ALTER DEFAULT PRIVILEGES FOR ROLE kelpark_admin IN SCHEMA structured
GRANT SELECT ON TABLES TO internal_user, kara_app;

ALTER DEFAULT PRIVILEGES FOR ROLE kelpark_admin IN SCHEMA structured
GRANT USAGE, SELECT ON SEQUENCES TO internal_user, kara_app;

-- ============================================================
-- VIEW SCHEMAS: when we create views later, they should be readable.
-- (Even though they're "tables" from a privileges standpoint.)
-- ============================================================

-- Public-facing views
ALTER DEFAULT PRIVILEGES FOR ROLE kelpark_admin IN SCHEMA public_structured
GRANT SELECT ON TABLES TO public_user, internal_user, kara_app;

-- Domain views
ALTER DEFAULT PRIVILEGES FOR ROLE kelpark_admin IN SCHEMA nongenresearch
GRANT SELECT ON TABLES TO nongenresearch_user, internal_user, kara_app;

ALTER DEFAULT PRIVILEGES FOR ROLE kelpark_admin IN SCHEMA restoration
GRANT SELECT ON TABLES TO restoration_user, internal_user, kara_app;

ALTER DEFAULT PRIVILEGES FOR ROLE kelpark_admin IN SCHEMA farmbreed
GRANT SELECT ON TABLES TO farmbreed_user, internal_user, kara_app;

ALTER DEFAULT PRIVILEGES FOR ROLE kelpark_admin IN SCHEMA genresearch
GRANT SELECT ON TABLES TO genresearch_user, internal_user, kara_app;

-- ============================================================
-- Kara doc schemas: leave conservative for now
-- (We can loosen once we create those tables and confirm exposure.)
-- ============================================================
ALTER DEFAULT PRIVILEGES FOR ROLE kelpark_admin IN SCHEMA kara_public
GRANT SELECT ON TABLES TO public_user, internal_user, kara_app;

ALTER DEFAULT PRIVILEGES FOR ROLE kelpark_admin IN SCHEMA kara_internal
GRANT SELECT ON TABLES TO internal_user, kara_app;

COMMIT;
