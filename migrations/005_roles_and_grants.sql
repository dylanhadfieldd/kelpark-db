-- 005_roles_and_grants.sql
BEGIN;

-- Create roles (NOLOGIN by default; your app/user can inherit or be granted membership)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kara_public') THEN
    CREATE ROLE kara_public NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kara_internal') THEN
    CREATE ROLE kara_internal NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kara_restoration') THEN
    CREATE ROLE kara_restoration NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kara_genetic') THEN
    CREATE ROLE kara_genetic NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kara_farming') THEN
    CREATE ROLE kara_farming NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kara_nongenetic') THEN
    CREATE ROLE kara_nongenetic NOLOGIN;
  END IF;
END $$;

-- Revoke broad public access (optional hardening; adjust to your environment)
REVOKE ALL ON SCHEMA staging FROM PUBLIC;
REVOKE ALL ON SCHEMA structured FROM PUBLIC;
REVOKE ALL ON SCHEMA ai_public FROM PUBLIC;
REVOKE ALL ON SCHEMA ai_internal FROM PUBLIC;
REVOKE ALL ON SCHEMA ai_restoration FROM PUBLIC;
REVOKE ALL ON SCHEMA ai_genetic FROM PUBLIC;
REVOKE ALL ON SCHEMA ai_farming FROM PUBLIC;
REVOKE ALL ON SCHEMA ai_nongenetic FROM PUBLIC;

-- Allow schema usage on view schemas only
GRANT USAGE ON SCHEMA ai_public TO kara_public;
GRANT USAGE ON SCHEMA ai_internal TO kara_internal;
GRANT USAGE ON SCHEMA ai_restoration TO kara_restoration;
GRANT USAGE ON SCHEMA ai_genetic TO kara_genetic;
GRANT USAGE ON SCHEMA ai_farming TO kara_farming;
GRANT USAGE ON SCHEMA ai_nongenetic TO kara_nongenetic;

-- Grant SELECT on views (explicit, least privilege)
GRANT SELECT ON ALL TABLES IN SCHEMA ai_public TO kara_public;
GRANT SELECT ON ALL TABLES IN SCHEMA ai_internal TO kara_internal;
GRANT SELECT ON ALL TABLES IN SCHEMA ai_restoration TO kara_restoration;
GRANT SELECT ON ALL TABLES IN SCHEMA ai_genetic TO kara_genetic;
GRANT SELECT ON ALL TABLES IN SCHEMA ai_farming TO kara_farming;
GRANT SELECT ON ALL TABLES IN SCHEMA ai_nongenetic TO kara_nongenetic;

-- Ensure future views also get privileges automatically
ALTER DEFAULT PRIVILEGES IN SCHEMA ai_public GRANT SELECT ON TABLES TO kara_public;
ALTER DEFAULT PRIVILEGES IN SCHEMA ai_internal GRANT SELECT ON TABLES TO kara_internal;
ALTER DEFAULT PRIVILEGES IN SCHEMA ai_restoration GRANT SELECT ON TABLES TO kara_restoration;
ALTER DEFAULT PRIVILEGES IN SCHEMA ai_genetic GRANT SELECT ON TABLES TO kara_genetic;
ALTER DEFAULT PRIVILEGES IN SCHEMA ai_farming GRANT SELECT ON TABLES TO kara_farming;
ALTER DEFAULT PRIVILEGES IN SCHEMA ai_nongenetic GRANT SELECT ON TABLES TO kara_nongenetic;

COMMIT;
