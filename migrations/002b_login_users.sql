-- 005_login_users.sql (idempotent)
BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='dylan_hadfield') THEN
    CREATE ROLE dylan_hadfield LOGIN PASSWORD 'kelpark' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='hayden_schneider') THEN
    CREATE ROLE hayden_schneider LOGIN PASSWORD 'hayden' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='nina_noujdina') THEN
    CREATE ROLE nina_noujdina LOGIN PASSWORD 'nina' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
  END IF;
END $$;

GRANT kelpark_admin TO dylan_admin;
GRANT internal_user TO hayden_schneider;
GRANT internal_user TO nina_noujdina;

COMMIT;
