-- 006_seed_app_users.sql
BEGIN;

-- bcrypt hash support
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Upsert users and (re)set passwords + active flag
WITH upsert AS (
  INSERT INTO auth.user_account (user_id, username, password_hash, is_active)
  VALUES
    (gen_random_uuid(), 'dylan_hadfield',   crypt('dh-admin', gen_salt('bf')), true),
    (gen_random_uuid(), 'hayden_schneider', crypt('hs-opa',   gen_salt('bf')), true),
    (gen_random_uuid(), 'nina_noujdina',    crypt('nn-cfoo',  gen_salt('bf')), true)
  ON CONFLICT (username) DO UPDATE
    SET password_hash = EXCLUDED.password_hash,
        is_active     = true
  RETURNING user_id, username
)

-- 2) Ensure roles are present (idempotent)
INSERT INTO auth.user_role (user_id, role)
SELECT ua.user_id, v.role
FROM auth.user_account ua
JOIN (VALUES
  ('dylan_hadfield', 'admin'),
  ('hayden_schneider', 'internal'),
  ('nina_noujdina', 'internal')
) AS v(username, role)
  ON v.username = ua.username
ON CONFLICT DO NOTHING;

COMMIT;
