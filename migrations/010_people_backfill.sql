BEGIN;

-- -------------------------
-- A) Microbes: insert role links
-- -------------------------
WITH extracted AS (
  SELECT
    mi.microbe_isolate_id,
    v.role,
    trim(tok) AS person_name_raw,
    lower(regexp_replace(trim(tok), '\s+', ' ', 'g')) AS person_name_norm
  FROM structured.microbe_isolate mi
  CROSS JOIN LATERAL (
    VALUES
      ('isolated_by'::text, mi.isolated_by),
      ('maintained_by'::text, mi.maintained_by),
      ('pcr_conducted_by'::text, mi.pcr_conducted_by),
      ('sequencing_conducted_by'::text, mi.sequencing_conducted_by)
  ) v(role, fieldval)
  CROSS JOIN LATERAL regexp_split_to_table(
    regexp_replace(
      regexp_replace(coalesce(v.fieldval,''), '[\r\n\t]+', ' ', 'g'),
      '\s*(and|&)\s*', ',', 'gi'
    ),
    '[,;/]+'
  ) AS tok
  WHERE NULLIF(regexp_replace(coalesce(v.fieldval,''), '\s+', '', 'g'), '') IS NOT NULL
),
clean AS (
  SELECT *
  FROM extracted
  WHERE NULLIF(regexp_replace(coalesce(person_name_raw,''), '\s+', '', 'g'), '') IS NOT NULL
    AND person_name_norm NOT IN ('na','n/a','none','null','unknown','-')
)
INSERT INTO structured.microbe_isolate_person_role (microbe_isolate_id, person_id, role)
SELECT
  c.microbe_isolate_id,
  pd.person_id,
  c.role
FROM clean c
JOIN structured.person_dim pd
  ON pd.person_name_norm = c.person_name_norm
ON CONFLICT DO NOTHING;

-- -------------------------
-- B) Kelps: insert role links (if kelp fields exist/populated)
-- -------------------------
WITH extracted AS (
  SELECT
    ks.kelp_sample_id,
    v.role,
    trim(tok) AS person_name_raw,
    lower(regexp_replace(trim(tok), '\s+', ' ', 'g')) AS person_name_norm
  FROM structured.kelp_sample ks
  CROSS JOIN LATERAL (
    VALUES
      ('collected'::text, ks.sampling_metadata_personnel_collected),
      ('deposited_by'::text, ks.sampling_metadata_deposited_by)
  ) v(role, fieldval)
  CROSS JOIN LATERAL regexp_split_to_table(
    regexp_replace(
      regexp_replace(coalesce(v.fieldval,''), '[\r\n\t]+', ' ', 'g'),
      '\s*(and|&)\s*', ',', 'gi'
    ),
    '[,;/]+'
  ) AS tok
  WHERE NULLIF(regexp_replace(coalesce(v.fieldval,''), '\s+', '', 'g'), '') IS NOT NULL
),
clean AS (
  SELECT *
  FROM extracted
  WHERE NULLIF(regexp_replace(coalesce(person_name_raw,''), '\s+', '', 'g'), '') IS NOT NULL
    AND person_name_norm NOT IN ('na','n/a','none','null','unknown','-')
)
INSERT INTO structured.kelp_sample_person_role (kelp_sample_id, person_id, role)
SELECT
  c.kelp_sample_id,
  pd.person_id,
  c.role
FROM clean c
JOIN structured.person_dim pd
  ON pd.person_name_norm = c.person_name_norm
ON CONFLICT DO NOTHING;

COMMIT;
