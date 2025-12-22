BEGIN;

DROP TABLE IF EXISTS structured.kelp_sample_person_role CASCADE;
DROP TABLE IF EXISTS structured.microbe_isolate_person_role CASCADE;
DROP TABLE IF EXISTS structured.person_dim CASCADE;

CREATE TABLE structured.person_dim (
  person_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  person_name_raw TEXT NOT NULL,
  person_name_norm TEXT NOT NULL
);

CREATE UNIQUE INDEX uq_person_dim_norm
  ON structured.person_dim(person_name_norm);

CREATE TABLE structured.kelp_sample_person_role (
  kelp_sample_id UUID NOT NULL
    REFERENCES structured.kelp_sample(kelp_sample_id) ON DELETE CASCADE,
  person_id UUID NOT NULL
    REFERENCES structured.person_dim(person_id) ON DELETE CASCADE,
  role TEXT NOT NULL,
  PRIMARY KEY (kelp_sample_id, person_id, role)
);

CREATE TABLE structured.microbe_isolate_person_role (
  microbe_isolate_id UUID NOT NULL
    REFERENCES structured.microbe_isolate(microbe_isolate_id) ON DELETE CASCADE,
  person_id UUID NOT NULL
    REFERENCES structured.person_dim(person_id) ON DELETE CASCADE,
  role TEXT NOT NULL,
  PRIMARY KEY (microbe_isolate_id, person_id, role)
);

-- =========================
-- MICROBE LINKS (source: structured.microbe_isolate)
-- =========================
WITH extracted AS (
  SELECT
    mi.microbe_isolate_id,
    'isolated_by'::text AS role,
    trim(tok) AS person_name_raw,
    lower(regexp_replace(trim(tok), '\s+', ' ', 'g')) AS person_name_norm
  FROM structured.microbe_isolate mi
  CROSS JOIN LATERAL regexp_split_to_table(
    regexp_replace(
      regexp_replace(coalesce(mi.isolated_by,''), '[\r\n\t]+', ' ', 'g'),
      '\s*(and|&)\s*', ',', 'gi'
    ),
    '[,;/]+'
  ) AS tok
  WHERE NULLIF(btrim(mi.isolated_by), '') IS NOT NULL

  UNION ALL
  SELECT
    mi.microbe_isolate_id,
    'maintained_by'::text,
    trim(tok),
    lower(regexp_replace(trim(tok), '\s+', ' ', 'g'))
  FROM structured.microbe_isolate mi
  CROSS JOIN LATERAL regexp_split_to_table(
    regexp_replace(
      regexp_replace(coalesce(mi.maintained_by,''), '[\r\n\t]+', ' ', 'g'),
      '\s*(and|&)\s*', ',', 'gi'
    ),
    '[,;/]+'
  ) AS tok
  WHERE NULLIF(btrim(mi.maintained_by), '') IS NOT NULL

  UNION ALL
  SELECT
    mi.microbe_isolate_id,
    'pcr_conducted_by'::text,
    trim(tok),
    lower(regexp_replace(trim(tok), '\s+', ' ', 'g'))
  FROM structured.microbe_isolate mi
  CROSS JOIN LATERAL regexp_split_to_table(
    regexp_replace(
      regexp_replace(coalesce(mi.pcr_conducted_by,''), '[\r\n\t]+', ' ', 'g'),
      '\s*(and|&)\s*', ',', 'gi'
    ),
    '[,;/]+'
  ) AS tok
  WHERE NULLIF(btrim(mi.pcr_conducted_by), '') IS NOT NULL

  UNION ALL
  SELECT
    mi.microbe_isolate_id,
    'sequencing_conducted_by'::text,
    trim(tok),
    lower(regexp_replace(trim(tok), '\s+', ' ', 'g'))
  FROM structured.microbe_isolate mi
  CROSS JOIN LATERAL regexp_split_to_table(
    regexp_replace(
      regexp_replace(coalesce(mi.sequencing_conducted_by,''), '[\r\n\t]+', ' ', 'g'),
      '\s*(and|&)\s*', ',', 'gi'
    ),
    '[,;/]+'
  ) AS tok
  WHERE NULLIF(btrim(mi.sequencing_conducted_by), '') IS NOT NULL
),
clean AS (
  SELECT *
  FROM extracted
  WHERE NULLIF(regexp_replace(coalesce(person_name_raw,''), '\s+', '', 'g'), '') IS NOT NULL
    AND person_name_norm NOT IN ('na','n/a','none','null','unknown','-')
),
ins_people AS (
  INSERT INTO structured.person_dim (person_name_raw, person_name_norm)
  SELECT DISTINCT person_name_raw, person_name_norm
  FROM clean
  ON CONFLICT (person_name_norm) DO NOTHING
  RETURNING person_id, person_name_norm
)
INSERT INTO structured.microbe_isolate_person_role (microbe_isolate_id, person_id, role)
SELECT
  c.microbe_isolate_id,
  p.person_id,
  c.role
FROM clean c
JOIN structured.person_dim p
  ON p.person_name_norm = c.person_name_norm
ON CONFLICT DO NOTHING;

-- =========================
-- KELP LINKS (source: structured.kelp_sample)
-- =========================
WITH extracted AS (
  SELECT
    ks.kelp_sample_id,
    'collected'::text AS role,
    trim(tok) AS person_name_raw,
    lower(regexp_replace(trim(tok), '\s+', ' ', 'g')) AS person_name_norm
  FROM structured.kelp_sample ks
  CROSS JOIN LATERAL regexp_split_to_table(
    regexp_replace(
      regexp_replace(coalesce(ks.sampling_metadata_personnel_collected,''), '[\r\n\t]+', ' ', 'g'),
      '\s*(and|&)\s*', ',', 'gi'
    ),
    '[,;/]+'
  ) AS tok
  WHERE NULLIF(btrim(ks.sampling_metadata_personnel_collected), '') IS NOT NULL

  UNION ALL
  SELECT
    ks.kelp_sample_id,
    'deposited_by'::text,
    trim(tok),
    lower(regexp_replace(trim(tok), '\s+', ' ', 'g'))
  FROM structured.kelp_sample ks
  CROSS JOIN LATERAL regexp_split_to_table(
    regexp_replace(
      regexp_replace(coalesce(ks.sampling_metadata_deposited_by,''), '[\r\n\t]+', ' ', 'g'),
      '\s*(and|&)\s*', ',', 'gi'
    ),
    '[,;/]+'
  ) AS tok
  WHERE NULLIF(btrim(ks.sampling_metadata_deposited_by), '') IS NOT NULL
),
clean AS (
  SELECT *
  FROM extracted
  WHERE NULLIF(regexp_replace(coalesce(person_name_raw,''), '\s+', '', 'g'), '') IS NOT NULL
    AND person_name_norm NOT IN ('na','n/a','none','null','unknown','-')
),
ins_people AS (
  INSERT INTO structured.person_dim (person_name_raw, person_name_norm)
  SELECT DISTINCT person_name_raw, person_name_norm
  FROM clean
  ON CONFLICT (person_name_norm) DO NOTHING
  RETURNING person_id, person_name_norm
)
INSERT INTO structured.kelp_sample_person_role (kelp_sample_id, person_id, role)
SELECT
  c.kelp_sample_id,
  p.person_id,
  c.role
FROM clean c
JOIN structured.person_dim p
  ON p.person_name_norm = c.person_name_norm
ON CONFLICT DO NOTHING;

COMMIT;
