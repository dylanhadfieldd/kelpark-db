-- 001_create_schemas.sql
BEGIN;

-- Extensions (pgvector likely already installed; we don't touch it here)
CREATE EXTENSION IF NOT EXISTS pgcrypto; -- for gen_random_uuid()

-- Core schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS structured;

-- View schemas by use-case
CREATE SCHEMA IF NOT EXISTS ai_public;
CREATE SCHEMA IF NOT EXISTS ai_internal;
CREATE SCHEMA IF NOT EXISTS ai_restoration;
CREATE SCHEMA IF NOT EXISTS ai_genetic;
CREATE SCHEMA IF NOT EXISTS ai_farming;
CREATE SCHEMA IF NOT EXISTS ai_nongenetic;

-- Ingestion batch log
CREATE TABLE IF NOT EXISTS structured.ingest_batch (
  ingest_batch_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_system TEXT NULL,
  source_filename TEXT NULL,
  dataset_name TEXT NOT NULL, -- 'kelps' | 'microbes'
  loaded_by TEXT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  row_count_loaded INTEGER NULL,
  row_count_rejected INTEGER NULL,
  notes TEXT NULL
);

COMMIT;
