-- ============================================================
-- 001_create_schemas.sql
-- Kara Platform – Database Initialization
-- Creates extensions and schemas ONLY
-- ============================================================

BEGIN;

-- ============================================================
-- Required Extensions
-- ============================================================

-- UUID generation
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Spatial data (PostGIS)
CREATE EXTENSION IF NOT EXISTS postgis;

-- Vector embeddings (RAG / semantic search)
CREATE EXTENSION IF NOT EXISTS vector;

-- (Optional but common) text search helpers
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================
-- Core Data Schemas (tables live here)
-- ============================================================

-- Raw ingestion / ETL landing zone
CREATE SCHEMA IF NOT EXISTS staging;

-- Canonical source-of-truth structured data
CREATE SCHEMA IF NOT EXISTS structured;

-- Vector storage for embeddings & document chunks
CREATE SCHEMA IF NOT EXISTS vector_store;

-- ============================================================
-- Internal / System View Schemas
-- ============================================================

-- Internal-only document research & embeddings
CREATE SCHEMA IF NOT EXISTS kara_internal;

-- Public-safe document research & embeddings
CREATE SCHEMA IF NOT EXISTS kara_public;

-- ============================================================
-- Domain / Role-Based View Schemas
-- (views only; no base tables)
-- ============================================================

-- Farmers & breeders
CREATE SCHEMA IF NOT EXISTS kara_farmbreed;

-- Restoration practitioners
CREATE SCHEMA IF NOT EXISTS kara_restoration;

-- Non-genetic research (phenotypic / environmental only)
CREATE SCHEMA IF NOT EXISTS kara_nongenresearch;

-- Genetic research (restricted access)
CREATE SCHEMA IF NOT EXISTS kara_genresearch;

-- Public-facing structured search views
CREATE SCHEMA IF NOT EXISTS kara_public_structured;

-- ============================================================
-- Optional / Future System Schemas
-- ============================================================

-- Audit logs, access tracking, compliance
CREATE SCHEMA IF NOT EXISTS audit;

-- Admin helpers, migrations metadata, system utilities
CREATE SCHEMA IF NOT EXISTS admin;

COMMIT;
