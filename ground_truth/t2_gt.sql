-- Ground-truth SQL for Task T2: Medication Administration
-- Author: 
-- Date: 2026-03-13
-- Data: MIMIC-IV in OMOP CDM format (PostgreSQL)
--
-- Output columns: case_id (BIGINT), activity (VARCHAR), timestamp (TIMESTAMP)
-- Case ID semantics: visit_occurrence_id (hospital admission)
--
-- Description: Track the lifecycle of medications for a hospital admission:
--   drug exposure started → drug exposure ended.
--
-- Tables: drug_exposure, concept
-- Complexity: Medium
--
-- Note: CDMPHI has rich XTN_ columns (order datetime, dispense datetime,
--   discontinuation datetime). MIMIC-IV OMOP has standard drug_exposure only.
--   We use drug_exposure_start_datetime and drug_exposure_end_datetime.
--   drug_era is excluded since it has no visit_occurrence_id FK in OMOP CDM.

-- ─── Activity 1: Drug Exposure Started ────────────────────────────────────
SELECT
    de.visit_occurrence_id AS case_id,
    'Drug Started: ' || c.concept_name AS activity,
    de.drug_exposure_start_datetime AS timestamp
FROM drug_exposure de
JOIN concept c ON de.drug_concept_id = c.concept_id
WHERE de.drug_exposure_start_datetime IS NOT NULL
  AND de.visit_occurrence_id IS NOT NULL

UNION ALL

-- ─── Activity 2: Drug Exposure Ended ──────────────────────────────────────
SELECT
    de.visit_occurrence_id AS case_id,
    'Drug Ended: ' || c.concept_name AS activity,
    de.drug_exposure_end_datetime AS timestamp
FROM drug_exposure de
JOIN concept c ON de.drug_concept_id = c.concept_id
WHERE de.drug_exposure_end_datetime IS NOT NULL
  AND de.visit_occurrence_id IS NOT NULL

ORDER BY case_id, timestamp;
