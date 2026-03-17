-- Ground-truth SQL for Task T6: Inpatient Diagnosis Pathway
-- Author: 
-- Date: 2026-03-17
-- Data: MIMIC-IV in OMOP CDM format (PostgreSQL)
--
-- Output columns: case_id (BIGINT), activity (VARCHAR), timestamp (TIMESTAMP)
-- Case ID semantics: visit_occurrence_id (inpatient visit)
--
-- Description: Track condition diagnoses over the course of an inpatient
--   hospital stay: admission → diagnosis recorded → ... → discharge.
--
-- Tables: visit_occurrence, condition_occurrence, concept
-- Complexity: Low
--
-- Note: Inpatient visits identified by visit_concept_id = 9201 (Inpatient Visit)
--   or 262 (Emergency Room and Inpatient Visit). Each condition_occurrence row
--   linked to the visit via visit_occurrence_id becomes a diagnosis event.

-- ─── Activity 1: Hospital Admission ───────────────────────────────────────
SELECT
    vo.visit_occurrence_id AS case_id,
    'Hospital Admission' AS activity,
    vo.visit_start_datetime AS timestamp
FROM visit_occurrence vo
WHERE vo.visit_concept_id IN (9201, 262)  -- Inpatient Visit, ER and Inpatient
  AND vo.visit_start_datetime IS NOT NULL

UNION ALL

-- ─── Activity 2: Condition Diagnosed ──────────────────────────────────────
SELECT
    co.visit_occurrence_id AS case_id,
    'Condition Diagnosed: ' || c.concept_name AS activity,
    co.condition_start_datetime AS timestamp
FROM condition_occurrence co
JOIN concept c ON co.condition_concept_id = c.concept_id
WHERE co.condition_start_datetime IS NOT NULL
  AND co.visit_occurrence_id IN (
      SELECT vo.visit_occurrence_id
      FROM visit_occurrence vo
      WHERE vo.visit_concept_id IN (9201, 262)
  )

UNION ALL

-- ─── Activity 3: Hospital Discharge ──────────────────────────────────────
SELECT
    vo.visit_occurrence_id AS case_id,
    'Hospital Discharge' AS activity,
    vo.visit_end_datetime AS timestamp
FROM visit_occurrence vo
WHERE vo.visit_concept_id IN (9201, 262)
  AND vo.visit_end_datetime IS NOT NULL

ORDER BY case_id, timestamp;
