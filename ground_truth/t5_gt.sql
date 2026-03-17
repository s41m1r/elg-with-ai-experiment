-- Ground-truth SQL for Task T5: Emergency Department Flow
-- Author: 
-- Date: 2026-03-13
-- Data: MIMIC-IV in OMOP CDM format (PostgreSQL)
--
-- Output columns: case_id (BIGINT), activity (VARCHAR), timestamp (TIMESTAMP)
-- Case ID semantics: visit_occurrence_id (ED visit)
--
-- Description: Track a patient's flow through the emergency department:
--   arrival → ED sub-stay → diagnosis → procedure → departure.
--
-- Tables: visit_occurrence, visit_detail, condition_occurrence,
--         procedure_occurrence, concept
-- Complexity: Low–Medium
--
-- Note: CDMPHI uses XTN_ columns (check-in, roomed, triage acuity, departure,
--   disposition, ED diagnosis flag). MIMIC-IV OMOP has visit_detail for
--   ED sub-stays (visit_detail_concept_id = 8870 = Emergency Room).
--   ED arrival = visit_start_datetime; departure = visit_end_datetime.
--   All diagnoses linked to the ED visit are included (no ED-only flag available).

-- ─── Activity 1: ED Arrival ───────────────────────────────────────────────
SELECT
    vo.visit_occurrence_id AS case_id,
    'ED Arrival' AS activity,
    vo.visit_start_datetime AS timestamp
FROM visit_occurrence vo
WHERE vo.visit_concept_id IN (9203, 262)  -- Emergency Room Visit, ER and Inpatient
  AND vo.visit_start_datetime IS NOT NULL

UNION ALL

-- ─── Activity 2: ED Sub-Stay (from visit_detail) ──────────────────────────
SELECT
    vd.visit_occurrence_id AS case_id,
    'ED Sub-Stay: ' || c.concept_name AS activity,
    vd.visit_detail_start_datetime AS timestamp
FROM visit_detail vd
JOIN concept c ON vd.visit_detail_concept_id = c.concept_id
WHERE vd.visit_detail_start_datetime IS NOT NULL
  AND vd.visit_occurrence_id IN (
      SELECT vo.visit_occurrence_id
      FROM visit_occurrence vo
      WHERE vo.visit_concept_id IN (9203, 262)
  )

UNION ALL

-- ─── Activity 3: ED Diagnosis Recorded ────────────────────────────────────
SELECT
    co.visit_occurrence_id AS case_id,
    'ED Diagnosis: ' || c.concept_name AS activity,
    co.condition_start_datetime AS timestamp
FROM condition_occurrence co
JOIN concept c ON co.condition_concept_id = c.concept_id
WHERE co.condition_start_datetime IS NOT NULL
  AND co.visit_occurrence_id IN (
      SELECT vo.visit_occurrence_id
      FROM visit_occurrence vo
      WHERE vo.visit_concept_id IN (9203, 262)
  )

UNION ALL

-- ─── Activity 4: ED Procedure ─────────────────────────────────────────────
SELECT
    po.visit_occurrence_id AS case_id,
    'ED Procedure: ' || c.concept_name AS activity,
    po.procedure_datetime AS timestamp
FROM procedure_occurrence po
JOIN concept c ON po.procedure_concept_id = c.concept_id
WHERE po.procedure_datetime IS NOT NULL
  AND po.visit_occurrence_id IN (
      SELECT vo.visit_occurrence_id
      FROM visit_occurrence vo
      WHERE vo.visit_concept_id IN (9203, 262)
  )

UNION ALL

-- ─── Activity 5: ED Departure ─────────────────────────────────────────────
SELECT
    vo.visit_occurrence_id AS case_id,
    'ED Departure' AS activity,
    vo.visit_end_datetime AS timestamp
FROM visit_occurrence vo
WHERE vo.visit_concept_id IN (9203, 262)
  AND vo.visit_end_datetime IS NOT NULL

ORDER BY case_id, timestamp;
