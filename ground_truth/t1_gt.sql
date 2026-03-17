-- Ground-truth SQL for Task T1: ICU Patient Pathway
-- Author:  
-- Date: 2026-03-13
-- Data: MIMIC-IV in OMOP CDM format (PostgreSQL)
--
-- Output columns: case_id (BIGINT), activity (VARCHAR), timestamp (TIMESTAMP)
-- Case ID semantics: visit_occurrence_id (hospital admission)
--
-- Description: Track a patient's journey from hospital admission
--   through ICU transfer(s) to discharge.
--
-- Tables: visit_occurrence, visit_detail, procedure_occurrence, concept
-- Complexity: Medium
--
-- Note: Unlike CDMPHI, MIMIC-IV OMOP has a VISIT_DETAIL table.
--   ICU stays are identified via visit_detail_concept_id = 32037 (Intensive Care).

-- ─── Activity 1: Hospital Admission ───────────────────────────────────────
SELECT
    vo.visit_occurrence_id AS case_id,
    'Hospital Admission' AS activity,
    vo.visit_start_datetime AS timestamp
FROM visit_occurrence vo
WHERE vo.visit_start_datetime IS NOT NULL
  AND vo.visit_concept_id IN (9201, 262)  -- Inpatient Visit, ER and Inpatient

UNION ALL

-- ─── Activity 2: ICU Admission ────────────────────────────────────────────
SELECT
    vd.visit_occurrence_id AS case_id,
    'ICU Admission: ' || c.concept_name AS activity,
    vd.visit_detail_start_datetime AS timestamp
FROM visit_detail vd
JOIN concept c ON vd.visit_detail_concept_id = c.concept_id
WHERE vd.visit_detail_start_datetime IS NOT NULL
  AND vd.visit_detail_concept_id = 32037  -- Intensive Care

UNION ALL

-- ─── Activity 3: ICU Discharge ────────────────────────────────────────────
SELECT
    vd.visit_occurrence_id AS case_id,
    'ICU Discharge' AS activity,
    vd.visit_detail_end_datetime AS timestamp
FROM visit_detail vd
WHERE vd.visit_detail_end_datetime IS NOT NULL
  AND vd.visit_detail_concept_id = 32037  -- Intensive Care

UNION ALL

-- ─── Activity 4: ICU Procedure ────────────────────────────────────────────
SELECT
    po.visit_occurrence_id AS case_id,
    'ICU Procedure: ' || c.concept_name AS activity,
    po.procedure_datetime AS timestamp
FROM procedure_occurrence po
JOIN concept c ON po.procedure_concept_id = c.concept_id
WHERE po.procedure_datetime IS NOT NULL
  AND po.visit_occurrence_id IS NOT NULL
  AND po.visit_occurrence_id IN (
      SELECT DISTINCT vd.visit_occurrence_id
      FROM visit_detail vd
      WHERE vd.visit_detail_concept_id = 32037
  )

UNION ALL

-- ─── Activity 5: Hospital Discharge ───────────────────────────────────────
SELECT
    vo.visit_occurrence_id AS case_id,
    'Hospital Discharge' AS activity,
    vo.visit_end_datetime AS timestamp
FROM visit_occurrence vo
WHERE vo.visit_end_datetime IS NOT NULL
  AND vo.visit_concept_id IN (9201, 262)

ORDER BY case_id, timestamp;
