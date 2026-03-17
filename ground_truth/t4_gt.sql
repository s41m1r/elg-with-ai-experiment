-- Ground-truth SQL for Task T4: Lab-Order-to-Result Cycle
-- Author: 
-- Date: 2026-03-04
-- Data: MIMIC-IV in OMOP CDM format
--
-- Output columns: case_id (BIGINT), activity (VARCHAR), timestamp (TIMESTAMP)
-- Case ID semantics: visit_occurrence_id (hospital admission)
--
-- Description: Track a laboratory test lifecycle from specimen collection
--   through result recording, flagging abnormal results.
--
-- Tables used: measurement, specimen, concept, visit_occurrence
-- Complexity: Low (2 main OMOP tables + concept lookup)
-- Note: specimen has no visit_occurrence_id; we link via person_id +
--   temporal proximity (specimen_datetime within visit start/end).

-- ─── Activity 1: Specimen Collected ───────────────────────────────────────
SELECT
    vo.visit_occurrence_id AS case_id,
    'Specimen Collected: ' || c.concept_name AS activity,
    s.specimen_datetime AS timestamp
FROM specimen s
JOIN person p ON s.person_id = p.person_id
JOIN visit_occurrence vo
    ON s.person_id = vo.person_id
    AND s.specimen_datetime >= vo.visit_start_datetime
    AND s.specimen_datetime <= vo.visit_end_datetime
JOIN concept c ON s.specimen_concept_id = c.concept_id
WHERE s.specimen_datetime IS NOT NULL

UNION ALL

-- ─── Activity 2: Lab Result Recorded (normal) ────────────────────────────
SELECT
    m.visit_occurrence_id AS case_id,
    'Lab Result: ' || c.concept_name AS activity,
    m.measurement_datetime AS timestamp
FROM measurement m
JOIN concept c ON m.measurement_concept_id = c.concept_id
WHERE m.measurement_datetime IS NOT NULL
  AND m.visit_occurrence_id IS NOT NULL
  AND (
      -- normal: within range, or range not available
      m.value_as_number IS NULL
      OR m.range_low IS NULL
      OR m.range_high IS NULL
      OR (m.value_as_number >= m.range_low AND m.value_as_number <= m.range_high)
  )

UNION ALL

-- ─── Activity 3: Abnormal Result Flagged ──────────────────────────────────
SELECT
    m.visit_occurrence_id AS case_id,
    'Abnormal Result: ' || c.concept_name AS activity,
    m.measurement_datetime AS timestamp
FROM measurement m
JOIN concept c ON m.measurement_concept_id = c.concept_id
WHERE m.measurement_datetime IS NOT NULL
  AND m.visit_occurrence_id IS NOT NULL
  AND m.value_as_number IS NOT NULL
  AND m.range_low IS NOT NULL
  AND m.range_high IS NOT NULL
  AND (m.value_as_number < m.range_low OR m.value_as_number > m.range_high)

ORDER BY case_id, timestamp;
