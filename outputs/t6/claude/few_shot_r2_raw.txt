SELECT
    v.visit_occurrence_id AS case_id,
    'Hospital Admission' AS activity,
    v.visit_start_datetime AS timestamp
FROM visit_occurrence v
WHERE v.visit_concept_id IN (9201, 262)
  AND v.visit_start_datetime IS NOT NULL

UNION ALL

SELECT
    co.visit_occurrence_id AS case_id,
    'Condition Diagnosed: ' || c.concept_name AS activity,
    co.condition_start_datetime AS timestamp
FROM condition_occurrence co
JOIN visit_occurrence v ON co.visit_occurrence_id = v.visit_occurrence_id
JOIN concept c ON co.condition_concept_id = c.concept_id
WHERE v.visit_concept_id IN (9201, 262)
  AND co.condition_start_datetime IS NOT NULL
  AND co.visit_occurrence_id IS NOT NULL

UNION ALL

SELECT
    v.visit_occurrence_id AS case_id,
    'Hospital Discharge' AS activity,
    v.visit_end_datetime AS timestamp
FROM visit_occurrence v
WHERE v.visit_concept_id IN (9201, 262)
  AND v.visit_end_datetime IS NOT NULL

ORDER BY case_id, timestamp;