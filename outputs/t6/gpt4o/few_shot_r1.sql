SELECT
    v.visit_occurrence_id AS case_id,
    'Hospital Admission' AS activity,
    v.visit_start_datetime AS timestamp
FROM visit_occurrence v
WHERE v.visit_start_datetime IS NOT NULL
  AND v.visit_concept_id IN (9201, 262)

UNION ALL

SELECT
    c.visit_occurrence_id AS case_id,
    'Condition Diagnosed: ' || con.concept_name AS activity,
    c.condition_start_datetime AS timestamp
FROM condition_occurrence c
JOIN concept con ON c.condition_concept_id = con.concept_id
WHERE c.condition_start_datetime IS NOT NULL
  AND c.visit_occurrence_id IS NOT NULL

UNION ALL

SELECT
    v.visit_occurrence_id AS case_id,
    'Hospital Discharge' AS activity,
    v.visit_end_datetime AS timestamp
FROM visit_occurrence v
WHERE v.visit_end_datetime IS NOT NULL
  AND v.visit_concept_id IN (9201, 262)

ORDER BY case_id, timestamp;