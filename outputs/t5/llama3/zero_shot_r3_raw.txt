SELECT visit_occurrence_id AS case_id, 'ED Arrival' AS activity, visit_start_datetime AS timestamp
FROM visit_occurrence
WHERE visit_concept_id IN (9203, 262) AND visit_start_datetime IS NOT NULL
UNION ALL
SELECT visit_occurrence_id AS case_id, 'ED Procedure' AS activity, procedure_datetime AS timestamp
FROM procedure_occurrence
WHERE procedure_datetime IS NOT NULL
UNION ALL
SELECT visit_occurrence_id AS case_id, 'ED Diagnosis Recorded' AS activity, condition_start_datetime AS timestamp
FROM condition_occurrence
WHERE condition_start_datetime IS NOT NULL
UNION ALL
SELECT visit_occurrence_id AS case_id, 'ED Discharge' AS activity, visit_end_datetime AS timestamp
FROM visit_occurrence
WHERE visit_concept_id IN (9203, 262) AND visit_end_datetime IS NOT NULL
ORDER BY case_id, timestamp;