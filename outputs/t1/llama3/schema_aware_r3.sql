SELECT visit_occurrence_id AS case_id, 'Hospital Admission' AS activity, visit_start_datetime AS timestamp
FROM visit_occurrence
WHERE visit_start_datetime IS NOT NULL

UNION ALL

SELECT visit_occurrence_id AS case_id, 'ICU Admission' AS activity, visit_detail_start_datetime AS timestamp
FROM visit_detail
WHERE visit_detail_concept_id = 9202 AND visit_detail_start_datetime IS NOT NULL

UNION ALL

SELECT visit_occurrence_id AS case_id, 'ICU Procedure' AS activity, procedure_datetime AS timestamp
FROM procedure_occurrence
WHERE visit_detail_id IN (SELECT visit_detail_id FROM visit_detail WHERE visit_detail_concept_id = 9202) AND procedure_datetime IS NOT NULL

UNION ALL

SELECT visit_occurrence_id AS case_id, 'ICU Discharge' AS activity, visit_detail_end_datetime AS timestamp
FROM visit_detail
WHERE visit_detail_concept_id = 9202 AND visit_detail_end_datetime IS NOT NULL

UNION ALL

SELECT visit_occurrence_id AS case_id, 'Hospital Discharge' AS activity, visit_end_datetime AS timestamp
FROM visit_occurrence
WHERE visit_end_datetime IS NOT NULL

ORDER BY case_id, timestamp