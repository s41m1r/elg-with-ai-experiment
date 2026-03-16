SELECT visit_occurrence_id AS case_id, 'Hospital Admission' AS activity, visit_start_date AS timestamp
FROM visit_occurrence
WHERE visit_start_date IS NOT NULL

UNION ALL

SELECT vd.visit_occurrence_id AS case_id, 'ICU Admission' AS activity, vd.visit_detail_start_date AS timestamp
FROM visit_detail vd
WHERE vd.visit_detail_concept_id IN (SELECT concept_id FROM concept WHERE concept_name IN ('ICU stay', 'Intensive care unit stay'))
AND vd.visit_detail_start_date IS NOT NULL

UNION ALL

SELECT po.visit_occurrence_id AS case_id, 'ICU Procedure' AS activity, po.procedure_date AS timestamp
FROM procedure_occurrence po
WHERE po.procedure_date IS NOT NULL
AND po.visit_occurrence_id IN (SELECT visit_occurrence_id FROM visit_detail WHERE visit_detail_concept_id IN (SELECT concept_id FROM concept WHERE concept_name IN ('ICU stay', 'Intensive care unit stay')))

UNION ALL

SELECT vd.visit_occurrence_id AS case_id, 'ICU Discharge' AS activity, vd.visit_detail_end_date AS timestamp
FROM visit_detail vd
WHERE vd.visit_detail_concept_id IN (SELECT concept_id FROM concept WHERE concept_name IN ('ICU stay', 'Intensive care unit stay'))
AND vd.visit_detail_end_date IS NOT NULL

UNION ALL

SELECT visit_occurrence_id AS case_id, 'Hospital Discharge' AS activity, visit_end_date AS timestamp
FROM visit_occurrence
WHERE visit_end_date IS NOT NULL

ORDER BY case_id, timestamp