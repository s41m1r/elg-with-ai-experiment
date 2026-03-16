SELECT 
    vo.visit_occurrence_id AS case_id,
    'ED Arrival' AS activity,
    vo.visit_start_datetime AS timestamp
FROM 
    visit_occurrence vo
WHERE 
    vo.visit_concept_id IN (9203, 262)
    AND vo.visit_start_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    'ED Discharge' AS activity,
    vo.visit_end_datetime AS timestamp
FROM 
    visit_occurrence vo
WHERE 
    vo.visit_concept_id IN (9203, 262)
    AND vo.visit_end_datetime IS NOT NULL

UNION ALL

SELECT 
    po.visit_occurrence_id AS case_id,
    'ED Procedure' AS activity,
    po.procedure_datetime AS timestamp
FROM 
    procedure_occurrence po
JOIN 
    visit_occurrence vo ON po.visit_occurrence_id = vo.visit_occurrence_id
WHERE 
    vo.visit_concept_id IN (9203, 262)
    AND po.procedure_datetime IS NOT NULL

UNION ALL

SELECT 
    co.visit_occurrence_id AS case_id,
    'ED Diagnosis Recorded' AS activity,
    co.condition_start_datetime AS timestamp
FROM 
    condition_occurrence co
JOIN 
    visit_occurrence vo ON co.visit_occurrence_id = vo.visit_occurrence_id
WHERE 
    vo.visit_concept_id IN (9203, 262)
    AND co.condition_start_datetime IS NOT NULL

ORDER BY 
    case_id, timestamp;