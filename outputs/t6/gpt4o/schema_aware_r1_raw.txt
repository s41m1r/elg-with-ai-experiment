SELECT 
    vo.visit_occurrence_id AS case_id,
    'Hospital Admission' AS activity,
    vo.visit_start_datetime AS timestamp
FROM 
    visit_occurrence vo
WHERE 
    vo.visit_concept_id IN (9201, 262)
    AND vo.visit_start_datetime IS NOT NULL

UNION ALL

SELECT 
    co.visit_occurrence_id AS case_id,
    c.concept_name AS activity,
    co.condition_start_datetime AS timestamp
FROM 
    condition_occurrence co
JOIN 
    concept c ON co.condition_concept_id = c.concept_id
WHERE 
    co.condition_start_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    'Hospital Discharge' AS activity,
    vo.visit_end_datetime AS timestamp
FROM 
    visit_occurrence vo
WHERE 
    vo.visit_concept_id IN (9201, 262)
    AND vo.visit_end_datetime IS NOT NULL

ORDER BY 
    case_id, timestamp;