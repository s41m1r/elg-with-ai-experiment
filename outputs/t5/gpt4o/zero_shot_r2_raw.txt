SELECT 
    vo.visit_occurrence_id AS case_id,
    'ED Arrival' AS activity,
    vd.visit_detail_start_datetime AS timestamp
FROM 
    visit_occurrence vo
JOIN 
    visit_detail vd ON vo.visit_occurrence_id = vd.visit_occurrence_id
WHERE 
    vo.visit_concept_id IN (9203, 262)
    AND vd.visit_detail_concept_id = 8870
    AND vd.visit_detail_start_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    'ED Procedure' AS activity,
    po.procedure_datetime AS timestamp
FROM 
    visit_occurrence vo
JOIN 
    procedure_occurrence po ON vo.visit_occurrence_id = po.visit_occurrence_id
WHERE 
    vo.visit_concept_id IN (9203, 262)
    AND po.procedure_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    'ED Diagnosis Recorded' AS activity,
    co.condition_start_datetime AS timestamp
FROM 
    visit_occurrence vo
JOIN 
    condition_occurrence co ON vo.visit_occurrence_id = co.visit_occurrence_id
WHERE 
    vo.visit_concept_id IN (9203, 262)
    AND co.condition_start_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    'ED Discharge' AS activity,
    vd.visit_detail_end_datetime AS timestamp
FROM 
    visit_occurrence vo
JOIN 
    visit_detail vd ON vo.visit_occurrence_id = vd.visit_occurrence_id
WHERE 
    vo.visit_concept_id IN (9203, 262)
    AND vd.visit_detail_concept_id = 8870
    AND vd.visit_detail_end_datetime IS NOT NULL

ORDER BY 
    case_id, timestamp;