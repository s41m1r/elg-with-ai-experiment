SELECT 
    vo.visit_occurrence_id AS case_id,
    'Hospital Admission' AS activity,
    vo.visit_start_datetime AS timestamp
FROM 
    visit_occurrence vo
WHERE 
    vo.visit_start_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    'ICU Admission' AS activity,
    vd.visit_detail_start_datetime AS timestamp
FROM 
    visit_occurrence vo
JOIN 
    visit_detail vd ON vo.visit_occurrence_id = vd.visit_occurrence_id
WHERE 
    vd.visit_detail_concept_id = 9201  -- ICU stay
    AND vd.visit_detail_start_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    'ICU Procedure: ' || c.concept_name AS activity,
    po.procedure_datetime AS timestamp
FROM 
    visit_occurrence vo
JOIN 
    visit_detail vd ON vo.visit_occurrence_id = vd.visit_occurrence_id
JOIN 
    procedure_occurrence po ON vd.visit_detail_id = po.visit_detail_id
JOIN 
    concept c ON po.procedure_concept_id = c.concept_id
WHERE 
    vd.visit_detail_concept_id = 9201  -- ICU stay
    AND po.procedure_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    'ICU Discharge' AS activity,
    vd.visit_detail_end_datetime AS timestamp
FROM 
    visit_occurrence vo
JOIN 
    visit_detail vd ON vo.visit_occurrence_id = vd.visit_occurrence_id
WHERE 
    vd.visit_detail_concept_id = 9201  -- ICU stay
    AND vd.visit_detail_end_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    'Hospital Discharge' AS activity,
    vo.visit_end_datetime AS timestamp
FROM 
    visit_occurrence vo
WHERE 
    vo.visit_end_datetime IS NOT NULL

ORDER BY 
    case_id, 
    timestamp;