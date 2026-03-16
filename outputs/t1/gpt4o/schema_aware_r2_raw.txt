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
    'Hospital Discharge' AS activity,
    vo.visit_end_datetime AS timestamp
FROM 
    visit_occurrence vo
WHERE 
    vo.visit_end_datetime IS NOT NULL

UNION ALL

SELECT 
    vd.visit_occurrence_id AS case_id,
    'ICU Admission' AS activity,
    vd.visit_detail_start_datetime AS timestamp
FROM 
    visit_detail vd
WHERE 
    vd.visit_detail_concept_id = (SELECT concept_id FROM concept WHERE concept_name = 'ICU') 
    AND vd.visit_detail_start_datetime IS NOT NULL

UNION ALL

SELECT 
    vd.visit_occurrence_id AS case_id,
    'ICU Discharge' AS activity,
    vd.visit_detail_end_datetime AS timestamp
FROM 
    visit_detail vd
WHERE 
    vd.visit_detail_concept_id = (SELECT concept_id FROM concept WHERE concept_name = 'ICU') 
    AND vd.visit_detail_end_datetime IS NOT NULL

UNION ALL

SELECT 
    po.visit_occurrence_id AS case_id,
    'ICU Procedure' AS activity,
    po.procedure_datetime AS timestamp
FROM 
    procedure_occurrence po
JOIN 
    visit_detail vd ON po.visit_detail_id = vd.visit_detail_id
WHERE 
    vd.visit_detail_concept_id = (SELECT concept_id FROM concept WHERE concept_name = 'ICU') 
    AND po.procedure_datetime IS NOT NULL

ORDER BY 
    case_id, timestamp;