SELECT case_id, activity, timestamp
FROM (
    SELECT 
        visit_occurrence_id as case_id,
        'Hospital Admission' as activity,
        visit_start_datetime as timestamp
    FROM visit_occurrence
    WHERE visit_start_datetime IS NOT NULL
    
    UNION ALL
    
    SELECT 
        vo.visit_occurrence_id as case_id,
        'Hospital Discharge' as activity,
        vo.visit_end_datetime as timestamp
    FROM visit_occurrence vo
    WHERE vo.visit_end_datetime IS NOT NULL
    
    UNION ALL
    
    SELECT 
        vd.visit_occurrence_id as case_id,
        'ICU Admission' as activity,
        vd.visit_detail_start_datetime as timestamp
    FROM visit_detail vd
    JOIN concept c ON vd.visit_detail_concept_id = c.concept_id
    WHERE vd.visit_detail_start_datetime IS NOT NULL
    AND (c.concept_name ILIKE '%intensive care%' OR c.concept_name ILIKE '%ICU%' OR c.concept_name ILIKE '%critical care%')
    
    UNION ALL
    
    SELECT 
        vd.visit_occurrence_id as case_id,
        'ICU Discharge' as activity,
        vd.visit_detail_end_datetime as timestamp
    FROM visit_detail vd
    JOIN concept c ON vd.visit_detail_concept_id = c.concept_id
    WHERE vd.visit_detail_end_datetime IS NOT NULL
    AND (c.concept_name ILIKE '%intensive care%' OR c.concept_name ILIKE '%ICU%' OR c.concept_name ILIKE '%critical care%')
    
    UNION ALL
    
    SELECT 
        po.visit_occurrence_id as case_id,
        'ICU Procedure' as activity,
        po.procedure_datetime as timestamp
    FROM procedure_occurrence po
    JOIN visit_detail vd ON po.visit_occurrence_id = vd.visit_occurrence_id
    JOIN concept c ON vd.visit_detail_concept_id = c.concept_id
    WHERE po.procedure_datetime IS NOT NULL
    AND po.procedure_datetime >= vd.visit_detail_start_datetime
    AND po.procedure_datetime <= vd.visit_detail_end_datetime
    AND (c.concept_name ILIKE '%intensive care%' OR c.concept_name ILIKE '%ICU%' OR c.concept_name ILIKE '%critical care%')
) events
WHERE timestamp IS NOT NULL
ORDER BY case_id, timestamp;