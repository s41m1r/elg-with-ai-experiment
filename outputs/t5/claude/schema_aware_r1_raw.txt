SELECT case_id, activity, timestamp
FROM (
    SELECT 
        vo.visit_occurrence_id as case_id,
        'ED Arrival' as activity,
        vo.visit_start_datetime as timestamp
    FROM visit_occurrence vo
    WHERE vo.visit_concept_id IN (9203, 262)
    AND vo.visit_start_datetime IS NOT NULL
    
    UNION ALL
    
    SELECT 
        vo.visit_occurrence_id as case_id,
        'ED Discharge' as activity,
        vo.visit_end_datetime as timestamp
    FROM visit_occurrence vo
    WHERE vo.visit_concept_id IN (9203, 262)
    AND vo.visit_end_datetime IS NOT NULL
    
    UNION ALL
    
    SELECT 
        po.visit_occurrence_id as case_id,
        'ED Procedure' as activity,
        po.procedure_datetime as timestamp
    FROM procedure_occurrence po
    JOIN visit_occurrence vo ON po.visit_occurrence_id = vo.visit_occurrence_id
    WHERE vo.visit_concept_id IN (9203, 262)
    AND po.procedure_datetime IS NOT NULL
    
    UNION ALL
    
    SELECT 
        co.visit_occurrence_id as case_id,
        'ED Diagnosis Recorded' as activity,
        co.condition_start_datetime as timestamp
    FROM condition_occurrence co
    JOIN visit_occurrence vo ON co.visit_occurrence_id = vo.visit_occurrence_id
    WHERE vo.visit_concept_id IN (9203, 262)
    AND co.condition_start_datetime IS NOT NULL
) events
ORDER BY case_id, timestamp;