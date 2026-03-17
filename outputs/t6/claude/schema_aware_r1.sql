SELECT case_id, activity, timestamp
FROM (
    SELECT 
        vo.visit_occurrence_id as case_id,
        'Hospital Admission' as activity,
        vo.visit_start_datetime as timestamp
    FROM visit_occurrence vo
    WHERE vo.visit_concept_id IN (9201, 262)
    AND vo.visit_start_datetime IS NOT NULL
    
    UNION ALL
    
    SELECT 
        vo.visit_occurrence_id as case_id,
        CONCAT('Condition Diagnosed: ', c.concept_name) as activity,
        co.condition_start_datetime as timestamp
    FROM visit_occurrence vo
    JOIN condition_occurrence co ON vo.visit_occurrence_id = co.visit_occurrence_id
    JOIN concept c ON co.condition_concept_id = c.concept_id
    WHERE vo.visit_concept_id IN (9201, 262)
    AND co.condition_start_datetime IS NOT NULL
    
    UNION ALL
    
    SELECT 
        vo.visit_occurrence_id as case_id,
        'Hospital Discharge' as activity,
        vo.visit_end_datetime as timestamp
    FROM visit_occurrence vo
    WHERE vo.visit_concept_id IN (9201, 262)
    AND vo.visit_end_datetime IS NOT NULL
) events
ORDER BY case_id, timestamp;