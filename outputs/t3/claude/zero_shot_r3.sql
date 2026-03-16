SELECT case_id, activity, timestamp
FROM (
    SELECT 
        vo.visit_occurrence_id as case_id,
        'Sepsis Diagnosed' as activity,
        co.condition_start_datetime as timestamp
    FROM visit_occurrence vo
    JOIN condition_occurrence co ON vo.visit_occurrence_id = co.visit_occurrence_id
    WHERE co.condition_concept_id IN (132797, 4103023, 40479642)
    AND co.condition_start_datetime IS NOT NULL
    
    UNION ALL
    
    SELECT 
        vo.visit_occurrence_id as case_id,
        'Lab Result Recorded' as activity,
        m.measurement_datetime as timestamp
    FROM visit_occurrence vo
    JOIN measurement m ON vo.visit_occurrence_id = m.visit_occurrence_id
    JOIN concept c ON m.measurement_concept_id = c.concept_id
    WHERE c.domain_id = 'Measurement'
    AND c.concept_class_id IN ('Lab Test', 'Laboratory')
    AND m.measurement_datetime IS NOT NULL
    
    UNION ALL
    
    SELECT 
        vo.visit_occurrence_id as case_id,
        'Vital Sign Measured' as activity,
        m.measurement_datetime as timestamp
    FROM visit_occurrence vo
    JOIN measurement m ON vo.visit_occurrence_id = m.visit_occurrence_id
    JOIN concept c ON m.measurement_concept_id = c.concept_id
    WHERE c.domain_id = 'Measurement'
    AND c.concept_class_id = 'Clinical Observation'
    AND m.measurement_datetime IS NOT NULL
    
    UNION ALL
    
    SELECT 
        vo.visit_occurrence_id as case_id,
        'Antibiotic Started' as activity,
        de.drug_exposure_start_datetime as timestamp
    FROM visit_occurrence vo
    JOIN drug_exposure de ON vo.visit_occurrence_id = de.visit_occurrence_id
    JOIN concept c ON de.drug_concept_id = c.concept_id
    JOIN concept_ancestor ca ON c.concept_id = ca.descendant_concept_id
    WHERE ca.ancestor_concept_id = 21602796
    AND de.drug_exposure_start_datetime IS NOT NULL
    
    UNION ALL
    
    SELECT 
        vo.visit_occurrence_id as case_id,
        'Antibiotic Ended' as activity,
        de.drug_exposure_end_datetime as timestamp
    FROM visit_occurrence vo
    JOIN drug_exposure de ON vo.visit_occurrence_id = de.visit_occurrence_id
    JOIN concept c ON de.drug_concept_id = c.concept_id
    JOIN concept_ancestor ca ON c.concept_id = ca.descendant_concept_id
    WHERE ca.ancestor_concept_id = 21602796
    AND de.drug_exposure_end_datetime IS NOT NULL
) events
WHERE timestamp IS NOT NULL
ORDER BY case_id, timestamp;