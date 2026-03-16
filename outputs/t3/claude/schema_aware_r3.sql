SELECT 
    case_id,
    activity,
    timestamp
FROM (
    -- Sepsis Diagnosed
    SELECT 
        co.visit_occurrence_id as case_id,
        'Sepsis Diagnosed' as activity,
        co.condition_start_datetime as timestamp
    FROM condition_occurrence co
    WHERE co.condition_concept_id IN (132797, 4103023, 40479642)
    AND co.condition_start_datetime IS NOT NULL
    AND co.visit_occurrence_id IS NOT NULL
    
    UNION ALL
    
    -- Lab Result Recorded
    SELECT 
        m.visit_occurrence_id as case_id,
        'Lab Result Recorded' as activity,
        m.measurement_datetime as timestamp
    FROM measurement m
    WHERE m.measurement_datetime IS NOT NULL
    AND m.visit_occurrence_id IS NOT NULL
    
    UNION ALL
    
    -- Antibiotic Started
    SELECT 
        de.visit_occurrence_id as case_id,
        'Antibiotic Started' as activity,
        de.drug_exposure_start_datetime as timestamp
    FROM drug_exposure de
    WHERE de.drug_exposure_start_datetime IS NOT NULL
    AND de.visit_occurrence_id IS NOT NULL
    
    UNION ALL
    
    -- Antibiotic Ended
    SELECT 
        de.visit_occurrence_id as case_id,
        'Antibiotic Ended' as activity,
        de.drug_exposure_end_datetime as timestamp
    FROM drug_exposure de
    WHERE de.drug_exposure_end_datetime IS NOT NULL
    AND de.visit_occurrence_id IS NOT NULL
    
    UNION ALL
    
    -- Vital Sign Measured
    SELECT 
        m.visit_occurrence_id as case_id,
        'Vital Sign Measured' as activity,
        m.measurement_datetime as timestamp
    FROM measurement m
    WHERE m.measurement_datetime IS NOT NULL
    AND m.visit_occurrence_id IS NOT NULL
) events
WHERE case_id IN (
    SELECT DISTINCT visit_occurrence_id 
    FROM condition_occurrence 
    WHERE condition_concept_id IN (132797, 4103023, 40479642)
    AND visit_occurrence_id IS NOT NULL
)
ORDER BY case_id, timestamp;