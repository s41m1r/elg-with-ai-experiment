SELECT 
    v.visit_occurrence_id AS case_id,
    COALESCE(
        CASE 
            WHEN v.visit_occurrence_id IN (SELECT visit_occurrence_id FROM visit_occurrence WHERE visit_concept_id = 9201) THEN 'Admission'
            WHEN c.condition_concept_id IS NOT NULL THEN 'Diagnosis'
            WHEN v.visit_occurrence_id IN (SELECT visit_occurrence_id FROM visit_occurrence WHERE visit_concept_id = 9202) THEN 'Discharge'
        END,
        'Unknown'
    ) AS activity,
    COALESCE(
        v.admit_time,
        c.condition_start_date,
        v.discharge_time
    ) AS timestamp
FROM 
    visit_occurrence v
LEFT JOIN 
    condition_occurrence c ON v.visit_occurrence_id = c.visit_occurrence_id
WHERE 
    v.visit_concept_id IN (9201, 9202)  -- Admission and Discharge
    OR c.condition_concept_id IS NOT NULL  -- Diagnoses
ORDER BY 
    v.visit_occurrence_id, 
    COALESCE(
        v.admit_time,
        c.condition_start_date,
        v.discharge_time
    );