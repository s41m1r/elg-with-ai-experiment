SELECT DISTINCT
    v.person_id AS case_id,
    CASE 
        WHEN v.visit_start_date IS NOT NULL THEN 'Admission'
        WHEN co.condition_start_date IS NOT NULL THEN CONCAT('Diagnosis: ', c.concept_name)
        WHEN v.visit_end_date IS NOT NULL THEN 'Discharge'
    END AS activity,
    COALESCE(
        v.visit_start_datetime,
        co.condition_start_datetime,
        v.visit_end_datetime
    ) AS timestamp
FROM visit_occurrence v
LEFT JOIN condition_occurrence co ON v.visit_occurrence_id = co.visit_occurrence_id
LEFT JOIN concept c ON co.condition_concept_id = c.concept_id
WHERE v.visit_concept_id IN (
    SELECT concept_id 
    FROM concept 
    WHERE domain_id = 'Visit' 
    AND concept_name ILIKE '%inpatient%'
)
AND (
    v.visit_start_date IS NOT NULL 
    OR co.condition_start_date IS NOT NULL 
    OR v.visit_end_date IS NOT NULL
)
ORDER BY case_id, timestamp;