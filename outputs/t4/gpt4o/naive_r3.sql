SELECT 
    p.person_id AS case_id,
    'Specimen Collection' AS activity,
    sp.specimen_date AS timestamp
FROM 
    specimen sp
JOIN 
    person p ON sp.person_id = p.person_id

UNION ALL

SELECT 
    p.person_id AS case_id,
    'Result Recording' AS activity,
    m.measurement_date AS timestamp
FROM 
    measurement m
JOIN 
    person p ON m.person_id = p.person_id
WHERE 
    m.measurement_type_concept_id IN (
        SELECT concept_id 
        FROM concept 
        WHERE concept_name = 'Lab Result'
    )
ORDER BY 
    case_id, timestamp;