SELECT 
    v.visit_occurrence_id AS case_id,
    c.concept_name AS activity,
    s.specimen_datetime AS timestamp
FROM 
    specimen s
JOIN 
    visit_occurrence v ON s.person_id = v.person_id
JOIN 
    concept c ON s.specimen_concept_id = c.concept_id
WHERE 
    s.specimen_datetime IS NOT NULL

UNION ALL

SELECT 
    v.visit_occurrence_id AS case_id,
    c.concept_name AS activity,
    m.measurement_datetime AS timestamp
FROM 
    measurement m
JOIN 
    visit_occurrence v ON m.person_id = v.person_id
JOIN 
    concept c ON m.measurement_concept_id = c.concept_id
WHERE 
    m.measurement_datetime IS NOT NULL

UNION ALL

SELECT 
    v.visit_occurrence_id AS case_id,
    'Abnormal Result Flagged' AS activity,
    m.measurement_datetime AS timestamp
FROM 
    measurement m
JOIN 
    visit_occurrence v ON m.person_id = v.person_id
WHERE 
    m.measurement_datetime IS NOT NULL
    AND (m.value_as_number < m.range_low OR m.value_as_number > m.range_high)

ORDER BY 
    case_id, timestamp;