SELECT 
    v.visit_occurrence_id AS case_id,
    'Specimen Collected' AS activity,
    s.specimen_datetime AS timestamp
FROM 
    visit_occurrence v
JOIN 
    specimen s ON v.person_id = s.person_id
WHERE 
    s.specimen_datetime IS NOT NULL

UNION ALL

SELECT 
    v.visit_occurrence_id AS case_id,
    'Lab Result Recorded' AS activity,
    m.measurement_datetime AS timestamp
FROM 
    visit_occurrence v
JOIN 
    measurement m ON v.person_id = m.person_id
WHERE 
    m.measurement_datetime IS NOT NULL

UNION ALL

SELECT 
    v.visit_occurrence_id AS case_id,
    'Abnormal Result Flagged' AS activity,
    m.measurement_datetime AS timestamp
FROM 
    visit_occurrence v
JOIN 
    measurement m ON v.person_id = m.person_id
WHERE 
    m.measurement_datetime IS NOT NULL
    AND (m.value_as_number < m.range_low OR m.value_as_number > m.range_high)

ORDER BY 
    case_id, timestamp;