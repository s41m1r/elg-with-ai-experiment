SELECT 
    vo.visit_occurrence_id AS case_id,
    'Specimen Collected' AS activity,
    s.specimen_datetime AS timestamp
FROM 
    visit_occurrence vo
JOIN 
    specimen s ON vo.person_id = s.person_id
JOIN 
    concept c ON s.specimen_concept_id = c.concept_id
WHERE 
    s.specimen_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    'Lab Result Recorded' AS activity,
    m.measurement_datetime AS timestamp
FROM 
    visit_occurrence vo
JOIN 
    measurement m ON vo.visit_occurrence_id = m.visit_occurrence_id
JOIN 
    concept c ON m.measurement_concept_id = c.concept_id
WHERE 
    m.measurement_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    'Abnormal Result Flagged' AS activity,
    m.measurement_datetime AS timestamp
FROM 
    visit_occurrence vo
JOIN 
    measurement m ON vo.visit_occurrence_id = m.visit_occurrence_id
JOIN 
    concept c ON m.measurement_concept_id = c.concept_id
WHERE 
    m.measurement_datetime IS NOT NULL
    AND (m.value_as_number < m.range_low OR m.value_as_number > m.range_high)

ORDER BY 
    case_id, 
    timestamp;