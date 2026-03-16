SELECT 
    vo.visit_occurrence_id as case_id,
    'Specimen Collected' as activity,
    s.specimen_datetime as timestamp
FROM specimen s
JOIN visit_occurrence vo ON s.person_id = vo.person_id 
    AND s.specimen_datetime BETWEEN vo.visit_start_datetime AND COALESCE(vo.visit_end_datetime, vo.visit_start_datetime + INTERVAL '30 days')
WHERE s.specimen_datetime IS NOT NULL

UNION ALL

SELECT 
    m.visit_occurrence_id as case_id,
    'Lab Result Recorded' as activity,
    m.measurement_datetime as timestamp
FROM measurement m
WHERE m.measurement_datetime IS NOT NULL
    AND m.visit_occurrence_id IS NOT NULL

UNION ALL

SELECT 
    m.visit_occurrence_id as case_id,
    'Abnormal Result Flagged' as activity,
    m.measurement_datetime as timestamp
FROM measurement m
WHERE m.measurement_datetime IS NOT NULL
    AND m.visit_occurrence_id IS NOT NULL
    AND m.value_as_number IS NOT NULL
    AND m.range_low IS NOT NULL
    AND m.range_high IS NOT NULL
    AND (m.value_as_number < m.range_low OR m.value_as_number > m.range_high)

ORDER BY case_id, timestamp;