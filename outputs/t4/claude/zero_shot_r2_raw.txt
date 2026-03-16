SELECT case_id, activity, timestamp
FROM (
    SELECT 
        vo.visit_occurrence_id as case_id,
        'Specimen Collected' as activity,
        s.specimen_datetime as timestamp
    FROM specimen s
    JOIN person p ON s.person_id = p.person_id
    JOIN visit_occurrence vo ON p.person_id = vo.person_id
    WHERE s.specimen_datetime IS NOT NULL
    
    UNION ALL
    
    SELECT 
        vo.visit_occurrence_id as case_id,
        'Lab Result Recorded' as activity,
        m.measurement_datetime as timestamp
    FROM measurement m
    JOIN visit_occurrence vo ON m.visit_occurrence_id = vo.visit_occurrence_id
    WHERE m.measurement_datetime IS NOT NULL
    
    UNION ALL
    
    SELECT 
        vo.visit_occurrence_id as case_id,
        'Abnormal Result Flagged' as activity,
        m.measurement_datetime as timestamp
    FROM measurement m
    JOIN visit_occurrence vo ON m.visit_occurrence_id = vo.visit_occurrence_id
    WHERE m.measurement_datetime IS NOT NULL
    AND m.value_as_number IS NOT NULL
    AND m.range_low IS NOT NULL
    AND m.range_high IS NOT NULL
    AND (m.value_as_number < m.range_low OR m.value_as_number > m.range_high)
) events
WHERE timestamp IS NOT NULL
ORDER BY case_id, timestamp;