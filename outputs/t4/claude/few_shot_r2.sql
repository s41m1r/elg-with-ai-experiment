WITH specimen_events AS (
    SELECT 
        vo.visit_occurrence_id as case_id,
        'Specimen Collected: ' || COALESCE(c.concept_name, 'Unknown') as activity,
        s.specimen_datetime as timestamp
    FROM specimen s
    JOIN visit_occurrence vo ON s.person_id = vo.person_id
    LEFT JOIN concept c ON s.specimen_concept_id = c.concept_id
    WHERE s.specimen_datetime IS NOT NULL
),
lab_result_events AS (
    SELECT 
        m.visit_occurrence_id as case_id,
        'Lab Result Recorded: ' || COALESCE(c.concept_name, 'Unknown') as activity,
        m.measurement_datetime as timestamp
    FROM measurement m
    LEFT JOIN concept c ON m.measurement_concept_id = c.concept_id
    WHERE m.measurement_datetime IS NOT NULL
      AND m.visit_occurrence_id IS NOT NULL
),
abnormal_result_events AS (
    SELECT 
        m.visit_occurrence_id as case_id,
        'Abnormal Result Flagged: ' || COALESCE(c.concept_name, 'Unknown') as activity,
        m.measurement_datetime as timestamp
    FROM measurement m
    LEFT JOIN concept c ON m.measurement_concept_id = c.concept_id
    WHERE m.measurement_datetime IS NOT NULL
      AND m.visit_occurrence_id IS NOT NULL
      AND m.value_as_number IS NOT NULL
      AND m.range_low IS NOT NULL
      AND m.range_high IS NOT NULL
      AND (m.value_as_number < m.range_low OR m.value_as_number > m.range_high)
)
SELECT case_id, activity, timestamp
FROM specimen_events
UNION ALL
SELECT case_id, activity, timestamp
FROM lab_result_events
UNION ALL
SELECT case_id, activity, timestamp
FROM abnormal_result_events
ORDER BY case_id, timestamp;