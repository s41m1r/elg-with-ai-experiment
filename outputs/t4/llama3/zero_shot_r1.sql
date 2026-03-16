SELECT 
  vo.visit_occurrence_id AS case_id,
  c1.concept_name AS activity,
  s.specimen_datetime AS timestamp
FROM 
  specimen s
  JOIN visit_occurrence vo ON s.person_id = vo.person_id
  JOIN concept c1 ON s.specimen_concept_id = c1.concept_id
WHERE 
  s.specimen_datetime IS NOT NULL
UNION ALL
SELECT 
  vo.visit_occurrence_id AS case_id,
  c2.concept_name AS activity,
  m.measurement_datetime AS timestamp
FROM 
  measurement m
  JOIN visit_occurrence vo ON m.person_id = vo.person_id
  JOIN concept c2 ON m.measurement_concept_id = c2.concept_id
WHERE 
  m.measurement_datetime IS NOT NULL
UNION ALL
SELECT 
  vo.visit_occurrence_id AS case_id,
  'Abnormal Result Flagged' AS activity,
  m.measurement_datetime AS timestamp
FROM 
  measurement m
  JOIN visit_occurrence vo ON m.person_id = vo.person_id
WHERE 
  m.measurement_datetime IS NOT NULL
  AND (m.value_as_number < m.range_low OR m.value_as_number > m.range_high)
ORDER BY 
  case_id, 
  timestamp;