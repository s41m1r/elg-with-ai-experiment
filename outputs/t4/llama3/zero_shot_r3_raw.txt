SELECT 
  vo.visit_occurrence_id AS case_id,
  c1.concept_name AS activity,
  s.specimen_datetime AS timestamp
FROM 
  specimen s
  JOIN visit_occurrence vo ON s.visit_occurrence_id = vo.visit_occurrence_id
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
  JOIN visit_occurrence vo ON m.visit_occurrence_id = vo.visit_occurrence_id
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
  JOIN visit_occurrence vo ON m.visit_occurrence_id = vo.visit_occurrence_id
WHERE 
  m.measurement_datetime IS NOT NULL
  AND (m.value_as_number < (SELECT range_low FROM concept c3 WHERE c3.concept_id = m.measurement_concept_id)
       OR m.value_as_number > (SELECT range_high FROM concept c3 WHERE c3.concept_id = m.measurement_concept_id))
ORDER BY 
  case_id, 
  timestamp;