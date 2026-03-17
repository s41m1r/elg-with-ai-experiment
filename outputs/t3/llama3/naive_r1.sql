WITH sepsis_diagnosis AS (
  SELECT person_id, visit_occurrence_id, condition_start_date AS timestamp, 'Sepsis Diagnosis' AS activity
  FROM condition_occurrence
  WHERE condition_concept_id IN (SELECT concept_id FROM concept WHERE concept_name IN ('Sepsis', 'Septicemia'))
),
lab_tests AS (
  SELECT person_id, visit_occurrence_id, specimen_date AS timestamp, 'Lab Test' AS activity
  FROM measurement_occurrence
  WHERE measurement_concept_id IN (SELECT concept_id FROM concept WHERE concept_name IN ('White Blood Cell Count', 'C-Reactive Protein', 'Lactate'))
),
antibiotics AS (
  SELECT person_id, visit_occurrence_id, drug_exposure_start_date AS timestamp, 'Antibiotics' AS activity
  FROM drug_exposure
  WHERE drug_concept_id IN (SELECT concept_id FROM concept WHERE concept_name IN ('Ceftriaxone', 'Vancomycin', 'Piperacillin-Tazobactam'))
),
vitals AS (
  SELECT person_id, visit_occurrence_id, measurement_date AS timestamp, 'Vitals' AS activity
  FROM measurement_occurrence
  WHERE measurement_concept_id IN (SELECT concept_id FROM concept WHERE concept_name IN ('Body Temperature', 'Heart Rate', 'Blood Pressure'))
)
SELECT 
  ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY timestamp) AS case_id,
  activity,
  timestamp
FROM (
  SELECT person_id, activity, timestamp
  FROM sepsis_diagnosis
  UNION ALL
  SELECT person_id, activity, timestamp
  FROM lab_tests
  UNION ALL
  SELECT person_id, activity, timestamp
  FROM antibiotics
  UNION ALL
  SELECT person_id, activity, timestamp
  FROM vitals
) AS subquery
ORDER BY case_id, timestamp;