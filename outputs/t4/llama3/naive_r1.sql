WITH lab_order AS (
  SELECT person_id, visit_occurrence_id, procedure_date AS order_date, 
         'Lab Order' AS activity
  FROM procedure_occurrence
  WHERE procedure_concept_id IN (SELECT concept_id FROM concept 
                                  WHERE concept_name IN ('Lab Order', 'Laboratory Order'))
),
specimen_collection AS (
  SELECT person_id, visit_occurrence_id, procedure_date AS collection_date, 
         'Specimen Collection' AS activity
  FROM procedure_occurrence
  WHERE procedure_concept_id IN (SELECT concept_id FROM concept 
                                   WHERE concept_name IN ('Specimen Collection', 'Blood Collection'))
),
lab_result AS (
  SELECT person_id, visit_occurrence_id, measurement_date AS result_date, 
         'Lab Result' AS activity
  FROM measurement
  WHERE measurement_concept_id IN (SELECT concept_id FROM concept 
                                    WHERE concept_name IN ('Lab Result', 'Laboratory Result'))
)
SELECT DISTINCT 
  ROW_NUMBER() OVER (PARTITION BY po.person_id, po.visit_occurrence_id ORDER BY po.procedure_date) AS case_id,
  COALESCE(lo.activity, sc.activity, lr.activity) AS activity,
  COALESCE(lo.order_date, sc.collection_date, lr.result_date) AS timestamp
FROM procedure_occurrence po
LEFT JOIN lab_order lo ON po.person_id = lo.person_id AND po.visit_occurrence_id = lo.visit_occurrence_id
LEFT JOIN specimen_collection sc ON po.person_id = sc.person_id AND po.visit_occurrence_id = sc.visit_occurrence_id
LEFT JOIN lab_result lr ON po.person_id = lr.person_id AND po.visit_occurrence_id = lr.visit_occurrence_id
WHERE po.procedure_concept_id IN (SELECT concept_id FROM concept 
                                   WHERE concept_name IN ('Lab Order', 'Laboratory Order', 
                                                          'Specimen Collection', 'Blood Collection', 
                                                          'Lab Result', 'Laboratory Result'))
ORDER BY case_id, timestamp;