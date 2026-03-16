SELECT
    vo.visit_occurrence_id AS case_id,
    'Hospital Admission' AS activity,
    vo.visit_start_datetime AS timestamp
FROM visit_occurrence vo
WHERE vo.visit_start_datetime IS NOT NULL

UNION ALL

SELECT
    vd.visit_occurrence_id AS case_id,
    'ICU Admission' AS activity,
    vd.visit_detail_start_datetime AS timestamp
FROM visit_detail vd
JOIN concept c ON vd.visit_detail_concept_id = c.concept_id
WHERE vd.visit_detail_start_datetime IS NOT NULL
  AND c.concept_name = 'ICU'

UNION ALL

SELECT
    po.visit_occurrence_id AS case_id,
    'ICU Procedure: ' || c.concept_name AS activity,
    po.procedure_datetime AS timestamp
FROM procedure_occurrence po
JOIN concept c ON po.procedure_concept_id = c.concept_id
WHERE po.procedure_datetime IS NOT NULL
  AND po.visit_detail_id IN (
      SELECT vd.visit_detail_id
      FROM visit_detail vd
      JOIN concept c ON vd.visit_detail_concept_id = c.concept_id
      WHERE c.concept_name = 'ICU'
  )

UNION ALL

SELECT
    vd.visit_occurrence_id AS case_id,
    'ICU Discharge' AS activity,
    vd.visit_detail_end_datetime AS timestamp
FROM visit_detail vd
JOIN concept c ON vd.visit_detail_concept_id = c.concept_id
WHERE vd.visit_detail_end_datetime IS NOT NULL
  AND c.concept_name = 'ICU'

UNION ALL

SELECT
    vo.visit_occurrence_id AS case_id,
    'Hospital Discharge' AS activity,
    vo.visit_end_datetime AS timestamp
FROM visit_occurrence vo
WHERE vo.visit_end_datetime IS NOT NULL

ORDER BY case_id, timestamp;