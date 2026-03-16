SELECT 
    de.visit_occurrence_id as case_id,
    'Drug Exposure Started: ' || c.concept_name as activity,
    de.drug_exposure_start_datetime as timestamp
FROM drug_exposure de
JOIN concept c ON de.drug_concept_id = c.concept_id
WHERE de.drug_exposure_start_datetime IS NOT NULL
  AND de.visit_occurrence_id IS NOT NULL

UNION ALL

SELECT 
    de.visit_occurrence_id as case_id,
    'Drug Exposure Ended: ' || c.concept_name as activity,
    de.drug_exposure_end_datetime as timestamp
FROM drug_exposure de
JOIN concept c ON de.drug_concept_id = c.concept_id
WHERE de.drug_exposure_end_datetime IS NOT NULL
  AND de.visit_occurrence_id IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id as case_id,
    'Drug Era Started: ' || c.concept_name as activity,
    CAST(dr.drug_era_start_date AS TIMESTAMP) as timestamp
FROM drug_era dr
JOIN concept c ON dr.drug_concept_id = c.concept_id
JOIN visit_occurrence vo ON dr.person_id = vo.person_id
WHERE dr.drug_era_start_date IS NOT NULL
  AND CAST(dr.drug_era_start_date AS TIMESTAMP) >= vo.visit_start_datetime
  AND CAST(dr.drug_era_start_date AS TIMESTAMP) <= COALESCE(vo.visit_end_datetime, vo.visit_start_datetime + INTERVAL '365 days')

UNION ALL

SELECT 
    vo.visit_occurrence_id as case_id,
    'Drug Era Ended: ' || c.concept_name as activity,
    CAST(dr.drug_era_end_date AS TIMESTAMP) as timestamp
FROM drug_era dr
JOIN concept c ON dr.drug_concept_id = c.concept_id
JOIN visit_occurrence vo ON dr.person_id = vo.person_id
WHERE dr.drug_era_end_date IS NOT NULL
  AND CAST(dr.drug_era_end_date AS TIMESTAMP) >= vo.visit_start_datetime
  AND CAST(dr.drug_era_end_date AS TIMESTAMP) <= COALESCE(vo.visit_end_datetime, vo.visit_start_datetime + INTERVAL '365 days')

ORDER BY case_id, timestamp;