SELECT 
  de.visit_occurrence_id AS case_id,
  'Drug Exposure Started: ' || c.concept_name AS activity,
  de.drug_exposure_start_datetime AS timestamp
FROM 
  drug_exposure de
  JOIN concept c ON de.drug_concept_id = c.concept_id
WHERE 
  de.drug_exposure_start_datetime IS NOT NULL
UNION ALL
SELECT 
  de.visit_occurrence_id AS case_id,
  'Drug Exposure Ended: ' || c.concept_name AS activity,
  de.drug_exposure_end_datetime AS timestamp
FROM 
  drug_exposure de
  JOIN concept c ON de.drug_concept_id = c.concept_id
WHERE 
  de.drug_exposure_end_datetime IS NOT NULL
UNION ALL
SELECT 
  de.visit_occurrence_id AS case_id,
  'Drug Era Started: ' || c.concept_name AS activity,
  de_start.drug_era_start_date AS timestamp
FROM 
  drug_era de
  JOIN concept c ON de.drug_concept_id = c.concept_id
  JOIN visit_occurrence vo ON de.person_id = vo.person_id
WHERE 
  de.drug_era_start_date IS NOT NULL
UNION ALL
SELECT 
  de.visit_occurrence_id AS case_id,
  'Drug Era Ended: ' || c.concept_name AS activity,
  de_end.drug_era_end_date AS timestamp
FROM 
  drug_era de
  JOIN concept c ON de.drug_concept_id = c.concept_id
  JOIN visit_occurrence vo ON de.person_id = vo.person_id
WHERE 
  de.drug_era_end_date IS NOT NULL
ORDER BY 
  case_id, 
  timestamp;