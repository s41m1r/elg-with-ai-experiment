SELECT 
    de.visit_occurrence_id AS case_id,
    'Drug Exposure Started: ' || c.concept_name AS activity,
    de.drug_exposure_start_datetime AS timestamp
FROM 
    drug_exposure de
JOIN 
    concept c ON de.drug_concept_id = c.concept_id
WHERE 
    de.drug_exposure_start_datetime IS NOT NULL

UNION ALL

SELECT 
    de.visit_occurrence_id AS case_id,
    'Drug Exposure Ended: ' || c.concept_name AS activity,
    de.drug_exposure_end_datetime AS timestamp
FROM 
    drug_exposure de
JOIN 
    concept c ON de.drug_concept_id = c.concept_id
WHERE 
    de.drug_exposure_end_datetime IS NOT NULL

UNION ALL

SELECT 
    de.visit_occurrence_id AS case_id,
    'Drug Era Started: ' || c.concept_name AS activity,
    CAST(dr.drug_era_start_date AS TIMESTAMP) AS timestamp
FROM 
    drug_era dr
JOIN 
    concept c ON dr.drug_concept_id = c.concept_id
JOIN 
    drug_exposure de ON dr.person_id = de.person_id AND dr.drug_concept_id = de.drug_concept_id

UNION ALL

SELECT 
    de.visit_occurrence_id AS case_id,
    'Drug Era Ended: ' || c.concept_name AS activity,
    CAST(dr.drug_era_end_date AS TIMESTAMP) AS timestamp
FROM 
    drug_era dr
JOIN 
    concept c ON dr.drug_concept_id = c.concept_id
JOIN 
    drug_exposure de ON dr.person_id = de.person_id AND dr.drug_concept_id = de.drug_concept_id

ORDER BY 
    case_id, 
    timestamp;