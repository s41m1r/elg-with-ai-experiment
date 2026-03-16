SELECT 
    visit_occurrence_id AS case_id,
    'Sepsis Diagnosed' AS activity,
    condition_start_datetime AS timestamp
FROM 
    condition_occurrence
JOIN 
    concept ON condition_occurrence.condition_concept_id = concept.concept_id
WHERE 
    condition_concept_id IN (132797, 4103023, 40479642)
    AND condition_start_datetime IS NOT NULL

UNION ALL

SELECT 
    visit_occurrence_id AS case_id,
    'Lab Result Recorded' AS activity,
    measurement_datetime AS timestamp
FROM 
    measurement
WHERE 
    measurement_datetime IS NOT NULL

UNION ALL

SELECT 
    visit_occurrence_id AS case_id,
    'Antibiotic Started' AS activity,
    drug_exposure_start_datetime AS timestamp
FROM 
    drug_exposure
JOIN 
    concept ON drug_exposure.drug_concept_id = concept.concept_id
WHERE 
    drug_exposure_start_datetime IS NOT NULL
    AND concept.concept_class_id = 'Antibiotic'

UNION ALL

SELECT 
    visit_occurrence_id AS case_id,
    'Antibiotic Ended' AS activity,
    drug_exposure_end_datetime AS timestamp
FROM 
    drug_exposure
JOIN 
    concept ON drug_exposure.drug_concept_id = concept.concept_id
WHERE 
    drug_exposure_end_datetime IS NOT NULL
    AND concept.concept_class_id = 'Antibiotic'

UNION ALL

SELECT 
    visit_occurrence_id AS case_id,
    'Vital Sign Measured' AS activity,
    measurement_datetime AS timestamp
FROM 
    measurement
JOIN 
    concept ON measurement.measurement_concept_id = concept.concept_id
WHERE 
    measurement_datetime IS NOT NULL
    AND concept.concept_class_id = 'Vital Sign'

ORDER BY 
    case_id, timestamp;