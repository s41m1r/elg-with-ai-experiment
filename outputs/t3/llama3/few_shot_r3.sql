SELECT 
    vo.visit_occurrence_id AS case_id,
    c.concept_name AS activity,
    co.condition_start_datetime AS timestamp
FROM 
    condition_occurrence co
JOIN 
    concept c ON co.condition_concept_id = c.concept_id
JOIN 
    visit_occurrence vo ON co.visit_occurrence_id = vo.visit_occurrence_id
WHERE 
    co.condition_concept_id IN (132797, 4103023, 40479642)
    AND co.condition_start_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    c.concept_name AS activity,
    m.measurement_datetime AS timestamp
FROM 
    measurement m
JOIN 
    concept c ON m.measurement_concept_id = c.concept_id
JOIN 
    visit_occurrence vo ON m.visit_occurrence_id = vo.visit_occurrence_id
WHERE 
    m.measurement_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    'Antibiotic Started' AS activity,
    de.drug_exposure_start_datetime AS timestamp
FROM 
    drug_exposure de
JOIN 
    visit_occurrence vo ON de.visit_occurrence_id = vo.visit_occurrence_id
WHERE 
    de.drug_exposure_start_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    'Antibiotic Ended' AS activity,
    de.drug_exposure_end_datetime AS timestamp
FROM 
    drug_exposure de
JOIN 
    visit_occurrence vo ON de.visit_occurrence_id = vo.visit_occurrence_id
WHERE 
    de.drug_exposure_end_datetime IS NOT NULL

UNION ALL

SELECT 
    vo.visit_occurrence_id AS case_id,
    c.concept_name AS activity,
    o.observation_datetime AS timestamp
FROM 
    observation o
JOIN 
    concept c ON o.observation_concept_id = c.concept_id
JOIN 
    visit_occurrence vo ON o.visit_occurrence_id = vo.visit_occurrence_id
WHERE 
    o.observation_datetime IS NOT NULL

ORDER BY 
    case_id, 
    timestamp;