SELECT 
    visit_occurrence.visit_occurrence_id AS case_id,
    concept.concept_name AS activity,
    drug_exposure.drug_exposure_start_datetime AS timestamp
FROM 
    drug_exposure
JOIN 
    visit_occurrence ON drug_exposure.visit_occurrence_id = visit_occurrence.visit_occurrence_id
JOIN 
    concept ON drug_exposure.drug_concept_id = concept.concept_id
WHERE 
    concept.domain_id = 'Drug'
    AND visit_occurrence.visit_concept_id IN (
        SELECT concept_id 
        FROM concept 
        WHERE concept_name = 'Inpatient Visit'
    )
ORDER BY 
    case_id, timestamp;