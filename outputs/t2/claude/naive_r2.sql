SELECT DISTINCT
    p.person_id AS case_id,
    CASE 
        WHEN de.drug_exposure_start_date IS NOT NULL AND de.drug_exposure_end_date IS NULL THEN 'Medication Prescribed'
        WHEN de.drug_exposure_start_date IS NOT NULL AND de.drug_exposure_end_date IS NOT NULL THEN 'Medication Administered'
        WHEN de.drug_exposure_end_date IS NOT NULL THEN 'Medication Discontinued'
    END AS activity,
    COALESCE(de.drug_exposure_start_datetime, de.drug_exposure_start_date::timestamp) AS timestamp
FROM person p
JOIN drug_exposure de ON p.person_id = de.person_id
JOIN visit_occurrence vo ON de.visit_occurrence_id = vo.visit_occurrence_id
JOIN concept c_visit ON vo.visit_concept_id = c_visit.concept_id
WHERE c_visit.concept_name IN ('Inpatient Visit', 'Emergency Room and Inpatient Visit')
    AND de.drug_exposure_start_date IS NOT NULL

UNION ALL

SELECT DISTINCT
    p.person_id AS case_id,
    'Medication Discontinued' AS activity,
    COALESCE(de.drug_exposure_end_datetime, de.drug_exposure_end_date::timestamp) AS timestamp
FROM person p
JOIN drug_exposure de ON p.person_id = de.person_id
JOIN visit_occurrence vo ON de.visit_occurrence_id = vo.visit_occurrence_id
JOIN concept c_visit ON vo.visit_concept_id = c_visit.concept_id
WHERE c_visit.concept_name IN ('Inpatient Visit', 'Emergency Room and Inpatient Visit')
    AND de.drug_exposure_end_date IS NOT NULL

ORDER BY case_id, timestamp;