SELECT
    visit_occurrence.visit_occurrence_id AS case_id,
    'Medication Administration' AS activity,
    drug_exposure.drug_exposure_start_datetime AS timestamp
FROM
    visit_occurrence
JOIN
    drug_exposure ON visit_occurrence.person_id = drug_exposure.person_id
WHERE
    visit_occurrence.visit_concept_id IN (
        SELECT concept_id
        FROM concept
        WHERE concept_name = 'Inpatient Visit'
    )
    AND drug_exposure.drug_type_concept_id IN (
        SELECT concept_id
        FROM concept
        WHERE concept_name = 'Medication Administration'
    )
ORDER BY
    case_id, timestamp;