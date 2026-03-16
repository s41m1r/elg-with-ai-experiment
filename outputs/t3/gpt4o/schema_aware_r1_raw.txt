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
    'Vital Sign Measured' AS activity,
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
WHERE
    drug_exposure_start_datetime IS NOT NULL

UNION ALL

SELECT
    visit_occurrence_id AS case_id,
    'Antibiotic Ended' AS activity,
    drug_exposure_end_datetime AS timestamp
FROM
    drug_exposure
WHERE
    drug_exposure_end_datetime IS NOT NULL

UNION ALL

SELECT
    visit_occurrence_id AS case_id,
    'Clinical Observation' AS activity,
    observation_datetime AS timestamp
FROM
    observation
WHERE
    observation_datetime IS NOT NULL

ORDER BY
    case_id, timestamp;