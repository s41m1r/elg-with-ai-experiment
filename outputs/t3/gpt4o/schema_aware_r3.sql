SELECT
    co.visit_occurrence_id AS case_id,
    'Sepsis Diagnosed' AS activity,
    co.condition_start_datetime AS timestamp
FROM
    condition_occurrence co
JOIN
    concept c ON co.condition_concept_id = c.concept_id
WHERE
    co.condition_concept_id IN (132797, 4103023, 40479642)
    AND co.condition_start_datetime IS NOT NULL

UNION ALL

SELECT
    m.visit_occurrence_id AS case_id,
    'Lab Result Recorded' AS activity,
    m.measurement_datetime AS timestamp
FROM
    measurement m
WHERE
    m.measurement_datetime IS NOT NULL

UNION ALL

SELECT
    de.visit_occurrence_id AS case_id,
    'Antibiotic Started' AS activity,
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
    'Antibiotic Ended' AS activity,
    de.drug_exposure_end_datetime AS timestamp
FROM
    drug_exposure de
JOIN
    concept c ON de.drug_concept_id = c.concept_id
WHERE
    de.drug_exposure_end_datetime IS NOT NULL

UNION ALL

SELECT
    m.visit_occurrence_id AS case_id,
    'Vital Sign Measured' AS activity,
    m.measurement_datetime AS timestamp
FROM
    measurement m
WHERE
    m.measurement_datetime IS NOT NULL

ORDER BY
    case_id, timestamp;