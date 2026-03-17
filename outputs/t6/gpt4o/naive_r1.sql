SELECT
    visit_occurrence.visit_occurrence_id AS case_id,
    'Admission' AS activity,
    visit_occurrence.visit_start_datetime AS timestamp
FROM
    visit_occurrence
WHERE
    visit_occurrence.visit_concept_id IN (9201, 262) -- Inpatient Visit

UNION ALL

SELECT
    visit_occurrence.visit_occurrence_id AS case_id,
    'Diagnosis' AS activity,
    condition_occurrence.condition_start_datetime AS timestamp
FROM
    visit_occurrence
JOIN
    condition_occurrence ON visit_occurrence.person_id = condition_occurrence.person_id
WHERE
    visit_occurrence.visit_concept_id IN (9201, 262) -- Inpatient Visit
    AND condition_occurrence.visit_occurrence_id = visit_occurrence.visit_occurrence_id

UNION ALL

SELECT
    visit_occurrence.visit_occurrence_id AS case_id,
    'Discharge' AS activity,
    visit_occurrence.visit_end_datetime AS timestamp
FROM
    visit_occurrence
WHERE
    visit_occurrence.visit_concept_id IN (9201, 262) -- Inpatient Visit
ORDER BY
    case_id, timestamp;