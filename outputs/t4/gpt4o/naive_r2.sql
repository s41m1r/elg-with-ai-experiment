SELECT
    lo.order_id AS case_id,
    'Specimen Collection' AS activity,
    lo.order_datetime AS timestamp
FROM
    lab_orders lo
JOIN
    specimen sp ON lo.order_id = sp.order_id
WHERE
    lo.order_concept_id IN (SELECT concept_id FROM concept WHERE concept_name = 'Specimen Collection')
UNION ALL
SELECT
    lr.order_id AS case_id,
    'Result Recording' AS activity,
    lr.result_datetime AS timestamp
FROM
    lab_results lr
JOIN
    lab_orders lo ON lr.order_id = lo.order_id
WHERE
    lr.result_concept_id IN (SELECT concept_id FROM concept WHERE concept_name = 'Result Recording')
ORDER BY
    case_id, timestamp;