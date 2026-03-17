SELECT DISTINCT
    p.person_id AS case_id,
    CASE 
        WHEN m.measurement_concept_id IS NOT NULL THEN 'Lab Order Placed'
        WHEN sp.procedure_concept_id IS NOT NULL THEN 'Specimen Collection'
        WHEN m.value_as_number IS NOT NULL OR m.value_as_concept_id IS NOT NULL THEN 'Result Recording'
    END AS activity,
    COALESCE(m.measurement_datetime, m.measurement_date, sp.procedure_datetime, sp.procedure_date) AS timestamp
FROM person p
LEFT JOIN measurement m ON p.person_id = m.person_id
LEFT JOIN procedure_occurrence sp ON p.person_id = sp.person_id 
    AND sp.procedure_concept_id IN (
        SELECT concept_id FROM concept 
        WHERE domain_id = 'Procedure' 
        AND (concept_name ILIKE '%specimen%' OR concept_name ILIKE '%collection%' OR concept_name ILIKE '%draw%')
    )
WHERE (
    m.measurement_concept_id IN (
        SELECT concept_id FROM concept 
        WHERE domain_id = 'Measurement' 
        AND concept_class_id = 'Lab Test'
    )
    OR sp.procedure_concept_id IS NOT NULL
)
AND COALESCE(m.measurement_datetime, m.measurement_date, sp.procedure_datetime, sp.procedure_date) IS NOT NULL
ORDER BY case_id, timestamp;