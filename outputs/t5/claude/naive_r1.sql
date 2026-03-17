SELECT DISTINCT
    p.person_id AS case_id,
    CASE 
        WHEN vo.visit_concept_id = 9203 THEN 'ED_Arrival'
        WHEN c.concept_name LIKE '%Triage%' OR c.concept_name LIKE '%triage%' THEN 'Triage'
        WHEN c.concept_name LIKE '%Admission%' OR c.concept_name LIKE '%admission%' THEN 'Admission'
        WHEN c.concept_name LIKE '%Discharge%' OR c.concept_name LIKE '%discharge%' THEN 'Discharge'
        WHEN c.concept_name LIKE '%Transfer%' OR c.concept_name LIKE '%transfer%' THEN 'Transfer'
        WHEN vo.visit_end_date IS NOT NULL THEN 'ED_Disposition'
        ELSE c.concept_name
    END AS activity,
    COALESCE(
        vo.visit_start_datetime,
        po.procedure_datetime,
        o.observation_datetime,
        vo.visit_start_date::timestamp
    ) AS timestamp
FROM visit_occurrence vo
JOIN person p ON vo.person_id = p.person_id
JOIN concept c_visit ON vo.visit_concept_id = c_visit.concept_id
LEFT JOIN procedure_occurrence po ON vo.visit_occurrence_id = po.visit_occurrence_id
LEFT JOIN concept c ON po.procedure_concept_id = c.concept_id
LEFT JOIN observation o ON vo.visit_occurrence_id = o.visit_occurrence_id
LEFT JOIN concept c2 ON o.observation_concept_id = c2.concept_id
WHERE vo.visit_concept_id = 9203  -- Emergency Room Visit
   OR c_visit.concept_name ILIKE '%emergency%'
   OR c_visit.concept_name ILIKE '%ED%'

UNION ALL

SELECT DISTINCT
    p.person_id AS case_id,
    'ED_Disposition' AS activity,
    COALESCE(vo.visit_end_datetime, vo.visit_end_date::timestamp) AS timestamp
FROM visit_occurrence vo
JOIN person p ON vo.person_id = p.person_id
JOIN concept c_visit ON vo.visit_concept_id = c_visit.concept_id
WHERE (vo.visit_concept_id = 9203 OR c_visit.concept_name ILIKE '%emergency%')
  AND vo.visit_end_date IS NOT NULL

ORDER BY case_id, timestamp;