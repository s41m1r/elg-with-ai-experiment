WITH icu_visits AS (
    SELECT DISTINCT
        vo.person_id,
        vo.visit_occurrence_id,
        vo.visit_start_datetime,
        vo.visit_end_datetime
    FROM visit_occurrence vo
    JOIN care_site cs ON vo.care_site_id = cs.care_site_id
    WHERE UPPER(cs.care_site_name) LIKE '%ICU%' 
       OR UPPER(cs.care_site_name) LIKE '%INTENSIVE CARE%'
       OR cs.place_of_service_concept_id IN (
           SELECT concept_id 
           FROM concept 
           WHERE UPPER(concept_name) LIKE '%INTENSIVE CARE%'
       )
),
event_log AS (
    -- ICU Admission
    SELECT 
        CAST(iv.person_id AS VARCHAR) || '_' || CAST(iv.visit_occurrence_id AS VARCHAR) as case_id,
        'ICU_Admission' as activity,
        iv.visit_start_datetime as timestamp
    FROM icu_visits iv
    
    UNION ALL
    
    -- ICU Transfers (location changes within ICU stay)
    SELECT 
        CAST(iv.person_id AS VARCHAR) || '_' || CAST(iv.visit_occurrence_id AS VARCHAR) as case_id,
        'ICU_Transfer_to_' || COALESCE(cs.care_site_name, 'Unknown_Unit') as activity,
        vd.visit_detail_start_datetime as timestamp
    FROM icu_visits iv
    JOIN visit_detail vd ON iv.visit_occurrence_id = vd.visit_occurrence_id
    JOIN care_site cs ON vd.care_site_id = cs.care_site_id
    WHERE vd.visit_detail_start_datetime > iv.visit_start_datetime
      AND vd.visit_detail_start_datetime < COALESCE(iv.visit_end_datetime, CURRENT_TIMESTAMP)
    
    UNION ALL
    
    -- Major procedures during ICU stay
    SELECT 
        CAST(iv.person_id AS VARCHAR) || '_' || CAST(iv.visit_occurrence_id AS VARCHAR) as case_id,
        'Procedure_' || c.concept_name as activity,
        po.procedure_datetime as timestamp
    FROM icu_visits iv
    JOIN procedure_occurrence po ON iv.person_id = po.person_id 
        AND po.procedure_datetime BETWEEN iv.visit_start_datetime 
        AND COALESCE(iv.visit_end_datetime, CURRENT_TIMESTAMP)
    JOIN concept c ON po.procedure_concept_id = c.concept_id
    WHERE po.visit_occurrence_id = iv.visit_occurrence_id
    
    UNION ALL
    
    -- ICU Discharge
    SELECT 
        CAST(iv.person_id AS VARCHAR) || '_' || CAST(iv.visit_occurrence_id AS VARCHAR) as case_id,
        'ICU_Discharge' as activity,
        iv.visit_end_datetime as timestamp
    FROM icu_visits iv
    WHERE iv.visit_end_datetime IS NOT NULL
)
SELECT 
    case_id,
    activity,
    timestamp
FROM event_log
WHERE timestamp IS NOT NULL
ORDER BY case_id, timestamp;