WITH icu_stays AS (
  SELECT person_id, visit_occurrence_id, 
         MIN(date_time) AS icu_admit_time, 
         MAX(date_time) AS icu_discharge_time
  FROM visit_detail
  WHERE visit_detail_concept_id = 32037  -- ICU stay
  GROUP BY person_id, visit_occurrence_id
),
icu_transfers AS (
  SELECT person_id, visit_occurrence_id, 
         date_time AS transfer_time, 
         transfer_type_concept_id
  FROM transfer
  WHERE transfer_type_concept_id IN (45877995, 45877996)  -- ICU transfer
),
discharges AS (
  SELECT person_id, visit_occurrence_id, 
         date_time AS discharge_time
  FROM visit_detail
  WHERE visit_detail_concept_id = 44818754  -- Discharge
)
SELECT 
  COALESCE(icu_stays.visit_occurrence_id, icu_transfers.visit_occurrence_id, discharges.visit_occurrence_id) AS case_id,
  CASE 
    WHEN icu_stays.icu_admit_time IS NOT NULL THEN 'ICU Admission'
    WHEN icu_transfers.transfer_time IS NOT NULL THEN 
      CASE 
        WHEN icu_transfers.transfer_type_concept_id = 45877995 THEN 'ICU Transfer In'
        WHEN icu_transfers.transfer_type_concept_id = 45877996 THEN 'ICU Transfer Out'
      END
    WHEN discharges.discharge_time IS NOT NULL THEN 'Discharge'
  END AS activity,
  COALESCE(icu_stays.icu_admit_time, icu_transfers.transfer_time, discharges.discharge_time) AS timestamp
FROM icu_stays
FULL OUTER JOIN icu_transfers 
  ON icu_stays.person_id = icu_transfers.person_id 
  AND icu_stays.visit_occurrence_id = icu_transfers.visit_occurrence_id
FULL OUTER JOIN discharges 
  ON icu_stays.person_id = discharges.person_id 
  AND icu_stays.visit_occurrence_id = discharges.visit_occurrence_id
ORDER BY case_id, timestamp;