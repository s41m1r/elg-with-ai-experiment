WITH sepsis_diagnoses AS (
    SELECT 
        co.visit_occurrence_id AS case_id,
        'Sepsis Diagnosed' AS activity,
        co.condition_start_datetime AS timestamp
    FROM condition_occurrence co
    WHERE co.condition_concept_id IN (132797, 4103023, 40479642)
      AND co.condition_start_datetime IS NOT NULL
      AND co.visit_occurrence_id IS NOT NULL
),

lab_results AS (
    SELECT 
        m.visit_occurrence_id AS case_id,
        'Lab Result Recorded' AS activity,
        m.measurement_datetime AS timestamp
    FROM measurement m
    WHERE m.measurement_datetime IS NOT NULL
      AND m.visit_occurrence_id IS NOT NULL
),

vital_signs AS (
    SELECT 
        m.visit_occurrence_id AS case_id,
        'Vital Sign Measured' AS activity,
        m.measurement_datetime AS timestamp
    FROM measurement m
    JOIN concept c ON m.measurement_concept_id = c.concept_id
    WHERE m.measurement_datetime IS NOT NULL
      AND m.visit_occurrence_id IS NOT NULL
      AND c.domain_id = 'Measurement'
      AND (c.concept_name ILIKE '%vital%' OR c.concept_name ILIKE '%blood pressure%' 
           OR c.concept_name ILIKE '%heart rate%' OR c.concept_name ILIKE '%temperature%'
           OR c.concept_name ILIKE '%respiratory rate%' OR c.concept_name ILIKE '%oxygen%')
),

antibiotic_starts AS (
    SELECT 
        de.visit_occurrence_id AS case_id,
        'Antibiotic Started' AS activity,
        de.drug_exposure_start_datetime AS timestamp
    FROM drug_exposure de
    JOIN concept c ON de.drug_concept_id = c.concept_id
    WHERE de.drug_exposure_start_datetime IS NOT NULL
      AND de.visit_occurrence_id IS NOT NULL
      AND (c.concept_name ILIKE '%antibiotic%' OR c.concept_name ILIKE '%penicillin%'
           OR c.concept_name ILIKE '%vancomycin%' OR c.concept_name ILIKE '%ceftriaxone%'
           OR c.concept_name ILIKE '%azithromycin%' OR c.concept_name ILIKE '%ciprofloxacin%'
           OR c.concept_name ILIKE '%clindamycin%' OR c.concept_name ILIKE '%doxycycline%')
),

antibiotic_ends AS (
    SELECT 
        de.visit_occurrence_id AS case_id,
        'Antibiotic Ended' AS activity,
        de.drug_exposure_end_datetime AS timestamp
    FROM drug_exposure de
    JOIN concept c ON de.drug_concept_id = c.concept_id
    WHERE de.drug_exposure_end_datetime IS NOT NULL
      AND de.visit_occurrence_id IS NOT NULL
      AND (c.concept_name ILIKE '%antibiotic%' OR c.concept_name ILIKE '%penicillin%'
           OR c.concept_name ILIKE '%vancomycin%' OR c.concept_name ILIKE '%ceftriaxone%'
           OR c.concept_name ILIKE '%azithromycin%' OR c.concept_name ILIKE '%ciprofloxacin%'
           OR c.concept_name ILIKE '%clindamycin%' OR c.concept_name ILIKE '%doxycycline%')
)

SELECT case_id, activity, timestamp
FROM sepsis_diagnoses
UNION ALL
SELECT case_id, activity, timestamp
FROM lab_results
UNION ALL
SELECT case_id, activity, timestamp
FROM vital_signs
UNION ALL
SELECT case_id, activity, timestamp
FROM antibiotic_starts
UNION ALL
SELECT case_id, activity, timestamp
FROM antibiotic_ends
ORDER BY case_id, timestamp;