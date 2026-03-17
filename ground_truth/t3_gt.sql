-- Ground-truth SQL for Task T3: Sepsis Treatment Trajectory
-- Author: 
-- Date: 2026-03-13
-- Data: MIMIC-IV in OMOP CDM format (PostgreSQL)
--
-- Output columns: case_id (BIGINT), activity (VARCHAR), timestamp (TIMESTAMP)
-- Case ID semantics: visit_occurrence_id (hospital admission)
--
-- Description: Track diagnostic and treatment steps for patients with sepsis:
--   diagnosis → lab tests → drug administration → vitals monitoring.
--
-- Tables: condition_occurrence, measurement, drug_exposure, observation, concept
-- Complexity: Very High
--
-- Sepsis concept list: expanded by Zeinab Soleimani via OMOP concept_ancestor
--   traversal from core sepsis SNOMED codes. Full list of 129 concepts used here.
--
-- Note: CDMPHI uses XTN_IS_ABNORMAL_RESULT and XTN_ORDER_DATETIME flags.
--   In MIMIC-IV OMOP, abnormal results are identified via value_as_number vs
--   range_low/range_high. Vitals are in the measurement table (from chartevents),
--   not observation. Drug ordering time is not available; start datetime is used.

WITH sepsis_visits AS (
    SELECT DISTINCT co.visit_occurrence_id
    FROM condition_occurrence co
    WHERE co.condition_concept_id IN (
        132797, 40487101, 40484176, 37017557, 44784136, 44782822, 44805136,
        37394658, 46284901, 4000938, 4102318, 4029281, 4285746, 37163133,
        42690418, 46284902, 35622880, 35622881, 4111261, 133594, 37163131,
        4009954, 44784138, 36674642, 4071063, 4029251, 40486058, 44782631,
        37164410, 40487062, 40487063, 40486629, 607321, 40489980, 37018498,
        607325, 44782630, 37312594, 607320, 37167232, 37167199, 37163129,
        37151377, 37163132, 40491961, 40493039, 40493415, 40487059, 37116435,
        46269944, 46269946, 37016131, 46270051, 40489908, 37163128, 37162933,
        40489979, 40486685, 42539372, 36715806, 36715567, 602996, 40489913,
        36717263, 42538750, 37018499, 40487064, 40489907, 46269807, 40491523,
        4154698, 37163134, 45757198, 40489912, 40487616, 37163130, 40486631,
        40486059, 40489909, 761851, 761852, 46284320, 46287153, 37163135,
        44784137, 37168933, 45768767, 1244226, 4048275, 37019087, 37162984,
        36716312, 37017566, 40493038, 40489910, 40487617, 603033, 763165,
        37395591, 36715430, 36684427, 1075272, 4073090, 4071727, 4048594,
        42536689, 42536690, 40487662, 40491960, 763027, 3655975, 46270052,
        46270041, 1076394, 36716754, 760987, 4197963, 760981, 760984,
        37018755, 37395517, 37395520, 37175321, 4103655, 37164448, 4124677,
        4121450, 3655135, 4028062
    )
    AND co.visit_occurrence_id IS NOT NULL
)

-- ─── Activity 1: Sepsis Diagnosed ─────────────────────────────────────────
SELECT
    co.visit_occurrence_id AS case_id,
    'Sepsis Diagnosed: ' || c.concept_name AS activity,
    co.condition_start_datetime AS timestamp
FROM condition_occurrence co
JOIN concept c ON co.condition_concept_id = c.concept_id
WHERE co.condition_concept_id IN (
    132797, 40487101, 40484176, 37017557, 44784136, 44782822, 44805136,
    37394658, 46284901, 4000938, 4102318, 4029281, 4285746, 37163133,
    42690418, 46284902, 35622880, 35622881, 4111261, 133594, 37163131,
    4009954, 44784138, 36674642, 4071063, 4029251, 40486058, 44782631,
    37164410, 40487062, 40487063, 40486629, 607321, 40489980, 37018498,
    607325, 44782630, 37312594, 607320, 37167232, 37167199, 37163129,
    37151377, 37163132, 40491961, 40493039, 40493415, 40487059, 37116435,
    46269944, 46269946, 37016131, 46270051, 40489908, 37163128, 37162933,
    40489979, 40486685, 42539372, 36715806, 36715567, 602996, 40489913,
    36717263, 42538750, 37018499, 40487064, 40489907, 46269807, 40491523,
    4154698, 37163134, 45757198, 40489912, 40487616, 37163130, 40486631,
    40486059, 40489909, 761851, 761852, 46284320, 46287153, 37163135,
    44784137, 37168933, 45768767, 1244226, 4048275, 37019087, 37162984,
    36716312, 37017566, 40493038, 40489910, 40487617, 603033, 763165,
    37395591, 36715430, 36684427, 1075272, 4073090, 4071727, 4048594,
    42536689, 42536690, 40487662, 40491960, 763027, 3655975, 46270052,
    46270041, 1076394, 36716754, 760987, 4197963, 760981, 760984,
    37018755, 37395517, 37395520, 37175321, 4103655, 37164448, 4124677,
    4121450, 3655135, 4028062
)
  AND co.condition_start_datetime IS NOT NULL
  AND co.visit_occurrence_id IS NOT NULL

UNION ALL

-- ─── Activity 2: Lab Result Recorded ──────────────────────────────────────
-- For sepsis visits only; abnormal flag derived from value vs range
SELECT
    m.visit_occurrence_id AS case_id,
    'Lab Result: ' || c.concept_name
        || CASE
            WHEN m.value_as_number IS NOT NULL
             AND m.range_low IS NOT NULL
             AND m.range_high IS NOT NULL
             AND (m.value_as_number < m.range_low OR m.value_as_number > m.range_high)
            THEN ' [ABNORMAL]'
            ELSE ''
           END AS activity,
    m.measurement_datetime AS timestamp
FROM measurement m
JOIN concept c ON m.measurement_concept_id = c.concept_id
WHERE m.visit_occurrence_id IN (SELECT visit_occurrence_id FROM sepsis_visits)
  AND m.measurement_datetime IS NOT NULL

UNION ALL

-- ─── Activity 3: Drug Exposure Started ────────────────────────────────────
-- All drugs for sepsis visits (antibiotic filtering not applied —
-- no reliable pharmaceutical class column in MIMIC-IV OMOP without concept_ancestor)
SELECT
    de.visit_occurrence_id AS case_id,
    'Drug Started: ' || c.concept_name AS activity,
    de.drug_exposure_start_datetime AS timestamp
FROM drug_exposure de
JOIN concept c ON de.drug_concept_id = c.concept_id
WHERE de.visit_occurrence_id IN (SELECT visit_occurrence_id FROM sepsis_visits)
  AND de.drug_exposure_start_datetime IS NOT NULL

UNION ALL

-- ─── Activity 4: Clinical Observation ─────────────────────────────────────
-- Vital signs and other observations for sepsis visits
SELECT
    o.visit_occurrence_id AS case_id,
    'Observation: ' || c.concept_name AS activity,
    o.observation_datetime AS timestamp
FROM observation o
JOIN concept c ON o.observation_concept_id = c.concept_id
WHERE o.visit_occurrence_id IN (SELECT visit_occurrence_id FROM sepsis_visits)
  AND o.observation_datetime IS NOT NULL

ORDER BY case_id, timestamp;
