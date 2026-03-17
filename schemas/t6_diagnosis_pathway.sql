-- ============================================================
-- OMOP CDM Schema for Inpatient Diagnosis Pathway
-- Task: T6
-- Source: MIMIC-IV mapped to OMOP Common Data Model v5.4
-- Generated from demo CSV column headers + OMOP CDM spec
-- ============================================================
-- OMOP CDM: visit_occurrence (hospital visits/admissions)
CREATE TABLE visit_occurrence (
    visit_occurrence_id           BIGINT        NOT NULL PRIMARY KEY,
    person_id                     BIGINT        NOT NULL,
    visit_concept_id              INTEGER       NOT NULL,
    visit_start_date              DATE          NOT NULL,
    visit_start_datetime          TIMESTAMP,
    visit_end_date                DATE          NOT NULL,
    visit_end_datetime            TIMESTAMP,
    visit_type_concept_id         INTEGER       NOT NULL,
    provider_id                   BIGINT,
    care_site_id                  BIGINT,
    visit_source_value            VARCHAR(50),
    visit_source_concept_id       INTEGER,
    admitting_source_concept_id   INTEGER,
    admitting_source_value        VARCHAR(50),
    discharge_to_concept_id       INTEGER,
    discharge_to_source_value     VARCHAR(50),
    preceding_visit_occurrence_id BIGINT
);

-- OMOP CDM: condition_occurrence (diagnoses)
CREATE TABLE condition_occurrence (
    condition_occurrence_id       BIGINT        NOT NULL PRIMARY KEY,
    person_id                     BIGINT        NOT NULL,
    condition_concept_id          INTEGER       NOT NULL,
    condition_start_date          DATE          NOT NULL,
    condition_start_datetime      TIMESTAMP,
    condition_end_date            DATE,
    condition_end_datetime        TIMESTAMP,
    condition_type_concept_id     INTEGER       NOT NULL,
    stop_reason                   VARCHAR(20),
    provider_id                   BIGINT,
    visit_occurrence_id           BIGINT,
    visit_detail_id               BIGINT,
    condition_source_value        VARCHAR(50),
    condition_source_concept_id   INTEGER,
    condition_status_source_value VARCHAR(50),
    condition_status_concept_id   INTEGER
);

-- OMOP CDM: concept table (vocabulary lookup)
CREATE TABLE concept (
    concept_id          INTEGER       NOT NULL PRIMARY KEY,
    concept_name        VARCHAR(255)  NOT NULL,
    domain_id           VARCHAR(20)   NOT NULL,
    vocabulary_id       VARCHAR(20)   NOT NULL,
    concept_class_id    VARCHAR(20)   NOT NULL,
    standard_concept    VARCHAR(1),
    concept_code        VARCHAR(50)   NOT NULL,
    valid_start_date    DATE          NOT NULL,
    valid_end_date      DATE          NOT NULL,
    invalid_reason      VARCHAR(1)
);
