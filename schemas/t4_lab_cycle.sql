-- ============================================================
-- OMOP CDM Schema for Lab-Order-to-Result Cycle
-- Task: T4
-- Source: MIMIC-IV mapped to OMOP Common Data Model v5.4
-- Generated from demo CSV column headers + OMOP CDM spec
-- ============================================================
-- OMOP CDM: measurement (lab results, vitals, other measurements)
CREATE TABLE measurement (
    measurement_id               BIGINT        NOT NULL PRIMARY KEY,
    person_id                    BIGINT        NOT NULL,
    measurement_concept_id       INTEGER       NOT NULL,
    measurement_date             DATE          NOT NULL,
    measurement_datetime         TIMESTAMP,
    measurement_time             VARCHAR(10),
    measurement_type_concept_id  INTEGER       NOT NULL,
    operator_concept_id          INTEGER,
    value_as_number              NUMERIC,
    value_as_concept_id          INTEGER,
    unit_concept_id              INTEGER,
    range_low                    NUMERIC,
    range_high                   NUMERIC,
    provider_id                  BIGINT,
    visit_occurrence_id          BIGINT,
    visit_detail_id              BIGINT,
    measurement_source_value     VARCHAR(50),
    measurement_source_concept_id INTEGER,
    unit_source_value            VARCHAR(50),
    value_source_value           VARCHAR(50)
);

-- OMOP CDM: specimen (biological specimens collected)
CREATE TABLE specimen (
    specimen_id                  BIGINT        NOT NULL PRIMARY KEY,
    person_id                    BIGINT        NOT NULL,
    specimen_concept_id          INTEGER       NOT NULL,
    specimen_type_concept_id     INTEGER       NOT NULL,
    specimen_date                DATE          NOT NULL,
    specimen_datetime            TIMESTAMP,
    quantity                     NUMERIC,
    unit_concept_id              INTEGER,
    anatomic_site_concept_id     INTEGER,
    disease_status_concept_id    INTEGER,
    specimen_source_id           VARCHAR(50),
    specimen_source_value        VARCHAR(50),
    unit_source_value            VARCHAR(50),
    anatomic_site_source_value   VARCHAR(50),
    disease_status_source_value  VARCHAR(50)
);

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
