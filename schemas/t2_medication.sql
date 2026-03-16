-- ============================================================
-- OMOP CDM Schema for Medication Administration
-- Task: T2
-- Source: MIMIC-IV mapped to OMOP Common Data Model v5.4
-- Generated from demo CSV column headers + OMOP CDM spec
-- ============================================================
-- OMOP CDM: drug_exposure (individual medication administrations)
CREATE TABLE drug_exposure (
    drug_exposure_id             BIGINT        NOT NULL PRIMARY KEY,
    person_id                    BIGINT        NOT NULL,
    drug_concept_id              INTEGER       NOT NULL,
    drug_exposure_start_date     DATE          NOT NULL,
    drug_exposure_start_datetime TIMESTAMP,
    drug_exposure_end_date       DATE          NOT NULL,
    drug_exposure_end_datetime   TIMESTAMP,
    verbatim_end_date            DATE,
    drug_type_concept_id         INTEGER       NOT NULL,
    stop_reason                  VARCHAR(20),
    refills                      INTEGER,
    quantity                     NUMERIC,
    days_supply                  INTEGER,
    sig                          TEXT,
    route_concept_id             INTEGER,
    lot_number                   VARCHAR(50),
    provider_id                  BIGINT,
    visit_occurrence_id          BIGINT,
    visit_detail_id              BIGINT,
    drug_source_value            VARCHAR(50),
    drug_source_concept_id       INTEGER,
    route_source_value           VARCHAR(50),
    dose_unit_source_value       VARCHAR(50)
);

-- OMOP CDM: drug_era (aggregated continuous drug exposure periods)
CREATE TABLE drug_era (
    drug_era_id            BIGINT    NOT NULL PRIMARY KEY,
    person_id              BIGINT    NOT NULL,
    drug_concept_id        INTEGER   NOT NULL,
    drug_era_start_date    DATE      NOT NULL,
    drug_era_end_date      DATE      NOT NULL,
    drug_exposure_count    INTEGER,
    gap_days               INTEGER
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
