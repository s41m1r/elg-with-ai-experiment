-- ============================================================
-- OMOP CDM Schema for ICU Patient Pathway
-- Task: T1
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

-- OMOP CDM: visit_detail (sub-visits: ICU stays, ED stays, ward transfers)
CREATE TABLE visit_detail (
    visit_detail_id               BIGINT        NOT NULL PRIMARY KEY,
    person_id                     BIGINT        NOT NULL,
    visit_detail_concept_id       INTEGER       NOT NULL,
    visit_detail_start_date       DATE          NOT NULL,
    visit_detail_start_datetime   TIMESTAMP,
    visit_detail_end_date         DATE          NOT NULL,
    visit_detail_end_datetime     TIMESTAMP,
    visit_detail_type_concept_id  INTEGER       NOT NULL,
    provider_id                   BIGINT,
    care_site_id                  BIGINT,
    admitting_source_concept_id   INTEGER,
    discharge_to_concept_id       INTEGER,
    preceding_visit_detail_id     BIGINT,
    visit_detail_source_value     VARCHAR(50),
    visit_detail_source_concept_id INTEGER,
    admitting_source_value        VARCHAR(50),
    discharge_to_source_value     VARCHAR(50),
    visit_detail_parent_id        BIGINT,
    visit_occurrence_id           BIGINT        NOT NULL
);

-- OMOP CDM: procedure_occurrence (clinical procedures)
CREATE TABLE procedure_occurrence (
    procedure_occurrence_id     BIGINT        NOT NULL PRIMARY KEY,
    person_id                   BIGINT        NOT NULL,
    procedure_concept_id        INTEGER       NOT NULL,
    procedure_date              DATE          NOT NULL,
    procedure_datetime          TIMESTAMP,
    procedure_type_concept_id   INTEGER       NOT NULL,
    modifier_concept_id         INTEGER,
    quantity                    INTEGER,
    provider_id                 BIGINT,
    visit_occurrence_id         BIGINT,
    visit_detail_id             BIGINT,
    procedure_source_value      VARCHAR(50),
    procedure_source_concept_id INTEGER,
    modifier_source_value       VARCHAR(50)
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
