# Azure Data Platform Migration Playbook

This document serves as the **developer-level playbook** for the end-to-end migration of Azure SQL DB, Azure Synapse, and ADLS environments under the Enterprise Data Pipeline (EDP) framework.

_Last Updated: November 2025_

---

## Table of Contents
- [1. Overview](#1-overview)
- [2. Infrastructure Setup (Infra Team)](#2-infrastructure-setup-infra-team)
- [3. Database Migration (DaaS)](#3-database-migration-daas)
- [4. Validation and Cutover (ITTE)](#4-validation-and-cutover-itte)
- [5. Synapse Migration](#5-synapse-migration)
- [6. ADLS Migration](#6-adls-migration)
- [7. Risk and Mitigation](#7-risk-and-mitigation)
- [8. Lessons Learned](#8-lessons-learned)

---

## 1. Overview

The Azure Data Platform migration involves moving existing Azure SQL databases, Synapse analytics, and ADLS data to a new subscription using the **EDP (Enterprise Data Pipeline)** orchestration.  
The process is coordinated across three main teams: **Infra**, **DaaS**, and **ITTE**, with oversight from Deloitte and the AVP leadership team.

**Key Objectives:**
- Achieve cross-subscription data migration with minimal downtime.
- Standardize migration via EDP YAML configurations.
- Validate full data, schema, and metadata parity before cutover.

---

## 2. Infrastructure Setup (Infra Team)

**Goal:** Prepare the new Azure subscription and foundational infrastructure for migration.

**Steps:**
1. Provision new Azure subscription under corporate tenant.
2. Configure **Network Security Groups (NSGs)** and **firewall IP whitelisting**.
3. Create and validate **Bitbucket repositories** for EDP YAML configuration.
4. Ensure **subscription access checks** for Infra, DaaS, and ITTE teams.
5. Conduct **environment shake-down testing** to validate access, deployment, and connectivity.

**Deliverables:**
- Subscription and access checklist.
- NSG and firewall rules documentation.
- Initial EDP YAML skeleton repository in Bitbucket.

---

## 3. Database Migration (DaaS)

**Goal:** Migrate Azure SQL DB using backup and restore process under EDP.

**Steps:**
1. Collect **database inventory** — including size, object counts, schemas, views, and stored procedures.
2. Perform **point-in-time backup** of source DB from old subscription.
3. Update **EDP YAML** parameters for new DB instance and trigger the restore pipeline.
4. Validate backup and restore completion and record benchmark execution time.
5. Submit pull request and merge YAML changes post-validation.
6. Confirm DB access and schema integrity post-migration.

**Deliverables:**
- Backup and restore logs.
- EDP benchmark timing report.
- Schema and data validation confirmation.

---

## 4. Validation and Cutover (ITTE)

**Goal:** Verify data parity and perform final cutover to the new DB.

**Steps:**
1. Validate schema and data consistency using row counts and sample comparisons.
2. Execute **application testing** using new DB endpoint.
3. Confirm **stored procedures, triggers, and indexes** migrated successfully.
4. Provide **cutover validation confirmation** to leadership.
5. Post-cutover, ensure no delta changes occurred between PIT and validation.

**Deliverables:**
- Validation report with parity confirmation.
- ITTE sign-off memo for production switch.

---

## 5. Synapse Migration

**Goal:** Migrate analytical workloads to new Synapse instance following EDP standards.

**Steps:**
1. Create EDP YAML definition similar to SQL DB restore flow.
2. Test connectivity between Synapse and ADLS staging zones.
3. Validate data warehouse schemas and integration pipelines.
4. Perform pilot test and record benchmark timing.
5. Coordinate post-deploy validation with DaaS and ITTE.

**Deliverables:**
- Synapse EDP YAML configuration.
- Connectivity validation report.
- Performance benchmark summary.

---

## 6. ADLS Migration

**Goal:** Migrate ADLS storage and data using selected tool (AzCopy or ADF).

**Steps:**
1. Finalize tool decision and document configuration parameters.
2. Prepare target folder structure under new subscription.
3. Execute data copy from old ADLS to new ADLS.
4. Validate data integrity and structure alignment.
5. Record migration statistics and data comparison logs.

**Deliverables:**
- Migration tool configuration doc.
- Data validation and integrity report.
- Completion log for all ADLS paths.

---

## 7. Risk and Mitigation

| Risk ID | Description | Severity | Mitigation |
|----------|--------------|-----------|-------------|
| R1 | Cross-subscription EDP flow untested in PROD | High | Extend maintenance window; perform mock validation run |
| R2 | No lower environment for pre-check | High | Rely on controlled PROD validation sequence |
| R3 | Tool decision pending for ADLS | Medium | Finalize early between AzCopy/ADF |
| R4 | Tight cutover window may cause delta mismatch | High | Freeze source changes during EDP run |
| R5 | Limited resource overlap between ARE and DaaS | Medium | Align owners and communication cadence |

---

## 8. Lessons Learned

- Always confirm backup restore window before starting EDP flow.
- Maintain standard folder and schema structures across services.
- Keep YAML configurations modular and reusable for multiple environments.
- Validate post-migration data early to avoid extended validation cycles.
- Involve ITTE early for cutover readiness and application dependency testing.

---
