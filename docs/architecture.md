# Mobility Data Platform – Architecture

## 1. Overview

This document defines the architectural vision of the project.
All structural decisions must align with it.

This project simulates an AWS-style event-driven data platform running fully locally with Docker.

Goal:
Build a structured data platform from raw bus mobility events to analytical rankings.

---

## 2. High-Level Flow

Mobility API (JSON)
        ↓
Ingestion Layer (Python)
        ↓
Bronze Layer (Raw Events – append only)
        ↓
Silver Layer (Validated & Normalized Data)
        ↓
Gold Layer (Warehouse – Star Schema)
        ↓
Analytical Views (Top 15 slowest lines 04h–20h)
        ↓
Streamlit Dashboard

---

## 3. Layers

### Bronze
- Raw JSON events
- Immutable
- Append-only
- Stores ingestion timestamp

Simulates: S3 raw zone

---

### Silver
- Cleaned & typed data
- Deduplicated events
- Validated coordinates and timestamps

Simulates: Structured data lake layer

---

### Gold
- Dimensional model
- Fact table: bus_positions
- Dimension tables: dim_line, dim_time
- Indexed for analytical queries

Simulates: Redshift

---

## 4. Orchestration

Apache Airflow DAG:
- extract_events
- validate_events
- normalize_events
- load_warehouse
- aggregate_rankings

---

## 5. Design Decisions

- Append-only ingestion
- Idempotent loads
- Separation of concerns
- Cloud-ready schema
- No distributed processing

---

## 6. Future AWS Migration

Local → AWS mapping:

Raw folder → S3
PostgreSQL → Redshift
Local Airflow → MWAA
Streamlit → AWS Lambda