API de mobilidade de BH: https://dados.pbh.gov.br/dataset/tempo_real_onibus_-_coordenada

Data layers:

- Bronze: raw JSON files stored locally (simulating S3).
- Silver: normalized structured data (Parquet or relational staging).
- Gold: analytical warehouse tables in PostgreSQL.

Orquestração: Apache Airflow DAG:
- extract_events
- validate_events
- normalize_events
- load_warehouse
- aggregate_rankings

Dashboard: Streamlit consumindo warehouse data.