# Fintech Data Engineering
Data Engineering Solution for a fintech dataset

## Overview
This project involves working with a fintech dataset to perform exploratory data analysis (EDA), feature engineering, data pre-processing, and building pipelines for data ingestion, transformation, and analysis. The project integrates tools like Docker, PySpark, Kafka, PostgreSQL, Airflow, and Dash to achieve its objectives.

---

## Badges

<div align="center">
  <a href="https://www.python.org/doc/" target="_blank">
    <img src="https://img.shields.io/badge/Python-blue?logo=python&logoColor=white" alt="Python" height="40">
  </a>
  <a href="https://spark.apache.org/docs/latest/api/python/" target="_blank">
    <img src="https://img.shields.io/badge/PySpark-orange?logo=apache-spark&logoColor=white" alt="PySpark" height="40">
  </a>
  <a href="https://kafka.apache.org/documentation/" target="_blank">
    <img src="https://img.shields.io/badge/Kafka-yellow?logo=apache-kafka&logoColor=white" alt="Kafka" height="40">
  </a>
  <a href="https://www.postgresql.org/docs/" target="_blank">
    <img src="https://img.shields.io/badge/PostgreSQL-green?logo=postgresql&logoColor=white" alt="PostgreSQL" height="40">
  </a>
  <a href="https://docs.docker.com/" target="_blank">
    <img src="https://img.shields.io/badge/Docker-blue?logo=docker&logoColor=white" alt="Docker" height="40">
  </a>
  <a href="https://airflow.apache.org/docs/" target="_blank">
    <img src="https://img.shields.io/badge/Airflow-purple?logo=apache-airflow&logoColor=white" alt="Airflow" height="40">
  </a>
  <a href="https://dash.plotly.com/introduction" target="_blank">
    <img src="https://img.shields.io/badge/Dash-lightgrey?logo=dash&logoColor=white" alt="Dash" height="40">
  </a>
</div>


---

## Tools and Technologies

- **Programming Languages**: Python ![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
- **Data Processing Frameworks**: Pandas, PySpark ![PySpark](https://img.shields.io/badge/PySpark-3.4-orange?logo=apache-spark&logoColor=white)
- **Data Streaming**: Apache Kafka Python ![Kafka](https://img.shields.io/badge/Kafka-2.0.2-yellow?logo=apache-kafka&logoColor=white)
- **Database**: PostgreSQL ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-green?logo=postgresql&logoColor=white)
- **Containerization**: Docker Engine - Ubuntu ![Docker](https://img.shields.io/badge/Docker-7.1.0-blue?logo=docker&logoColor=white)
- **Workflow Orchestration**: Apache Airflow ![Airflow](https://img.shields.io/badge/Airflow-2.10.2-purple?logo=apache-airflow&logoColor=white)
- **Visualization**: Dash ![Dash](https://img.shields.io/badge/Dash-2.18.2-lightgrey?logo=dash&logoColor=white)

---

## How to Run
1. Clone the repository.
2. cd to the root
3. Build & Run the Docker compose:
   ```bash
   docker compose up --build
   ```
4. Configure PostgreSQL and Kafka as per the documentation.
5. Execute the ETL pipeline via Airflow.
6. Access the dashboard at `http://0.0.0.0:8050/`.

---
<!--
## Project Structure
```
fintech-data-engineering/
|-- data/
|   |-- raw/
|   |-- processed/
|-- scripts/
|   |-- eda.py
|   |-- feature_engineering.py
|   |-- kafka_consumer.py
|   |-- kafka_producer.py
|-- docker/
|   |-- Dockerfile
|-- airflow/
|   |-- dags/
|   |-- plugins/
|-- dashboard/
|   |-- app.py
|-- README.md
```
-->

