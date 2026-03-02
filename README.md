# 🚀 Data Engineering Playground

This repository contains 10 automated data pipelines managed via **dbt** and orchestrated with **Airflow**, connected to an **n8n AI Agent**.

## 📂 Repository Structure
* **/dbt/models/staging/**: Initial data cleaning and type casting.
* **/dbt/models/marts/**: Final business-ready tables (e.g., `cust_ltv.sql`).
* **/dags/**: Airflow DAGs for pipeline orchestration.

## 🤖 AI Agent Integration
The n8n AI Agent uses the GitHub API to:
1.  Analyze SQL logic within the dbt models.
2.  Explain data lineage from source to gold layer.
3.  Provide direct GitHub permalinks for code reviews.

## 🛠️ Tech Stack
* **Transformation**: dbt Core
* **Orchestration**: Apache Airflow
* **Automation**: n8n (Local LLM via Ollama)