# qversity-data-2025-Montevideo-FranciscoEscobar


## Overview

This project implements a comprehensive ELT (Extract, Load, Transform) data pipeline for mobile telecommunications customer analytics. The pipeline processes raw customer data through a Bronze-Silver-Gold architecture, providing business-ready insights for customer segmentation, revenue analysis, and operational metrics.



**Technology Stack:**
- **Orchestration**: Apache Airflow
- **Data Transformation**: dbt (data build tool)
- **Database**: PostgreSQL
- **Containerization**: Docker & Docker Compose
- **Languages**: SQL, Python

## Participant

- **Name**: Francisco Escobar
- **Email**: franciscomateoescobar1@gmail.com

## Architecture

This project implements a Bronze-Silver-Gold data lakehouse architecture:

- **Bronze Layer**: Raw data ingestion from S3 into PostgreSQL as JSONB
- **Silver Layer**: Cleaned, normalized, and standardized data with data quality tests
- **Gold Layer**: Business-ready analytics views and aggregated metrics

## Project Structure

```
/
├── dags/                 # Airflow DAG definitions
│   ├── bronze_pipeline_dag.py         # S3 to Bronze ingestion
│   ├── silver_pipeline_dag.py         # Bronze to Silver transformation
│   ├── gold_pipeline_dag.py           # Silver to Gold analytics
│   └── master_pipeline_dag.py         # Master orchestration DAG
├── dbt/                  # dbt project
│   ├── models/           # dbt models (bronze, silver, gold)
│   │   ├── bronze/       # Raw data staging
│   │   ├── silver/       # Cleaned data with normalization
│   │   │   ├── stg_mobile_customers_cleaned.sql
│   │   │   ├── stg_payment_history.sql
│   │   │   ├── stg_customer_services.sql
│   │   │   └── stg_dim_services.sql
│   │   └── gold/         # Business analytics
│   │       ├── tables/   # Core analytical tables
│   │       │   └── customer_analytics.sql
│   │       └── views/    # Business intelligence views
│   │           ├── arpu_by_plan_type.sql
│   │           ├── revenue_by_geo.sql
│   │           ├── services_popularity.sql
│   │           ├── credit_score_vs_payment_behavior.sql
│   │           └── ... (20+ analytical views)
│   ├── macros/           # Reusable SQL functions
│   ├── tests/            # dbt tests
│   ├── dbt_project.yml   # dbt configuration
│   └── profiles.yml      # Database connections
├── scripts/              # Setup and utility scripts
├── data/                 # Data files
│   ├── raw/              # Raw input data
│   └── processed/        # Processed output data
├── logs/                 # Application logs
├── env.example           # Environment variables template
├── .gitignore            # Python/SQL gitignore
├── docker compose.yml    # Docker environment setup
├── requirements.txt      # Python dependencies
└── README.md             # This file
```

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 4GB RAM available
- Git for cloning the repository

### Setup

1. **Clone the repository**:
```bash
git clone https://github.com/FranciszekaMateu/qversity-data-2025-Montevideo-FranciscoEscobar
cd qversity-data-final-project-2025
```

2. **Setup environment**:
```bash
# Copy environment template
cp env.example .env
# Edit .env file with your specific configurations if needed
```

3. **Start the services**:
```bash
# Start all services (Airflow, PostgreSQL, dbt)
docker compose up -d

# Wait for services to be ready (about 2-3 minutes)
docker compose logs -f airflow
```

4. **Verify setup**:
```bash
# Check all services are running
docker compose ps

# Access Airflow UI at http://localhost:8080
# Login: admin/admin
```

## Running the Pipeline

### Option 1: Using Master DAG (Recommended)
```bash
# Access Airflow UI at http://localhost:8080
# Navigate to DAGs page
# Find "master_pipeline_dag" DAG
# Click "Trigger DAG" button
```

### Option 2: Using Airflow CLI
```bash
# Trigger the complete pipeline
docker compose exec airflow airflow dags trigger master_pipeline_dag

# Monitor pipeline execution
docker compose exec airflow airflow dags state master_pipeline_dag

# View task logs
docker compose logs -f airflow
```

### Option 3: Manual Layer Execution
```bash
# Run Bronze layer (data ingestion)
docker compose exec airflow airflow dags trigger bronze_pipeline_dag

# Run Silver layer (data cleaning)
docker compose exec airflow airflow dags trigger silver_pipeline_dag

# Run Gold layer (analytics)
docker compose exec airflow airflow dags trigger gold_pipeline_dag
```

## Data Quality & Testing

### dbt Data Quality Tests
```bash
# Run all dbt tests
docker compose exec dbt dbt test

# Run tests for specific layer
docker compose exec dbt dbt test --models bronze
docker compose exec dbt dbt test --models silver
docker compose exec dbt dbt test --models gold

# Run specific test types
docker compose exec dbt dbt test --models silver --select test_type:unique
docker compose exec dbt dbt test --models silver --select test_type:not_null
```

### Specific Test Categories
```bash
# Test Silver layer data quality
docker compose exec dbt dbt test --models tag:silver

# Test customer data integrity
docker compose exec dbt dbt test --models stg_mobile_customers_cleaned

# Test service normalization
docker compose exec dbt dbt test --models stg_customer_services stg_dim_services

# Test payment data quality
docker compose exec dbt dbt test --models stg_payment_history
```

### Data Validation Queries
```bash
# Connect to database for manual validation
docker compose exec postgres psql -U qversity-admin -d qversity

# Check row counts across layers
SELECT 'bronze' as layer, COUNT(*) FROM public_bronze.raw_mobile_customers
UNION ALL
SELECT 'silver_customers', COUNT(*) FROM public_silver.stg_mobile_customers_cleaned
UNION ALL
SELECT 'gold_analytics', COUNT(*) FROM public_gold.customer_analytics;

# Validate service normalization
SELECT service_code, COUNT(*) as customers 
FROM public_silver.stg_customer_services cs
JOIN public_silver.stg_dim_services s ON cs.service_id = s.service_id
GROUP BY service_code;
```

## Business Insights

Comprehensive business insights and analytical findings derived from this project are detailed in the accompanying PDF report, available [here](https://github.com/FranciszekaMateu/qversity-data-2025-Montevideo-FranciscoEscobar/blob/main/business_insights_report.pdf).

## Access Points

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **PostgreSQL**: localhost:5432 (qversity-admin/qversity-admin)
- **Database**: qversity

## Common Commands

### Airflow
```bash
# View Airflow logs
docker compose logs -f airflow

# Trigger a DAG manually
docker compose exec airflow airflow dags trigger master_pipeline_dag

# List all DAGs
docker compose exec airflow airflow dags list

# Pause/Unpause DAG
docker compose exec airflow airflow dags pause master_pipeline_dag
docker compose exec airflow airflow dags unpause master_pipeline_dag
```

### dbt
```bash
# Enter dbt container
docker compose exec dbt bash

# Run all models
dbt run

# Run specific layer
dbt run --models bronze
dbt run --models silver
dbt run --models gold

# Run specific model
dbt run --models customer_analytics

# Test data quality
dbt test

# Generate and serve documentation
dbt docs generate
dbt docs serve --port 8081
```


## Testing

```bash
# Run all dbt tests
docker compose exec dbt dbt test

# Run specific test categories
docker compose exec dbt dbt test --models tag:silver
docker compose exec dbt dbt test --models tag:gold

# Test specific models
docker compose exec dbt dbt test --models customer_analytics
docker compose exec dbt dbt test --models stg_mobile_customers_cleaned

# Run data freshness tests
docker compose exec dbt dbt source freshness
