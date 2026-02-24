# Wistia Data Engineering Pipeline

## Bronze → Silver → Gold Architecture with AWS S3 & Airflow Orchestration

------------------------------------------------------------------------

## Project Overview

This project implements a production-style data engineering pipeline for
ingesting Wistia media analytics data via API, storing it in AWS S3,
transforming it through layered architecture, and orchestrating
workflows using Apache Airflow.

The pipeline follows a modern medallion architecture:

-   **Bronze Layer**: Raw JSON ingestion from Wistia API
-   **Silver Layer**: Cleaned, structured Parquet transformation
-   **Gold Layer**: Aggregated analytics-ready datasets

------------------------------------------------------------------------

## Architecture Diagram (Logical Flow)

Wistia API\
↓\
Bronze (Raw JSON) → S3\
↓\
Silver (Structured Parquet) → S3\
↓\
Gold (Aggregated Metrics) → S3\
↓\
Airflow DAG (Scheduled Orchestration)

------------------------------------------------------------------------

## Tech Stack

-   Python 3.12
-   Pandas
-   Boto3
-   PyArrow
-   AWS S3
-   Apache Airflow (LocalExecutor)
-   Parquet Format
-   Layered Data Architecture

------------------------------------------------------------------------
## Data Flow

### Bronze Layer

-   Raw JSON pulled daily from Wistia API
-   Stored in: s3://wistia-modelling/Bronze_layer/
-   Date_wise partition
-   Immutable storage
-   No data model as raw data

### Silver Layer

-   JSON normalized into tabular schema
-   Converted to Parquet format
-   Stored in: s3://wistia-modelling/Silver_layer/
-   Optimized for query performance
-   Data Model as show below 
  
dim_media (date wise partition) such as s3://wistia-modelling/silverlayer/dim_media/extraction_date=2026-02-24/
    - media_id
    - title (name)
    - URL
    - created_at

fact_media_engagement (date wise partition) such as s3://wistia-modelling/silverlayer/fact_media_stats/extraction_date=2026-02-24/fact_media_stats.csv
    - media_id
    - extraction_date
    - load_count
    - play_count
    - play_rate
    - hours_watched
    - engagement
    - visitors

  ### Gold Layer

-   Business-level aggregations
-   KPI computations
-   Analytical summaries
-   Stored in: s3://wistia-modelling/Gold_layer/
-   Views created according to dashboard requirements from bronze layer

------------------------------------------------------------------------

## Pipeline Scheduling

The pipeline is orchestrated using Apache Airflow:

DAG Tasks:

1.  Ingestion (Bronze)
2.  Silver Transformation
3.  Gold Aggregation

Schedule: Daily at 02:00 AM\
Executor: LocalExecutor\
Dependency Model: Sequential Task Execution

------------------------------------------------------------------------

## Business KPIs Generated (Gold Layer)

The Gold layer produces the following analytics metrics:

-   Total Media Count
-   Total Plays
-   Average Engagement Rate
-   Completion Rate
-   Median Watch Time
-   Top Performing Media by Engagement
-   Bottom Performing Media by Drop-Off Rate

------------------------------------------------------------------------

## Performance Metrics (Simulated Production Metrics)

Pipeline Performance Benchmarks:

-   Average Bronze Ingestion Time: 45 seconds
-   Silver Transformation Runtime: 18 seconds
-   Gold Aggregation Runtime: 7 seconds
-   Total End-to-End Pipeline Runtime: 1.2 minutes

Storage Optimization:

-   JSON → Parquet Compression Savings: 68%
-   Average Bronze File Size: 2.1 MB
-   Average Silver File Size: 670 KB
-   Gold Aggregation Dataset Size: 120 KB

Operational Metrics:

-   Daily Records Processed: \~8,500 rows
-   Media Objects Processed: 350 per run
-   Data Freshness SLA: \< 24 hours
-   Failure Rate: \< 1%
-   Automated Retry Attempts: 2 per task

------------------------------------------------------------------------

## Scalability Design

-   Layered architecture for separation of concerns
-   Parquet format for columnar compression
-   Idempotent transformations
-   Date-based incremental ingestion logic
-   Airflow DAG modularization
-   Ready for migration to:
    -   AWS MWAA
    -   EC2-based Airflow
    -   AWS Glue
    -   Databricks

------------------------------------------------------------------------

## Security & Governance

-   IAM-based S3 authentication
-   No hardcoded AWS credentials
-   Structured logging
-   Version-controlled ETL scripts
-   Separation between raw and curated data layers

------------------------------------------------------------------------


## Future Enhancements

-   Add data validation layer (Great Expectations)
-   Implement partitioning strategy
-   Add Redshift/Athena external tables
-   Implement CI/CD for DAG deployment
-   Add Slack/email alerts for failures
-   Introduce monitoring dashboards

------------------------------------------------------------------------

## Summary

This project demonstrates a production-ready modern data engineering
pipeline using:

-   Medallion architecture
-   Cloud object storage (S3)
-   Columnar optimization (Parquet)
-   Automated orchestration (Airflow)
-   Incremental ingestion logic
-   Analytical aggregation design

It reflects best practices for scalable, maintainable, and cloud-native
data systems.

------------------------------------------------------------------------

Author: Prabhu Shankar
