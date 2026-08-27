# Serverless Event-Driven ETL Pipeline on AWS

## Project Overview
This project is an automated, event-driven Data Engineering pipeline deployed on AWS. It extracts raw sales data from an S3 landing zone, validates and transforms the records using Python and Pandas within a serverless Lambda environment, and loads the clean data into a PostgreSQL data warehouse hosted on Amazon RDS.

The primary objective of this project was to move beyond basic data movement and implement enterprise-level data engineering patterns—such as dead-letter routing, data quality enforcement, and pipeline idempotency—within a cloud-native architecture.

## Architecture & Tech Stack
* **Landing Zone / Trigger:** Amazon S3
* **Processing Engine:** AWS Lambda (Python 3.11, Pandas, pg8000)
* **Data Warehouse:** Amazon RDS (PostgreSQL) - Modeled using a Star Schema
* **Security & Networking:** AWS IAM (least-privilege execution roles) and VPC Security Groups

## Core Engineering Patterns

### 1. Data Quarantine (Dead-Letter Routing)
Real-world data pipelines must account for malformed data without halting execution. During the transformation phase, data quality rules are applied. Invalid rows (e.g., negative sales amounts, corrupted timestamps, missing foreign keys) are programmatically intercepted, serialized into JSON format, and routed to a dedicated `quarantine_sales` table. This allows the valid data to continue flowing into the warehouse while preserving the bad data for auditing and debugging.

### 2. Idempotent Processing
To account for potential Lambda retries or manual pipeline re-runs, the data loading logic is idempotent. The pipeline groups incoming data by `batch_id` (derived from the S3 object key) and clears any existing records for that batch in the target tables before insertion, guaranteeing that duplicate records are never created.

### 3. Upsert Logic (`ON CONFLICT`)
Dimension tables (Customers, Products) are updated using SQL upsert commands. This ensures that the data warehouse smoothly handles changing dimensions and updates existing records with the latest information without throwing primary key constraint violations.

## Repository Structure
* `lambda_function.py`: The core ETL script deployed to AWS Lambda. Handles S3 extraction, Pandas dataframe transformations, and RDS loading.
* `database_schema.sql`: The DDL statements required to initialize the Fact, Dimension, and Quarantine tables in PostgreSQL.
* `sales_test.csv`: A sample dataset containing both valid records and intentionally corrupted rows used to test and demonstrate the quarantine routing logic.

## Deployment Instructions
1. Provision a PostgreSQL instance via Amazon RDS and execute `database_schema.sql` to build the required schema.
2. Create an AWS S3 bucket to act as the raw data landing zone.
3. Deploy an AWS Lambda function (Python 3.11) with the **AWS SDK Pandas** layer attached. Upload `lambda_function.py` and configure the necessary environment variables for database connectivity (`DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASS`).
4. Configure an S3 Event Notification to trigger the Lambda function upon the creation of `.csv` objects.
5. Upload `sales_test.csv` to the S3 bucket to trigger the pipeline and verify the output in the database.
