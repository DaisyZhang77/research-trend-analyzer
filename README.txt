Research Trend Analyzer

This project implements a cloud-based pipeline that analyzes emerging research topics using OpenAlex data.

Architecture:


Components:

lambda/
Contains all Lambda functions used for data ingestion and API endpoints.

emr/
Contains the Spark job (trend_features.py) used to compute topic trend features.

sagemaker/
Contains the clustering processor script for SageMaker Processing jobs.

database/
Contains the SQL schema used to create the MySQL database.

Setup:

1. Create an S3 bucket

2. Create RDS MySQL instance and run:

   database/schema.sql

3. Deploy Lambda functions:

   lambda/fetch_openalex
   lambda/trigger_trend_emr
   lambda/trigger_sagemaker
   lambda/s3_to_rds
   lambda/get_clusters
   lambda/get_recent_trends
