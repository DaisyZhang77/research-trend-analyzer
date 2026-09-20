Research Trend Analyzer

This project implements a cloud-based pipeline that analyzes emerging research topics using OpenAlex data.

Architecture

  Data flow:
  1. fetch_openalex (Lambda) pulls recent papers from the OpenAlex API (last 7 days),
     normalizes them (id, title, publication_date, abstract_text, topic), and uploads
     NDJSON to S3 under raw/openalex/year=.../month=.../day=.../run_*.ndjson.

  2. trigger_trend_emr (Lambda) starts an EMR Serverless job that runs trend_features.py.
     The Spark job reads raw OpenAlex JSON from S3, aggregates daily paper counts per topic,
     computes growth rate, 3-day moving average, and emerging_score, then writes Parquet
     to S3 at features/topic_trends/.

  3. trigger_sagemaker (Lambda) starts a SageMaker Processing job that runs
     clustering_processor.py. The script loads NDJSON from S3, builds TF-IDF embeddings
     and runs KMeans clustering, then writes cluster assignments and summaries to S3 at
     clusters/run.json and clusters/summary.json.

  4. s3_to_rds (Lambda) syncs S3 results into RDS MySQL: it reads clusters/summary.json,
     clusters/run.json, and the Parquet at features/topic_trends/, then truncates and
     repopulates research_clusters, representative_papers, and topic_trends.

  5. get_clusters and get_recent_trends (Lambdas) expose API endpoints that query the
     MySQL database and return cluster metadata (with optional representative papers)
     and top trending topics (sortable by emerging_score, growth_rate, or paper_count).

  Components:

  lambda/
    Contains all Lambda functions used for data ingestion and API endpoints.
    Each has its own API Gateway integration; use the HTTP method shown:
    - fetch_openalex       POST. Ingests OpenAlex works into S3.
    - trigger_trend_emr    POST. Starts EMR Serverless job for trend feature computation.
    - trigger_sagemaker    POST. Starts SageMaker Processing job for clustering.
    - s3_to_rds            POST. Syncs S3 clusters + topic_trends Parquet into RDS MySQL.
    - get_clusters         GET. List clusters or cluster detail by cluster_id.
    - get_recent_trends    GET. Top trends (query param sort=emerging_score|growth_rate|paper_count).

  emr/
    Spark job (trend_features.py) that reads raw OpenAlex JSON from S3, computes
    topic-level daily counts, growth rate, moving_avg_3d, and emerging_score,
    and writes Parquet to features/topic_trends/.

  sagemaker/
    Clustering processor script (clustering_processor.py) for SageMaker Processing:
    loads NDJSON from S3, TF-IDF + KMeans, writes clusters/run.json and clusters/summary.json.

  database/
    SQL schema (schema.sql) to create the MySQL database researchtrend and tables:
    research_clusters, topic_trends, representative_papers (and lambda_user with grants).

  client/
    CLI (research_client.py) that calls the deployed API for trends and clusters.

Database Schema (RDS MySQL)

  Database name: researchtrend

  Three tables. research_clusters is the parent; both topic_trends and
  representative_papers reference it through cluster_id. topic_trends.cluster_id
  is nullable, so a trend row can exist before — or without — being assigned to
  a cluster.

  Table: research_clusters
    cluster_id        INT           PRIMARY KEY
    size              INT           number of papers in the cluster
    top_topics        TEXT          JSON array of top topic labels
    last_updated      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP

  Table: topic_trends
    id                INT           AUTO_INCREMENT PRIMARY KEY
    topic_name        VARCHAR(255)
    publication_date  DATE
    paper_count       INT
    growth_rate       DOUBLE
    moving_avg_3d     DOUBLE        3-day moving average
    emerging_score    DOUBLE
    cluster_id        INT           FK -> research_clusters(cluster_id), nullable

    Index: idx_date_score on (publication_date, emerging_score DESC)
      Supports get_recent_trends, which selects the most recent
      publication_date and then ranks that day's rows. Only the default
      sort (emerging_score) is covered by the index; sort=growth_rate and
      sort=paper_count still sort at query time.

  Table: representative_papers
    paper_id          VARCHAR(255)  PRIMARY KEY (OpenAlex work ID)
    title             TEXT
    topic             VARCHAR(255)
    cluster_id        INT           FK -> research_clusters(cluster_id)
    publication_date  DATE
    abstract_summary  TEXT

    InnoDB creates an index on the cluster_id foreign key, which serves
    get_clusters' per-cluster paper lookup.

  Write ordering:
    Because both child tables reference research_clusters, a full refresh
    must insert research_clusters first and delete it last. s3_to_rds
    depends on this ordering.

Setup

  Dependencies

    Client (local): requests, rich
      pip install requests rich

    Lambda (bundle into each deployment package as needed):
      fetch_openalex     requests, boto3 (boto3 in Lambda runtime)
      trigger_trend_emr  boto3
      trigger_sagemaker  boto3
      s3_to_rds          boto3, pymysql, awswrangler
      get_clusters       pymysql
      get_recent_trends  pymysql

    EMR (trend_features.py): PySpark is provided by the EMR Serverless runtime.

    SageMaker (clustering_processor.py): pandas, numpy, scikit-learn, boto3
      are provided by the SageMaker scikit-learn image (e.g. 257758044811.dkr.ecr.*/sagemaker-scikit-learn).

  1. Create an S3 bucket (e.g. research-trend-analyzer) and optional prefixes:
     - raw/openalex/     (filled by fetch_openalex)
     - features/topic_trends/  (filled by EMR trend job)
     - clusters/         (filled by SageMaker job; s3_to_rds reads summary.json, run.json)
     - logs/emr/         (optional; EMR Serverless logs)

  2. Create RDS MySQL instance and run the schema:
     mysql -h <RDS_ENDPOINT> -u <ADMIN_USER> -p < database/schema.sql
     (Schema creates database researchtrend, tables, and user lambda_user.
      See "Database Schema (RDS MySQL)" above for the table definitions.)

  3. Upload assets to S3:
     - EMR: upload emr/trend_features.py to a path such as s3://<bucket>/scripts/trend_features.py
            and set TREND_SCRIPT_S3_URI to that S3 URI.
     - SageMaker: upload sagemaker/clustering_processor.py to a path such as
                  s3://<bucket>/scripts/clustering_processor.py and set SCRIPT_S3_URI to
                  the S3 URI of the script (or the parent prefix so the file is at
                  .../clustering_processor.py as expected by the Lambda container path).

  4. Create EMR Serverless application and note:
     - EMR_APPLICATION_ID
     - EMR_EXECUTION_ROLE_ARN (IAM role for the job)
     Set EMR_LOG_S3_URI to s3://<bucket>/logs/emr/ if desired.

  5. Deploy Lambda functions with the following environment variables (and ensure each
     has the correct IAM permissions for S3, EMR Serverless, SageMaker, and/or RDS as needed):

     fetch_openalex:
       OPENALEX_API_KEY, bucket_name, region_name

     trigger_trend_emr:
       EMR_APPLICATION_ID, EMR_EXECUTION_ROLE_ARN, TREND_SCRIPT_S3_URI
       Optional: EMR_LOG_S3_URI

     trigger_sagemaker:
       SAGEMAKER_ROLE_ARN, SCRIPT_S3_URI
       Optional: BUCKET_NAME, INPUT_PREFIX, AWS_REGION (request body can override bucket/input_prefix/num_clusters/instance_type)

     s3_to_rds:
       DB_HOST, DB_USER, DB_PASS
       (Uses bucket research-trend-analyzer and DB name researchtrend unless changed in code.
        Requires awswrangler and pymysql in the deployment package; VPC access to RDS if in VPC.)

     get_clusters:
       DB_HOST, DB_USER, DB_PASS, DB_NAME

     get_recent_trends:
       DB_HOST, DB_USER, DB_PASS
       (Uses DB name researchtrend in code.)

  6. Wire APIs: create an API Gateway (or one per Lambda) and connect each Lambda:
     GET:
       - get_recent_trends  (e.g. GET /results/trends?sort=...)
       - get_clusters       (e.g. GET /results/clusters?cluster_id=...)
     POST:
       - fetch_openalex
       - trigger_trend_emr
       - trigger_sagemaker
       - s3_to_rds

  7. Client: Install dependencies (see Dependencies above; client needs requests, rich). Paste your API Gateway base URL
     (no trailing slash) into client/research_client.py as BASE_URL. Sample runs:

     Trends (optional sort: emerging_score | growth_rate | paper_count; default emerging_score):
       python research_client.py trends
       python research_client.py trends paper_count
       python research_client.py trends growth_rate

     Clusters (optional cluster id for detail view):
       python research_client.py cluster
       python research_client.py cluster 1
