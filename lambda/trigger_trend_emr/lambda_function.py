import json
import os
import boto3
from datetime import datetime

emr_serverless = boto3.client("emr-serverless")

def lambda_handler(event, context):
    """
    Trigger EMR Serverless job for trend_features.py.
    """

    application_id = os.environ["EMR_APPLICATION_ID"]
    execution_role_arn = os.environ["EMR_EXECUTION_ROLE_ARN"]
    entry_point = os.environ["TREND_SCRIPT_S3_URI"]
    log_uri = os.environ.get("EMR_LOG_S3_URI", "s3://research-trend-analyzer/logs/emr/")

    job_name_prefix = "trend-features"
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    job_name = f"{job_name_prefix}-{timestamp}"

    try:
        response = emr_serverless.start_job_run(
            applicationId=application_id,
            executionRoleArn=execution_role_arn,
            name=job_name,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": entry_point,
                    "sparkSubmitParameters": (
                        "--conf spark.executor.memory=2g "
                        "--conf spark.driver.memory=2g "
                        "--conf spark.executor.cores=1 "
                        "--conf spark.driver.cores=1"
                    )
                }
            },
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": log_uri
                    }
                }
            }
        )

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "POST, OPTIONS"
            },
            "body": json.dumps({
                "success": True,
                "message": "EMR trend job started successfully",
                "applicationId": application_id,
                "jobRunId": response["jobRunId"],
                "jobName": job_name,
                "entryPoint": entry_point
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({
                "success": False,
                "message": "Failed to start EMR trend job",
                "error": str(e)
            })
        }