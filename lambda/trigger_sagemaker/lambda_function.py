import json
import boto3
import os
from datetime import datetime

sagemaker_client = boto3.client("sagemaker")


def lambda_handler(event, context):
    """
    Lambda function for API Gateway integration
    Triggers SageMaker Processing job for clustering
    """

    if "body" in event:
        body = json.loads(event["body"]) if isinstance(event["body"], str) else event["body"]
    else:
        body = event

    bucket = body.get("bucket", os.environ.get("BUCKET_NAME", "research-trend-analyzer"))
    input_prefix = body.get("input_prefix", os.environ.get("INPUT_PREFIX", "raw/openalex/"))
    num_clusters = body.get("num_clusters", 10)
    instance_type = body.get("instance_type", "ml.m5.xlarge")

    role_arn = os.environ["SAGEMAKER_ROLE_ARN"]
    script_s3_uri = os.environ["SCRIPT_S3_URI"]
    imagmbed_uri = '257758044811.dkr.ecr.us-east-2.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3'

    region = os.environ.get("AWS_REGION", "us-east-2")

    print(f"Starting clustering job for s3://{bucket}/{input_prefix}")
    print(f"Configuration: {num_clusters} clusters, {instance_type} instance")
    print(f"Using image: {imagmbed_uri}")

    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    job_name = f"clustering-job-{timestamp}"

    try:
        response = sagemaker_client.create_processing_job(
            ProcessingJobName=job_name,
            RoleArn=role_arn,
            ProcessingResources={
                "ClusterConfig": {
                    "InstanceCount": 1,
                    "InstanceType": instance_type,
                    "VolumeSizeInGB": 30
                }
            },
            AppSpecification={
                "ImageUri": imagmbed_uri,
                "ContainerEntrypoint": [
                    "python3",
                    "/opt/ml/processing/input/code/clustering_processor.py"
                ]
            },
            ProcessingInputs=[
                {
                    "InputName": "code",
                    "S3Input": {
                        "S3Uri": script_s3_uri,
                        "LocalPath": "/opt/ml/processing/input/code",
                        "S3DataType": "S3Prefix",
                        "S3InputMode": "File"
                    }
                },
                {
                    "InputName": "data",
                    "S3Input": {
                        "S3Uri": f"s3://{bucket}/{input_prefix}",
                        "LocalPath": "/opt/ml/processing/input/data",
                        "S3DataType": "S3Prefix",
                        "S3InputMode": "File"
                    }
                }
            ],
            ProcessingOutputConfig={
                "Outputs": [
                    {
                        "OutputName": "output",
                        "S3Output": {
                            "S3Uri": f"s3://{bucket}/clusters/{job_name}/",
                            "LocalPath": "/opt/ml/processing/output",
                            "S3UploadMode": "EndOfJob"
                        }
                    }
                ]
            },
            Environment={
                "BUCKET_NAME": bucket,
                "INPUT_PREFIX": input_prefix,
                "AWS_REGION": region,
                "NUM_CLUSTERS": str(num_clusters)
            },
            StoppingCondition={
                "MaxRuntimeInSeconds": 3600
            }
        )

        print(f"Successfully started SageMaker Processing job: {job_name}")

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
                "message": "Clustering job started successfully",
                "jobName": job_name,
                "jobArn": response["ProcessingJobArn"],
                "configuration": {
                    "bucket": bucket,
                    "inputPrefix": input_prefix,
                    "numClusters": num_clusters,
                    "instanceType": instance_type
                },
                "outputLocation": f"s3://{bucket}/clusters/{job_name}/"
            })
        }

    except Exception as e:
        error_msg = f"Error starting processing job: {str(e)}"
        print(error_msg)

        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({
                "success": False,
                "message": "Failed to start clustering job",
                "error": str(e)
            })
        }