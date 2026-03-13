import os
import json
import boto3
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer

INPUT_KEY = "raw/openalex/"

def load_records_from_prefix(bucket, prefix, region):
    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")
    records = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".ndjson"):
                continue

            response = s3.get_object(Bucket=bucket, Key=key)
            lines = response["Body"].read().decode("utf-8").splitlines()

            for line in lines:
                if line.strip():
                    records.append(json.loads(line))

    return records


def upload_json_to_s3(bucket, key, data, region):
    s3 = boto3.client("s3", region_name=region)
    body = json.dumps(data, ensure_ascii=False)
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


def main():
    OUTPUT_KEY = "clusters/run.json"
    SUMMARY_KEY = "clusters/summary.json"

    bucket = os.environ.get('BUCKET_NAME', 'research-trend-analyzer')
    region = os.environ.get('AWS_REGION', 'us-east-2')
    input_prefix = os.environ.get('INPUT_PREFIX', 'raw/openalex/')
    num_clusters = int(os.environ.get('NUM_CLUSTERS', '10'))

    print(f"Loading records from s3://{bucket}/{input_prefix}")
    records = load_records_from_prefix(bucket, input_prefix, region)

    if not records:
        print("No records found!")
        return

    df = pd.DataFrame(records)
    df = df[df["abstract_text"].notna()]
    df = df[df["abstract_text"].str.strip() != ""]

    print(f"Processing {len(df)} documents...")
    texts = df["abstract_text"].tolist()

    print("Creating text embeddings using TF-IDF...")
    vectorizer = TfidfVectorizer(max_features=384, stop_words='english', 
                                  max_df=0.8, min_df=2)
    embeddings = vectorizer.fit_transform(texts).toarray()

    print(f"Clustering documents into {num_clusters} clusters...")
    kmeans = KMeans(n_clusters=num_clusters, random_state=42)
    cluster_ids = kmeans.fit_predict(embeddings)

    df["cluster_id"] = cluster_ids

    if "abstract_text" in df.columns:
        df["abstract_summary"] = df["abstract_text"]

    cluster_summary = []
    for cid, group in df.groupby("cluster_id"):
        size = len(group)
        top_topics = (
            group["topic"]
            .value_counts()
            .head(3)
            .index
            .tolist()
        )
        cluster_summary.append({
            "cluster_id": int(cid),
            "size": int(size),
            "top_topics": top_topics
        })

    output_columns = ["id", "title", "topic", "abstract_summary", "publication_date", "cluster_id"]

    available_columns = [col for col in output_columns if col in df.columns]

    output_records = df[available_columns].to_dict(orient="records")

    print(f"Uploading results to s3://{bucket}/{OUTPUT_KEY}")
    upload_json_to_s3(bucket, OUTPUT_KEY, output_records, region)

    print(f"Uploading summary to s3://{bucket}/{SUMMARY_KEY}")
    upload_json_to_s3(bucket, SUMMARY_KEY, cluster_summary, region)

    print(f"Done! Results uploaded to s3://{bucket}/{OUTPUT_KEY}")
    print(f"Summary uploaded to s3://{bucket}/{SUMMARY_KEY}")
    print(f"Total clusters: {num_clusters}")
    print(f"Total documents processed: {len(df)}")
    print(f"Output fields: {', '.join(available_columns)}")


if __name__ == "__main__":
    main()
