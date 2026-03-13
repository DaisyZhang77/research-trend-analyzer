import requests
import boto3
from typing import Dict, Any, List, Optional
import json
import os
from datetime import datetime, timedelta, timezone


OPENALEX_URL = "https://api.openalex.org/works"


def fetch_work(
    api_key: str,
    cursor: str,
    from_date: str,
    to_date: str,
    per_page: int
) -> requests.Response:
    params = {
        "api_key": api_key,
        "filter": f"from_publication_date:{from_date},to_publication_date:{to_date},type:article|preprint",
        "per-page": per_page,
        "cursor": cursor,
        "sort": "publication_date:desc",
    }

    response = requests.get(OPENALEX_URL, params=params, timeout=30)
    response.raise_for_status()
    return response


def inverted_index_to_text(inverted_index: Optional[Dict[str, List[int]]]) -> str:
    if not inverted_index:
        return ""

    pos_to_word = {}

    for word, positions in inverted_index.items():
        for pos in positions:
            pos_to_word[pos] = word

    words = [pos_to_word[i] for i in sorted(pos_to_word.keys())]
    return " ".join(words)


def normalize_work(work: Dict[str, Any]) -> Dict[str, Any]:
    topic = None
    if work.get("primary_topic"):
        topic = work["primary_topic"].get("display_name")

    abstract_index = work.get("abstract_inverted_index")
    abstract_text = inverted_index_to_text(abstract_index)

    return {
        "id": work.get("id"),
        "title": work.get("title"),
        "publication_date": work.get("publication_date"),
        "publication_year": work.get("publication_year"),
        "abstract_inverted_index": abstract_index,
        "abstract_text": abstract_text,
        "topic": topic
    }


def upload_ndjson_to_s3(records: List[Dict[str, Any]], bucket: str, key: str, region: str) -> None:
    s3 = boto3.client("s3", region_name=region)

    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson"
    )


def main():
    api_key = os.environ["OPENALEX_API_KEY"]
    bucket = os.environ["bucket_name"]
    region = os.environ["region_name"]

    today = datetime.now(timezone.utc).date()

    from_date = (today - timedelta(days=7)).isoformat()
    to_date = today.isoformat()

    print("Fetching papers from:", from_date, "to", to_date)

    cursor = "*"
    per_page = 150
    max_page = 5
    records = []

    for _ in range(max_page):
        response = fetch_work(api_key, cursor, from_date, to_date, per_page)
        body = response.json()
        results = body.get("results", [])

        if not results:
            break

        for work in results:
            records.append(normalize_work(work))

        cursor = body["meta"].get("next_cursor")
        if not cursor:
            break

    print("Fetched papers:", len(records))

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")

    s3_key = f"raw/openalex/year={year}/month={month}/day={day}/run_{run_id}.ndjson"

    upload_ndjson_to_s3(records, bucket, s3_key, region)

    print("Uploaded to S3:", s3_key)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "OpenAlex fetch complete",
            "record_count": len(records),
            "s3_key": s3_key
        })
    }


def lambda_handler(event, context):
    return main()