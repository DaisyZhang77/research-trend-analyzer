import json
import os

import awswrangler as wr
import boto3
import pymysql

DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = "researchtrend"

BUCKET_NAME = "research-trend-analyzer"

# Rows per executemany round trip. The full refresh is small (a few thousand
# rows), so one batch size works for every table.
BATCH_SIZE = 500

s3_client = boto3.client("s3")


CLUSTER_SQL = """
    INSERT INTO research_clusters (cluster_id, size, top_topics)
    VALUES (%s, %s, %s)
"""

PAPER_SQL = """
    INSERT INTO representative_papers
        (paper_id, title, topic, cluster_id, publication_date, abstract_summary)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

TREND_SQL = """
    INSERT INTO topic_trends
        (topic_name, publication_date, paper_count, growth_rate,
         moving_avg_3d, emerging_score)
    VALUES (%s, %s, %s, %s, %s, %s)
"""


def _load_json_from_s3(key):
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def _dedupe_papers_by_title(papers):
    """Keep the first paper for each normalized title."""
    seen_titles = set()
    unique_papers = []

    for paper in papers:
        normalized_title = (paper.get("title") or "").strip().lower()
        if not normalized_title or normalized_title in seen_titles:
            continue
        seen_titles.add(normalized_title)
        unique_papers.append(paper)

    return unique_papers


def _build_cluster_rows(clusters_summary):
    return [
        (
            cluster["cluster_id"],
            cluster["size"],
            json.dumps(cluster.get("top_topics", [])),
        )
        for cluster in clusters_summary
    ]


def _build_paper_rows(papers_detail):
    return [
        (
            paper["id"],
            paper["title"],
            paper["topic"],
            paper["cluster_id"],
            paper.get("publication_date"),
            paper.get("abstract_summary"),
        )
        for paper in _dedupe_papers_by_title(papers_detail)
    ]


def _build_trend_rows(trends_df):
    rows = []

    for record in trends_df.to_dict("records"):
        paper_count = record["paper_count"]
        rows.append(
            (
                record["topic"],
                record["publication_date"],
                int(paper_count) if paper_count is not None else 0,
                record["growth_rate"],
                record["moving_avg_3d"],
                record["emerging_score"],
            )
        )

    return rows


def _insert_batched(cursor, sql, rows):
    """Insert rows with executemany instead of one round trip per row."""
    for start in range(0, len(rows), BATCH_SIZE):
        cursor.executemany(sql, rows[start : start + BATCH_SIZE])
    return len(rows)


def lambda_handler(event, context):
    # Read every source before opening the transaction. S3 and Parquet reads
    # are the slow, failure-prone part; doing them first keeps the write
    # transaction short and avoids holding row locks across network calls.
    try:
        clusters_summary = _load_json_from_s3("clusters/summary.json")
        papers_detail = _load_json_from_s3("clusters/run.json")
        trends_df = wr.s3.read_parquet(
            path=f"s3://{BUCKET_NAME}/features/topic_trends/"
        )
        trends_df = trends_df.replace({float("nan"): None})
    except Exception as exc:
        print(f"Failed to load source data from S3: {exc}")
        return {"statusCode": 502, "body": f"Source load failed: {exc}"}

    cluster_rows = _build_cluster_rows(clusters_summary)
    paper_rows = _build_paper_rows(papers_detail)
    trend_rows = _build_trend_rows(trends_df)

    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
        autocommit=False,
    )

    try:
        with conn.cursor() as cursor:
            # DELETE, not TRUNCATE.
            #
            # TRUNCATE is DDL: it commits implicitly and cannot be rolled
            # back, so a failure part way through the reload used to leave
            # the serving tables empty with no way back, and every sync had
            # a window where the read APIs returned partial or no rows.
            #
            # DELETE is DML, so the whole refresh is one transaction:
            # readers keep seeing the previous snapshot until it commits,
            # and any failure rolls the tables back to that snapshot.
            # Children first so foreign keys stay satisfied throughout.
            cursor.execute("DELETE FROM representative_papers;")
            cursor.execute("DELETE FROM topic_trends;")
            cursor.execute("DELETE FROM research_clusters;")

            # Parents before children, for the same reason.
            inserted_clusters = _insert_batched(cursor, CLUSTER_SQL, cluster_rows)
            inserted_papers = _insert_batched(cursor, PAPER_SQL, paper_rows)
            inserted_trends = _insert_batched(cursor, TREND_SQL, trend_rows)

        conn.commit()

        print(
            "Sync committed: "
            f"{inserted_clusters} clusters, "
            f"{inserted_papers} representative papers, "
            f"{inserted_trends} topic trend rows"
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Data sync completed successfully",
                    "research_clusters": inserted_clusters,
                    "representative_papers": inserted_papers,
                    "topic_trends": inserted_trends,
                }
            ),
        }

    except Exception as exc:
        conn.rollback()
        print(f"Sync failed and was rolled back: {exc}")
        return {"statusCode": 500, "body": f"Sync failed, rolled back: {exc}"}

    finally:
        conn.close()
