import json
import boto3
import pymysql
import awswrangler as wr
import os


DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASS = os.environ.get('DB_PASS')
DB_NAME = "researchtrend"

s3_client = boto3.client('s3')
BUCKET_NAME = "research-trend-analyzer"

def lambda_handler(event, context):
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, db=DB_NAME)
    
    try:
        with conn.cursor() as cursor:
            summary_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key="clusters/summary.json")
            clusters_summary = json.loads(summary_obj['Body'].read().decode('utf-8'))
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            cursor.execute("TRUNCATE TABLE topic_trends;")
            cursor.execute("TRUNCATE TABLE research_clusters;")
            cursor.execute("TRUNCATE TABLE representative_papers;")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            for cluster in clusters_summary:
                sql = """INSERT INTO research_clusters (cluster_id, size, top_topics) 
                         VALUES (%s, %s, %s) 
                         ON DUPLICATE KEY UPDATE size=%s, top_topics=%s"""
                topics_str = json.dumps(cluster.get('top_topics', []))
                cursor.execute(sql, (cluster['cluster_id'], cluster['size'], topics_str, 
                                     cluster['size'], topics_str))

            run_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key="clusters/run.json")
            papers_detail = json.loads(run_obj['Body'].read().decode('utf-8'))
            
            seen_titles = set()
            unique_papers = []

            for paper in papers_detail:
                normalized_title = paper['title'].strip().lower()
                
                if normalized_title not in seen_titles:
                    unique_papers.append(paper)
                    seen_titles.add(normalized_title)
            
            for paper in unique_papers:
                sql = """INSERT IGNORE INTO representative_papers 
                         (paper_id, title, topic, cluster_id, publication_date, abstract_summary) 
                         VALUES (%s, %s, %s, %s, %s, %s)"""
                cursor.execute(sql, (
                    paper['id'], 
                    paper['title'], 
                    paper['topic'], 
                    paper['cluster_id'],
                    paper.get('publication_date'), 
                    paper.get('abstract_summary')  
                ))

            parquet_path = f"s3://{BUCKET_NAME}/features/topic_trends/"
            df = wr.s3.read_parquet(path=parquet_path)
            
            df = df.replace({float('nan'): None})

            for _, row in df.iterrows():
                sql = """INSERT INTO topic_trends 
                         (topic_name, publication_date, paper_count, growth_rate, moving_avg_3d, emerging_score)
                         VALUES (%s, %s, %s, %s, %s, %s)"""
                
                cursor.execute(sql, (
                    row['topic'], 
                    row['publication_date'], 
                    int(row['paper_count']) if row['paper_count'] is not None else 0,
                    row['growth_rate'], 
                    row['moving_avg_3d'], 
                    row['emerging_score']
                ))

        conn.commit()
        return {"statusCode": 200, "body": "Data Sync Completed Successfully"}
        
    except Exception as e:
        print(f"Error occurred: {e}")
        return {"statusCode": 500, "body": str(e)}
    finally:
        conn.close()