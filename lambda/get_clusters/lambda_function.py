import json
import pymysql
import os

def lambda_handler(event, context):
    query_params = event.get('queryStringParameters') or {}
    cluster_id = query_params.get('cluster_id')
    
    conn = pymysql.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        db=os.environ['DB_NAME'],
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        with conn.cursor() as cursor:
            if cluster_id:
                sql = """
                SELECT cluster_id, size, top_topics, last_updated 
                FROM research_clusters 
                WHERE cluster_id = %s
                """
                cursor.execute(sql, (cluster_id,))
            else:
                sql = """
                SELECT cluster_id, size, top_topics, last_updated 
                FROM research_clusters 
                ORDER BY size DESC 
                LIMIT 20
                """
                cursor.execute(sql)
            
            clusters = cursor.fetchall()

            for c in clusters:
                c['last_updated'] = str(c['last_updated'])
                
                paper_sql = """
                SELECT paper_id, title, topic, publication_date, abstract_summary
                FROM representative_papers 
                WHERE cluster_id = %s 
                ORDER BY publication_date DESC LIMIT 1
                """
                cursor.execute(paper_sql, (c['cluster_id'],))
                paper = cursor.fetchone()
                
                if paper:
                    paper['publication_date'] = str(paper['publication_date'])
                    c['representative_paper'] = paper
                else:
                    c['representative_paper'] = None

            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps(clusters)
            }
    finally:
        conn.close()