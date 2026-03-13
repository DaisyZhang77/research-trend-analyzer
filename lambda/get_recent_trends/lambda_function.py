import json
import pymysql
import os

def lambda_handler(event, context):
    query_params = event.get('queryStringParameters') or {}
    sort_by = query_params.get('sort', 'emerging_score')
    
    allowed_columns = ['emerging_score', 'growth_rate', 'paper_count']
    if sort_by not in allowed_columns:
        sort_by = 'emerging_score'

    conn = pymysql.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        db="researchtrend",
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        with conn.cursor() as cursor:
            sql = f"""
            SELECT 
                topic_name as topic, 
                publication_date, 
                paper_count, 
                growth_rate, 
                emerging_score
            FROM topic_trends 
            WHERE publication_date = (SELECT MAX(publication_date) FROM topic_trends)
            ORDER BY ({sort_by} IS NOT NULL) DESC, {sort_by} DESC
            LIMIT 20
            """
            
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                row['publication_date'] = str(row['publication_date'])
                score = row['emerging_score']
                
                if score is None:
                    row['score_label'] = "NEW"
                elif score == 0.0:
                    row['score_label'] = "STABLE"
                else:
                    row['score_label'] = round(score, 4)

            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps(rows)
            }
    finally:
        conn.close()