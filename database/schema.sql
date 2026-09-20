USE sys;
DROP DATABASE IF EXISTS researchtrend;
CREATE DATABASE researchtrend;

USE researchtrend;

CREATE TABLE research_clusters (
    cluster_id INT PRIMARY KEY,          
    size INT,                          
    top_topics TEXT,                  
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE topic_trends (
    id INT AUTO_INCREMENT PRIMARY KEY,
    topic_name VARCHAR(255),            
    publication_date DATE,              
    paper_count INT,                  
    growth_rate DOUBLE,              
    moving_avg_3d DOUBLE,            
    emerging_score DOUBLE,
    cluster_id INT,                      
    INDEX idx_date_score (publication_date, emerging_score DESC),
    FOREIGN KEY (cluster_id) REFERENCES research_clusters(cluster_id)
);

CREATE TABLE representative_papers (
    paper_id VARCHAR(255) PRIMARY KEY,  
    title TEXT,                        
    topic VARCHAR(255),                
    cluster_id INT,                      
    publication_date DATE,            
    abstract_summary TEXT,              
    FOREIGN KEY (cluster_id) REFERENCES research_clusters(cluster_id)
);

ALTER USER 'lambda_user' IDENTIFIED WITH mysql_native_password BY 'abc123!';

GRANT SELECT, SHOW VIEW, INSERT, UPDATE, DELETE, DROP, CREATE, ALTER ON researchtrend.*
      TO 'lambda_user';
FLUSH PRIVILEGES;