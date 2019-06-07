DROP TABLE IF EXISTS AUDIT.AUDIT_LOG;

CREATE TABLE IF NOT EXISTS AUDIT.AUDIT_LOG
(
 database_name      VARCHAR(30)
,table_name         VARCHAR(30)
,forum_name         VARCHAR(100)
,insert_candidates  INT
,loaded_records     INT
,load_timestamp     TIMESTAMP
);



DROP TABLE IF EXISTS AUDIT.ETL_TIME_LOG;

CREATE TABLE IF NOT EXISTS AUDIT.ETL_TIME_LOG
(
 forum_name         VARCHAR(100)
,etl_step_name      VARCHAR(100)
,database_name      VARCHAR(30)
,table_name         VARCHAR(30)
,records_inserted   INT  
,records_deleted    INT 
,records_updated    INT 
,start_timestamp    TIMESTAMP
,end_timestamp      TIMESTAMP
,load_timestamp     TIMESTAMP
)
CLUSTERED BY(forum_name) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"transactional"="true"
);


