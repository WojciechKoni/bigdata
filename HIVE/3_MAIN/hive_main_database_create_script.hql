set hive.enforce.bucketing=true;

DROP TABLE IF EXISTS TEMP.MAIN_LOAD_ETL_TIME_LOG;

CREATE TABLE TEMP.MAIN_LOAD_ETL_TIME_LOG
AS 
select 
 cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name 
,cast('LOAD' as STRING) as etl_step_name      
,cast('MAIN' as STRING) as database_name      
,cast('ALL' as STRING) table_name         
,CURRENT_TIMESTAMP as start_timestamp    
;



----------------------------------------------
--
--               D_VOTE_TYPE
 
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.D_VOTE_TYPE
;

CREATE TABLE IF NOT EXISTS MAIN.D_VOTE_TYPE
(
  vote_type_id             SMALLINT,
  vote_type_name           VARCHAR(100),
  load_timestamp           TIMESTAMP
) STORED AS ORC
;


----------------------------------------------
--
--               D_CLOSE_REASON
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.D_CLOSE_REASON
;

CREATE TABLE IF NOT EXISTS MAIN.D_CLOSE_REASON
(
   close_reason_id             STRING,
   close_reason_name           STRING,
   load_timestamp              TIMESTAMP
) STORED AS ORC
;


----------------------------------------------
--
--               D_COUNTRY
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.D_COUNTRY
;

CREATE TABLE IF NOT EXISTS MAIN.D_COUNTRY
(
  country_id               SMALLINT,
  country_name             VARCHAR(100),
  iso_country_code         CHAR(2),
  load_timestamp           TIMESTAMP
) STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;


----------------------------------------------
--
--               F_COMMENT
--
----------------------------------------------


DROP TABLE IF EXISTS MAIN.F_COMMENT;

CREATE TABLE IF NOT EXISTS MAIN.F_COMMENT
 (
 comment_id                 INT
,post_id                    INT
,score                      INT
,text                       STRING 
,creation_date              STRING
,creation_date_timestamp    TIMESTAMP
,user_id                    INT
,creation_date_month        STRING
,creation_date_day          STRING
,creation_date_hour         STRING
,creation_date_year         SMALLINT
,load_timestamp             TIMESTAMP
) 
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(post_id) INTO 2 BUCKETS
STORED AS ORC
  TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;



----------------------------------------------
--
--                D_TAG
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.D_TAG;

CREATE TABLE IF NOT EXISTS MAIN.D_TAG (
  tag_id             INT
, tag_name           VARCHAR(200)
, count              INT
, load_timestamp     TIMESTAMP
 ) 
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(tag_id) INTO 2 BUCKETS
 STORED AS ORC
  TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;


----------------------------------------------
--
--                D_SITE
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.D_SITE;

CREATE TABLE IF NOT EXISTS MAIN.D_SITE (
  site_id             INT       
, site_name           STRING  
, load_timestamp      TIMESTAMP   
)
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(site_id) INTO 2 BUCKETS
 STORED AS ORC

;



----------------------------------------------
--
--                D_POST_HIST_TYPE
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.D_POST_HIST_TYPE
;

CREATE TABLE IF NOT EXISTS MAIN.D_POST_HIST_TYPE
(
  post_history_type_id      INT       
, post_history_type_name    VARCHAR(100)
, post_history_type_descr   VARCHAR(100)
, load_timestamp         TIMESTAMP   
)
 STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="ZLIB"
);

----------------------------------------------
--
--                D_POST_LINK_TYPE
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.D_POST_LINK_TYPE
;

CREATE TABLE IF NOT EXISTS MAIN.D_POST_LINK_TYPE
(
  post_link_type_id     INT       
, post_link_type_name   VARCHAR(100)
, load_timestamp        TIMESTAMP   
)
 STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="ZLIB"
);


----------------------------------------------
--
--                D_VOTE
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.D_VOTE;

CREATE TABLE IF NOT EXISTS MAIN.D_VOTE (
  vote_id                 INT
, post_id                 INT
, vote_type_id            INT
, creation_date           DATE
, user_id                 INT
, creation_date_year      SMALLINT
, load_timestamp          TIMESTAMP  
 )
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(post_id) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;



----------------------------------------------
--
--                D_LOCATION
--
----------------------------------------------
--, forum_name             VARCHAR(100)



DROP TABLE IF EXISTS MAIN.D_LOCATION;

CREATE TABLE IF NOT EXISTS MAIN.D_LOCATION (
  location_id            INT       
, location_descr         VARCHAR(200)
, country_id             SMALLINT
, part_id                SMALLINT
, city_id                INT 
, load_timestamp         TIMESTAMP
 ) 
CLUSTERED BY(location_id) INTO 2 BUCKETS
 STORED AS ORC
  TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;


----------------------------------------------
--
--                D_USER
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.D_USER
;

CREATE TABLE IF NOT EXISTS MAIN.D_USER
 (
  user_id                INT
, reputation             INT
, creation_date          DATE
, first_name             STRING
, last_name              STRING
, last_access_date       DATE
, last_access_date_hour  STRING
, location_id            STRING
, about_me               STRING
, user_site_id           SMALLINT  
, image_site_id          SMALLINT  
, views                  INT
, up_votes               INT
, down_votes             INT
, account_id             INT
, activity_status        VARCHAR(30)
, load_timestamp         TIMESTAMP
 ) 
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(user_id) INTO 2 BUCKETS
 STORED AS ORC
  TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;

----------------------------------------------
--
--             F_POST_TAG_RELATION
--
----------------------------------------------


DROP TABLE IF EXISTS MAIN.F_POST_TAG_RELATION
;

CREATE TABLE IF NOT EXISTS MAIN.F_POST_TAG_RELATION
 (
   post_id                INT 
 , tag_id                 INT 
 , load_timestamp         TIMESTAMP
 ) 
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(post_id) INTO 2 BUCKETS
 STORED AS ORC
  TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;

----------------------------------------------
--
--             F_POST_ACTIVITY
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.F_POST_ACTIVITY;

CREATE TABLE IF NOT EXISTS MAIN.F_POST_ACTIVITY (
 user_id                          INT
,post_id                          INT
,post_create_timestamp            TIMESTAMP
,first_answer_create_timestamp    TIMESTAMP
,first_answer_author_id           INT
,first_comment_create_timestamp   TIMESTAMP
,first_comment_author_id          INT
,accepted_answer_create_timestamp TIMESTAMP
,accepted_answer_author_id        INT
,post_create_year                 SMALLINT
,load_timestamp                   TIMESTAMP
 ) 
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(post_id) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;
 
 
 
----------------------------------------------
--
--             F_POST
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.F_POST;

CREATE TABLE IF NOT EXISTS MAIN.F_POST (
  post_id                    INT
, post_type_id               INT
, creation_timestamp         TIMESTAMP
, parent_id                  INT
, accepted_answer_id         INT
, last_editor_display_name   VARCHAR(300) 
, last_edit_timestamp        TIMESTAMP          
, last_activity_timestamp    TIMESTAMP     
, community_owned_timestamp  TIMESTAMP    
, closed_timestamp           TIMESTAMP 
, score                      INT
, view_count                 INT
, body                       STRING
, body_word_count            SMALLINT
, cleansed_body_word_count   SMALLINT
, owner_user_id              INT
, last_activity_date         DATE
, last_activity_hour         SMALLINT
, last_editor_user_id        INT
, title                      STRING
, answer_count               INT
, comment_count              INT
, favorite_count             INT
, creation_date              DATE
, creation_month             SMALLINT
, creation_day               SMALLINT
, creation_hour              SMALLINT
, creation_date_year         SMALLINT
, load_timestamp             TIMESTAMP
 ) 
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(post_id) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;
 
 
----------------------------------------------
--
--             D_CALENDAR
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.D_CALENDAR;

CREATE TABLE IF NOT EXISTS MAIN.D_CALENDAR (
 cal_date                 DATE 
,day_in_week              SMALLINT
,day_in_month             SMALLINT
,day_in_year              SMALLINT
,week_in_month            SMALLINT
,month_num                INT
,quarter_num              SMALLINT
,year_num                 SMALLINT
,year_month               CHAR(7)
,day_name                 VARCHAR(20)
,month_name               VARCHAR(20)
,load_timestamp           TIMESTAMP
 ) 
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;
 
 
----------------------------------------------
--
--             D_CALENDAR
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.D_BADGE;

CREATE TABLE IF NOT EXISTS MAIN.D_BADGE 
(
 badge_id              INT
,user_id               INT
,badge_name            VARCHAR(100)
,badge_date            DATE
,class                 VARCHAR(10) 
,load_timestamp        TIMESTAMP
)  
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(user_id) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;
  
 
--------------------------------------------
--
--             F_POST_LINK
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.F_POST_LINK;

CREATE TABLE IF NOT EXISTS MAIN.F_POST_LINK (
     link_id                INT
 ,   creation_timestamp     TIMESTAMP
 ,   post_id                INT
 ,   related_post_id        INT
 ,   link_type_id           INT
 ,   create_date            DATE
 ,   load_timestamp         TIMESTAMP
 )
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(post_id) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;


--------------------------------------------
--
--             F_POST_HISTORY
--
----------------------------------------------

DROP TABLE IF EXISTS MAIN.F_POST_HISTORY;

CREATE TABLE IF NOT EXISTS MAIN.F_POST_HISTORY (
    row_id               INT
,   post_history_type_id INT
,   post_id              INT
,   revision_guid        STRING
,   creation_timestamp   TIMESTAMP
,   user_id              INT
,   text                 STRING
,   create_date          DATE
,   load_timestamp       TIMESTAMP
)
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(post_id) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;





INSERT INTO AUDIT.ETL_TIME_LOG
SELECT 
 cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name 
,a.etl_step_name                     as etl_step_name
,a.database_name
,a.table_name
,0
,0
,0
,a.start_timestamp
,CURRENT_TIMESTAMP as end_timestamp
,CURRENT_TIMESTAMP as load_timestamp
from  TEMP.MAIN_LOAD_ETL_TIME_LOG a
;


