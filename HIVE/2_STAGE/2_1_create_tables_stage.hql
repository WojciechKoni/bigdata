
set hive.enforce.bucketing=true;
set hive.variable.substitute=true;


DROP TABLE IF EXISTS TEMP.STAGE_LOAD_ETL_TIME_LOG;

CREATE TABLE TEMP.STAGE_LOAD_ETL_TIME_LOG
AS 
select 
 cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name 
,cast('LOAD' as STRING) as etl_step_name      
,cast('STAGE' as STRING) as database_name      
,cast('ALL' as STRING) table_name         
,CURRENT_TIMESTAMP as start_timestamp    
;


CREATE TABLE IF NOT EXISTS STAGE.sites 
(
  site_id              INT       
, site_name            VARCHAR(100) 
,load_timestamp        TIMESTAMP   
)
PARTITIONED BY (forum_name VARCHAR(100))
 STORED AS ORC
  TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;

 

----------------------------------------------
--
--                posts_cleansed
--
----------------------------------------------


CREATE TABLE IF NOT EXISTS stage.posts_cleansed (
 postid                INT          
,body                  STRING       
,cleansed_body         STRING
,title                 STRING  
,year                  SMALLINT     
,load_timestamp        TIMESTAMP
)
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(postid) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;


----------------------------------------------
--
--                badges
--
----------------------------------------------

CREATE TABLE IF NOT EXISTS STAGE.badges
(
 badgeid              INT
,userid               INT
,name                 VARCHAR(200)
,timestamp_           TIMESTAMP 
,class                INT
,tagbased             STRING
,year                 SMALLINT  
,load_timestamp timestamp
)
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(userid) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;

----------------------------------------------
--
--                comments
--
----------------------------------------------

CREATE TABLE IF NOT EXISTS STAGE.comments (
 commentid       INT
,postid          INT 
,score           INT
,text            STRING  
,creationdatets  TIMESTAMP
,userid          INT
,year            SMALLINT  
,load_timestamp  TIMESTAMP
)
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(postid) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;


----------------------------------------------
--
--                posts
--
----------------------------------------------

CREATE TABLE IF NOT EXISTS stage.posts (
 postid                INT          
,posttypeid            INT          
,parentid              INT          
,acceptedanswerid      INT          
,creationdatets        TIMESTAMP    
,score                 INT          
,viewcount             INT          
,body                  STRING       
,owneruserid           INT          
,lasteditoruserid      INT          
,lasteditordisplayname STRING       
,lasteditdate          STRING       
,lastactivitydatets    TIMESTAMP    
,communityowneddate    STRING       
,closeddate            STRING       
,title                 STRING       
,tags                  STRING       
,answercount           INT          
,commentcount          INT
,favoritecount         INT 
,create_year           SMALLINT
,load_timestamp        TIMESTAMP
)
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(postid) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;

----------------------------------------------
--
--                tags
--
----------------------------------------------


CREATE TABLE IF NOT EXISTS STAGE.tags(
  tagid                 INT
, tagname               STRING
, count                 INT
, load_timestamp        TIMESTAMP
 ) 
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(tagid) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;


----------------------------------------------
--
--                users
--
----------------------------------------------

CREATE TABLE IF NOT EXISTS STAGE.users (
 userid INT
,reputation INT
,creationdate STRING
,displayname STRING
,lastaccessdate STRING
,location STRING
,aboutme STRING
,href STRING
,profileimageurl STRING
,views INT
,upvotes INT
,downvotes INT
,accountid INT
,create_year SMALLINT
,load_timestamp        TIMESTAMP
)
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(userid) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;


----------------------------------------------
--
--                votes
--
----------------------------------------------

CREATE TABLE IF NOT EXISTS STAGE.votes (
  voteid                INT
, postid                INT
, votetypeid            INT
, creationdate          STRING
, create_year           SMALLINT
, userid                INT
, load_timestamp        TIMESTAMP
)
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(postid) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;


----------------------------------------------
--
--                country
--
----------------------------------------------

CREATE TABLE IF NOT EXISTS STAGE.country 
(
  country_name       VARCHAR(100)
, iso_country_code   CHAR(2)
, country_id         INT
, load_timestamp     TIMESTAMP
)
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;


----------------------------------------------
--
--                closereason
--
----------------------------------------------

CREATE TABLE IF NOT EXISTS STAGE.closereason 
(
  close_reason_id     INT   
, close_reason_name   VARCHAR(100)
, forum_name          VARCHAR(100)
, load_timestamp      TIMESTAMP
)
STORED AS ORC
;


----------------------------------------------
--
--                votetype
--
----------------------------------------------

CREATE TABLE IF NOT EXISTS STAGE.votetype
(
  vote_type_id        INT   
, vote_type_name      VARCHAR(100)
, forum_name          VARCHAR(100)
, load_timestamp      TIMESTAMP
)
STORED AS ORC
;


----------------------------------------------
--
--                posthistory
--
----------------------------------------------

CREATE TABLE IF NOT EXISTS STAGE.posthistory
(
  rowid               INT   
, posthistorytypeid   INT   
, postid              INT   
, revisionguid        STRING
, creationdate        STRING
, userid              INT
, text                STRING
, create_year         SMALLINT
, load_timestamp      TIMESTAMP
)
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(postid) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;

----------------------------------------------
--
--                posthistorytype
--
----------------------------------------------

CREATE TABLE IF NOT EXISTS STAGE.posthistorytype
(
  post_history_type_id      INT   
, post_history_type_name    VARCHAR(100)
, load_timestamp            TIMESTAMP
)
STORED AS ORC
;


----------------------------------------------
--
--                postlinks
--
----------------------------------------------
CREATE TABLE IF NOT EXISTS STAGE.postlinks
(
  linkid         INT
, creationdate   STRING
, postid         INT
, relatedpostid  INT
, linktypeid     INT
, create_year    SMALLINT
, load_timestamp TIMESTAMP
)
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(postid) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;

----------------------------------------------
--
--                postlinktype
--
----------------------------------------------

CREATE TABLE IF NOT EXISTS STAGE.postlinktype
(
  post_link_type_id        INT
 ,post_link_type_name      VARCHAR(30)
, load_timestamp           TIMESTAMP
)
STORED AS ORC
;


----------------------------------------------
--
--                calendar
--
----------------------------------------------

CREATE TABLE IF NOT EXISTS STAGE.calendar
(
  cal_date                 DATE,
  day_in_week              SMALLINT, 
  day_in_month             SMALLINT,
  day_in_year              SMALLINT,
  week_in_month            SMALLINT,
  month_num                SMALLINT,
  quarter_num              SMALLINT,
  year_num                 SMALLINT,
  day_name                 VARCHAR(30),
  month_name               VARCHAR(30)
)
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;


----------------------------------------------
--
--                calendar
--
----------------------------------------------

CREATE TABLE IF NOT EXISTS STAGE.LOCATION (
  location_id  INT       
, location_descr STRING
, country_id SMALLINT
, part_id SMALLINT
, city_id INT         
)
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(location_id) INTO 2 BUCKETS
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;



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
from  TEMP.STAGE_LOAD_ETL_TIME_LOG a
;


