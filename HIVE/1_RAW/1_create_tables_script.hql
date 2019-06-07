-- external tables to create 

-- RAW.BADGES
-- RAW.COMMENTS
-- RAW.POSTS
-- RAW.NPOSTS
-- RAW.POSTHISTORYTYPE
-- RAW.POSTLINKTYPE
-- RAW.CLOSEREASON
-- RAW.VOTETYPE
-- RAW.COUNTRY
-- RAW.POSTHISTORY
-- RAW.POSTLINKS 
-- RAW.VOTES
-- RAW.USERS
-- RAW.TAGS
-- RAW.CALENDAR


--DROP DATABASE IF EXISTS RAW   CASCADE;
--CREATE DATABASE RAW   ;

add jar hdfs:///user/hive/unnamed.jar;

drop function if exists remove_stopwords;
create function remove_stopwords  as 'BigDataUDFs.RemoveStopWordsUDF';

drop function if exists count_words_without_stopwords; 
create function count_words_without_stopwords  as 'BigDataUDFs.CountWordsExceptStopWordsUDF';

set hive.enforce.bucketing=true;
set hive.variable.substitute=true;

DROP TABLE IF EXISTS TEMP.RAW_LOAD_ETL_TIME_LOG;

CREATE TABLE TEMP.RAW_LOAD_ETL_TIME_LOG
AS 
select 
 cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name 
,cast('LOAD' as STRING) as etl_step_name      
,cast('RAW' as STRING) as database_name      
,cast('ALL' as STRING) table_name         
,CURRENT_TIMESTAMP as start_timestamp    
;




----------------------------------------------
--
--                   CALENDAR
--
----------------------------------------------

DROP TABLE IF EXISTS RAW.CALENDAR;

CREATE EXTERNAL TABLE RAW.CALENDAR
(
  cal_date                 STRING,
  day_in_week              STRING, 
  day_in_month             STRING,
  day_in_year              STRING,
  week_in_month            STRING,
  month_num                STRING,
  quarter_num              STRING,
  year_num                 STRING,
  day_name                 STRING,
  month_name               STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\u003B" STORED AS TEXTFILE
LOCATION "/user/hive/txt/calendar";

----------------------------------------------
--
--                   BADGES
--
----------------------------------------------

DROP TABLE IF EXISTS RAW.BADGES;

CREATE EXTERNAL TABLE IF NOT EXISTS RAW.BADGES
(
badgeid STRING ,
userid STRING , 
name STRING, 
timestamp_ STRING, 
class STRING,  
tagbased STRING
)
COMMENT 'Badges.xml'
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.badgeid"="/row/@Id",
"column.xpath.userid"="/row/@UserId",
"column.xpath.name"="/row/@Name",
"column.xpath.timestamp_"="/row/@Date",
"column.xpath.class"="/row/@Class",
"column.xpath.tagbased"="/row/@TagBased"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/user/hive/xml/${hiveconf:FORUM_NAME}/Badges' 
TBLPROPERTIES (
"xmlinput.start"="<row "
,"xmlinput.end"="/>"
);

----------------------------------------------
--
--                     COMMENTS
--
----------------------------------------------

drop table RAW.COMMENTS;

CREATE EXTERNAL TABLE RAW.COMMENTS(commentid STRING
, postid STRING
, score STRING
, text STRING
, creationdatets STRING
, userid STRING)
COMMENT 'Comments.xml'
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.commentid"="/row/@Id",
"column.xpath.postid"="/row/@PostId",
"column.xpath.score"="/row/@Score",
"column.xpath.text"="/row/@Text",
"column.xpath.creationdatets"="/row/@CreationDate",
"column.xpath.userid"="/row/@UserId"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/user/hive/xml/${hiveconf:FORUM_NAME}/' 
TBLPROPERTIES (
"xmlinput.start"="<row "
,"xmlinput.end"="/>"
);

----------------------------------------------
--
--                    COMMENTS
--
----------------------------------------------

drop table RAW.COMMENTS;

CREATE EXTERNAL TABLE RAW.COMMENTS(
  commentid STRING
, postid STRING
, score STRING
, text STRING
, creationdatets STRING
, userid STRING)
COMMENT 'Comments.xml'
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.commentid"="/row/@Id",
"column.xpath.postid"="/row/@PostId",
"column.xpath.score"="/row/@Score",
"column.xpath.text"="/row/@Text",
"column.xpath.creationdatets"="/row/@CreationDate",
"column.xpath.userid"="/row/@UserId"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/user/hive/xml/${hiveconf:FORUM_NAME}/Comments' 
TBLPROPERTIES (
"xmlinput.start"="<row "
,"xmlinput.end"="/>"
);

----------------------------------------------
--
--                    POSTS
--
----------------------------------------------

DROP TABLE IF EXISTS RAW.POSTS;

CREATE EXTERNAL TABLE IF NOT EXISTS RAW.POSTS(
  postid STRING
, posttypeid STRING
, parentid STRING
, acceptedanswerid STRING
, creationdatets STRING
, score STRING
, viewcount STRING
, body STRING
, owneruserid STRING
, lasteditoruserid STRING
, lasteditordisplayname  STRING
, lasteditdate  STRING
, lastactivitydatets STRING
, communityowneddate STRING
, closeddate STRING
, title STRING
, tags STRING
, answercount STRING
, commentcount STRING
, favoritecount STRING
 ) 
 COMMENT 'Posts.xml'
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.postid"="/row/@Id",
"column.xpath.posttypeid"="/row/@PostTypeId",
"column.xpath.parentid"="/row/@ParentId",
"column.xpath.acceptedanswerid"="/row/@AcceptedAnswerId",
"column.xpath.creationdatets"="/row/@CreationDate",
"column.xpath.score"="/row/@Score",
"column.xpath.viewcount"="/row/@ViewCount",
"column.xpath.body"="/row/@Body",
"column.xpath.owneruserid"="/row/@OwnerUserId",
"column.xpath.lasteditoruserid"="/row/@LastEditorUserId",
"column.xpath.lasteditordisplayname"="/row/@LastEditorDisplayName",
"column.xpath.lasteditdate"="/row/@LastEditDate",
"column.xpath.lastactivitydatets"="/row/@LastActivityDate",
"column.xpath.communityowneddate"="/row/@CommunityOwnedDate",
"column.xpath.closeddate"="/row/@ClosedDate",
"column.xpath.title"="/row/@Title",
"column.xpath.tags"="/row/@Tags",
"column.xpath.answercount"="/row/@AnswerCount",
"column.xpath.commentcount"="/row/@CommentCount",
"column.xpath.favoritecount"="/row/@FavoriteCount"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/user/hive/xml/${hiveconf:FORUM_NAME}/Posts' 
TBLPROPERTIES (
"xmlinput.start"="<row "
,"xmlinput.end"="/>"
);


-- znarmalizowan
DROP TABLE IF EXISTS RAW.NPOSTS;

CREATE EXTERNAL TABLE IF NOT EXISTS RAW.NPOSTS(
  postid STRING
, posttypeid STRING
, parentid STRING
, acceptedanswerid STRING
, creationdatets STRING
, score STRING
, viewcount STRING
, body STRING
, body_without_stopwords STRING
, owneruserid STRING
, lasteditoruserid STRING
, lasteditordisplayname  STRING
, lasteditdate  STRING
, lastactivitydatets STRING
, communityowneddate STRING
, closeddate STRING
, title STRING
, tags STRING
, answercount STRING
, commentcount STRING
, favoritecount STRING
 ) 
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="ZLIB",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;

INSERT OVERWRITE TABLE RAW.NPOSTS
select  
 postid               
,posttypeid           
,parentid             
,acceptedanswerid     
,creationdatets       
,score                
,viewcount            
,body                
,upper(remove_stopwords(regexp_replace(body, '<.*?>' , "") ))                
,owneruserid          
,lasteditoruserid     
,lasteditordisplayname
,lasteditdate         
,lastactivitydatets   
,communityowneddate   
,closeddate           
,upper(title)                
,tags                 
,answercount          
,commentcount         
,favoritecount 

from raw.posts;       
 
----------------------------------------------
--
--                   TAGS
--
----------------------------------------------


DROP TABLE IF EXISTS RAW.TAGS;

CREATE EXTERNAL TABLE IF NOT EXISTS RAW.TAGS (
  tagid STRING
, tagname STRING
, count STRING
 ) 
 COMMENT 'Tags.xml'
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.tagid"="/row/@Id",
"column.xpath.tagname"="/row/@TagName",
"column.xpath.count"="/row/@Count"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/user/hive/xml/${hiveconf:FORUM_NAME}/Tags' 
TBLPROPERTIES (
"xmlinput.start"="<row "
,"xmlinput.end"="/>"
);

----------------------------------------------
--
--                   USERS
--
----------------------------------------------

DROP TABLE IF EXISTS RAW.USERS;

CREATE EXTERNAL TABLE IF NOT EXISTS RAW.USERS (
  userid STRING
, reputation STRING
, creationdate STRING
, displayname STRING
, lastaccessdate STRING
, location STRING
, aboutme STRING
, href STRING
, profileimageurl STRING
, views STRING
, upvotes STRING
, downvotes STRING
, accountid STRING
) 
 COMMENT 'Users.xml'
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.userid"="/row/@Id",
"column.xpath.reputation"="/row/@Reputation",
"column.xpath.creationdate"="/row/@CreationDate",
"column.xpath.displayname"="/row/@DisplayName",
"column.xpath.lastaccessdate"="/row/@LastAccessDate",
"column.xpath.location"="/row/@Location",
"column.xpath.aboutme"="/row/@AboutMe",
"column.xpath.href"="/row/@WebsiteUrl",
"column.xpath.profileimageurl"="/row/@ProfileImageUrl",
"column.xpath.views"="/row/@Views",
"column.xpath.upvotes"="/row/@UpVotes",
"column.xpath.downvotes"="/row/@DownVotes",
"column.xpath.accountid"="/row/@AccountId"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/user/hive/xml/${hiveconf:FORUM_NAME}/Users' 
TBLPROPERTIES (
"xmlinput.start"="<row "
,"xmlinput.end"="/>"
);


----------------------------------------------
--
--                   VOTES
--
----------------------------------------------


DROP TABLE IF EXISTS RAW.VOTES;

CREATE EXTERNAL TABLE IF NOT EXISTS RAW.VOTES
(
  voteid       STRING
, postid       STRING
, votetypeid   STRING
, creationdate STRING
, userid       STRING
, bountyamount STRING
 )
 COMMENT 'Votes.xml'
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.voteid"="/row/@Id",
"column.xpath.postid"="/row/@PostId",
"column.xpath.votetypeid"="/row/@VoteTypeId",
"column.xpath.creationdate"="/row/@CreationDate",
"column.xpath.userid"="/row/@UserId" ,
"column.xpath.bountyamount"="/row/@BountyAmount" 

)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/user/hive/xml/${hiveconf:FORUM_NAME}/Votes' 
TBLPROPERTIES (
"xmlinput.start"="<row "
,"xmlinput.end"="/>"
);

----------------------------------------------
--
--                  POSTLINKS
--
----------------------------------------------


DROP TABLE IF EXISTS RAW.POSTLINKS;

CREATE EXTERNAL TABLE IF NOT EXISTS RAW.POSTLINKS
(
  linkid        STRING
, creationdate  STRING
, postid        STRING
, relatedpostid STRING
, linktypeid    STRING
 )
 COMMENT 'PostLinks.xml'
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.linkid"="/row/@Id",
"column.xpath.creationdate"="/row/@CreationDate",
"column.xpath.postid"="/row/@PostId",
"column.xpath.relatedpostid"="/row/@RelatedPostId",
"column.xpath.linktypeid"="/row/@LinkTypeId"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/user/hive/xml/${hiveconf:FORUM_NAME}/PostLinks' 
TBLPROPERTIES (
"xmlinput.start"="<row "
,"xmlinput.end"="/>"
);


----------------------------------------------
--
--                 POSTHISTORY
--
----------------------------------------------


DROP TABLE IF EXISTS RAW.POSTHISTORY;

CREATE EXTERNAL TABLE IF NOT EXISTS RAW.POSTHISTORY
(
  rowid             STRING
, posthistorytypeid STRING 
, postid            STRING
, revisionguid      STRING
, creationdate      STRING
, userid            STRING
, text              STRING
)
 COMMENT 'PostHistory.xml'
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.rowid"="/row/@Id",
"column.xpath.posthistorytypeid"="/row/@PostHistoryTypeId",
"column.xpath.postid"="/row/@PostId",
"column.xpath.revisionguid"="/row/@RevisionGUID",
"column.xpath.creationdate"="/row/@CreationDate",
"column.xpath.userid"="/row/@UserId",
"column.xpath.text"="/row/@Text"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/user/hive/xml/${hiveconf:FORUM_NAME}/PostHistory' 
TBLPROPERTIES (
"xmlinput.start"="<row "
,"xmlinput.end"="/>"
);



DROP TABLE IF EXISTS RAW.COUNTRY;

CREATE EXTERNAL TABLE RAW.COUNTRY
(
  COUNTRY_NAME             STRING,
  ISO_COUNTRY_CODE         STRING,
  COUNTRY_ID               STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\u003B" STORED AS TEXTFILE
LOCATION "/user/hive/txt/kraje";


DROP TABLE IF EXISTS RAW.VOTETYPE;

CREATE EXTERNAL TABLE RAW.VOTETYPE
(
  VOTE_TYPE_ID             STRING,
  VOTE_TYPE_NAME           STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;' 
STORED AS TEXTFILE
LOCATION 'hdfs:///user/hive/txt/votetypeid';



DROP TABLE IF EXISTS RAW.CLOSEREASON;

CREATE EXTERNAL TABLE RAW.CLOSEREASON
(
  CLOSE_REASON_ID             STRING,
  CLOSE_REASON_NAME           STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;' 
STORED AS TEXTFILE
LOCATION 'hdfs:///user/hive/txt/closereasonid';




DROP TABLE IF EXISTS RAW.POSTLINKTYPE;

CREATE EXTERNAL TABLE RAW.POSTLINKTYPE
(
  POST_LINK_TYPE_ID           STRING,
  POST_LINK_TYPE_NAME           STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;' 
STORED AS TEXTFILE
LOCATION 'hdfs:///user/hive/txt/postlinktypeid';



DROP TABLE IF EXISTS RAW.POSTHISTORYTYPE;

CREATE EXTERNAL TABLE RAW.POSTHISTORYTYPE
(
  POST_HISTORY_TYPE_ID           STRING,
  POST_HISTORY_TYPE_NAME         STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;' 
STORED AS TEXTFILE
LOCATION 'hdfs:///user/hive/txt/posthistorytypeid';


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
from  TEMP.RAW_LOAD_ETL_TIME_LOG a
;


INSERT INTO AUDIT.ETL_TIME_LOG
SELECT 
 cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name 
,'TABLE_LOAD'                     as etl_step_name
,x.database_name
,x.table_name
,x.records_inserted
,0
,0
,CURRENT_TIMESTAMP as load_timestamp
,CURRENT_TIMESTAMP as end_timestamp
,CURRENT_TIMESTAMP as load_timestamp
from 
(
select 'RAW' as database_name ,'BADGES'          as table_name ,count(*) as records_inserted from RAW.BADGES            union all 
select 'RAW' as database_name ,'COMMENTS'        as table_name ,count(*) as records_inserted from RAW.COMMENTS          union all
select 'RAW' as database_name ,'POSTS'           as table_name ,count(*) as records_inserted from RAW.POSTS             union all
select 'RAW' as database_name ,'NPOSTS'          as table_name ,count(*) as records_inserted from RAW.NPOSTS            union all
select 'RAW' as database_name ,'POSTHISTORYTYPE' as table_name ,count(*) as records_inserted from RAW.POSTHISTORYTYPE   union all
select 'RAW' as database_name ,'POSTLINKTYPE'    as table_name ,count(*) as records_inserted from RAW.POSTLINKTYPE      union all
select 'RAW' as database_name ,'CLOSEREASON'     as table_name ,count(*) as records_inserted from RAW.CLOSEREASON       union all
select 'RAW' as database_name ,'VOTETYPE'        as table_name ,count(*) as records_inserted from RAW.VOTETYPE          union all
select 'RAW' as database_name ,'COUNTRY'         as table_name ,count(*) as records_inserted from RAW.COUNTRY           union all
select 'RAW' as database_name ,'POSTHISTORY'     as table_name ,count(*) as records_inserted from RAW.POSTHISTORY       union all
select 'RAW' as database_name ,'POSTLINKS'       as table_name ,count(*) as records_inserted from RAW.POSTLINKS         union all
select 'RAW' as database_name ,'VOTES'           as table_name ,count(*) as records_inserted from RAW.VOTES             union all
select 'RAW' as database_name ,'USERS'           as table_name ,count(*) as records_inserted from RAW.USERS             union all
select 'RAW' as database_name ,'TAGS'            as table_name ,count(*) as records_inserted from RAW.TAGS              union all
select 'RAW' as database_name ,'CALENDAR'        as table_name ,count(*) as records_inserted from RAW.CALENDAR
) x;