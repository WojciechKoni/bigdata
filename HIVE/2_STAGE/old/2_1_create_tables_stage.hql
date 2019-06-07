CREATE DATABASE STAGE;

CREATE TABLE IF NOT EXISTS STAGE.BADGES_TABLE(
BADGEID INT ,
USERID INT ,
NAME STRING,
TIMESTAMP_ STRING,
CLASS INT,
TAGBASED STRING,
DATE_ DATE,
MONTH STRING,
DAY STRING,
HOUR STRING)
PARTITIONED BY (YEAR SMALLINT)
STORED AS ORC
TBLPROPERTIES("orc.compress"="ZLIB","orc.compress.size"="262144","orc.create.index"="true","orc.stripe.size"="268435456","orc.row.index.stride"="3000");

drop table IF EXISTS STAGE.COMMENTS_TABLE;

CREATE TABLE IF NOT EXISTS STAGE.COMMENTS_TABLE (
  COMMENT_ID INT
, PostId INT
, Score INT
, Text STRING
, CreationDate DATE
, CreationDateTS STRING
, UserId INT
,MONTH STRING
,DAY STRING
,HOUR STRING
) 
PARTITIONED BY (YEAR SMALLINT)
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="ZLIB",
"orc.compress.size"="262144",
"orc.create.index"="true",
"orc.stripe.size"="268435456",
"orc.row.index.stride"="3000"
)       ;


DROP TABLE IF EXISTS STAGE.POSTS_TABLE;

CREATE TABLE IF NOT EXISTS STAGE.POSTS_TABLE (
  PostId INT
, PostTypeId INT
, CreationDateTS STRING
, Score INT
, ViewCount INT
, Body STRING
, OwnerUserId INT
, LastActivityDate DATE
, LastActivityTS STRING
, Title STRING
, Tags STRING
, AnswerCount INT
, CommentCount INT
, FavoriteCount INT
, CreationDate DATE
, MONTH STRING
, DAY STRING
, HOUR STRING
 ) 
PARTITIONED BY (YEAR SMALLINT) 
STORED AS ORC
TBLPROPERTIES (
"orc.compress"="ZLIB",
"orc.compress.size"="262144",
"orc.create.index"="true",
"orc.stripe.size"="268435456",
"orc.row.index.stride"="3000"
)       ;


----------------------------------------------
--
--                TAGS_TABLE
--
----------------------------------------------

DROP TABLE IF EXISTS STAGE.TAGS_TABLE;

CREATE TABLE IF NOT EXISTS STAGE.TAGS_TABLE (
  TagId INT
, TagName STRING
, Count INT
 ) STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="ZLIB",
"orc.compress.size"="262144",
"orc.create.index"="true",
"orc.stripe.size"="268435456",
"orc.row.index.stride"="3000"
);


----------------------------------------------
--
--                USERS_TABLE
--
-- , user_site_id SMALLINT --site_id
-- , image_site_id SMALLINT --site_id
----------------------------------------------

DROP TABLE IF EXISTS STAGE.USERS_TABLE;

CREATE TABLE IF NOT EXISTS STAGE.USERS_TABLE (
  UserId INT
, Reputation INT
, CreationDate DATE
, first_name STRING
, last_name STRING
, LastAccessDate DATE
, LastAccessDateHour STRING
, location_id STRING
, AboutMe STRING
, user_site_id SMALLINT  
, image_site_id SMALLINT  
, Views INT
, UpVotes INT
, DownVotes INT
, AccountId INT
 ) STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="ZLIB",
"orc.compress.size"="262144",
"orc.create.index"="true",
"orc.stripe.size"="268435456",
"orc.row.index.stride"="3000"
);



----------------------------------------------
--
--                VOTES_TABLE
--
----------------------------------------------


DROP TABLE IF EXISTS STAGE.VOTES_TABLE;

CREATE TABLE IF NOT EXISTS STAGE.VOTES_TABLE (
  VoteId INT
, PostId INT
, VoteTypeId INT
, CreationDateTS STRING
, CreationDate Date
 )
PARTITIONED BY (YEAR SMALLINT)
STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="ZLIB",
"orc.compress.size"="262144",
"orc.create.index"="true",
"orc.stripe.size"="268435456",
"orc.row.index.stride"="3000"
);



DROP TABLE IF EXISTS STAGE.POST_TAG_RELATION_TABLE;

CREATE TABLE IF NOT EXISTS STAGE.POST_TAG_RELATION_TABLE (
  PostId INT
, TagName STRING
 ) 
STORED AS ORC
TBLPROPERTIES (
"orc.compress"="ZLIB",
"orc.compress.size"="262144",
"orc.create.index"="true",
"orc.stripe.size"="268435456",
"orc.row.index.stride"="3000"
)       ;

insert into STAGE.POST_TAG_RELATION_TABLE
select postid, regexp_replace(regexp_replace(lv.*,'<',''),'>','') 
from raw.posts p 
lateral view explode(split(regexp_replace(tags,'><','>\,<'),',')) lv where tags is not null ;






----------------------------------------------
--
--                POSTHISTORY_TABLE
--
----------------------------------------------

DROP TABLE IF EXISTS STAGE.POSTHISTORY_TABLE;

CREATE TABLE IF NOT EXISTS STAGE.POSTHISTORY_TABLE (

  PostId INT
, PostHistoryTypeId INT
, RevisionGUID STRING
, CreationDateTS STRING
, UserId INT
, Text STRING
, CreationDate DATE
 )
PARTITIONED BY (YEAR SMALLINT)
 STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="ZLIB",
"orc.compress.size"="262144",
"orc.create.index"="true",
"orc.stripe.size"="268435456",
"orc.row.index.stride"="3000"
);

----------------------------------------------
--
--                POSTLINKS_TABLE
--
----------------------------------------------

DROP TABLE IF EXISTS STAGE.POSTLINKS_TABLE;

CREATE TABLE IF NOT EXISTS STAGE.POSTLINKS_TABLE (
  linkid  INT       
, creationdateTS STRING     
, postid  INT          
, relatedpostid    INT
, linktypeid       STRING
, CreationDate DATE
, MONTH STRING
, DAY STRING
, HOUR STRING
 )
PARTITIONED BY (YEAR SMALLINT)
 STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="ZLIB",
"orc.compress.size"="262144",
"orc.create.index"="true",
"orc.stripe.size"="268435456",
"orc.row.index.stride"="3000"
);

----------------------------------------------
--
--                SITE_TABLE
--
----------------------------------------------

DROP TABLE IF EXISTS STAGE.SITE_TABLE;

CREATE TABLE IF NOT EXISTS STAGE.SITE_TABLE (
  site_id  INT       
, site_name STRING     
)
 STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="ZLIB"
);

----------------------------------------------

truncate table STAGE.SITE_TABLE;

insert overwrite table STAGE.SITE_TABLE
select row_number() over (order by host_name) -1, coalesce(host_name,'UNKNOWN')
from (
select distinct regexp_replace(host_name,'www.','') as host_name from (
select parse_url(profileimageurl,'HOST') as host_name
from raw.users
union all
select parse_url(href,'HOST') as host_name
from raw.users
union all
select NULL as host_name
) a
) a2;



----------------------------------------------
--
--                LOCATION_TABLE
--
----------------------------------------------

DROP TABLE IF EXISTS STAGE.LOCATION_TABLE;

CREATE TABLE IF NOT EXISTS STAGE.LOCATION_TABLE (
  location_id  INT       
, location_descr STRING
, country_id SMALLINT
, part_id SMALLINT
, city_id INT         
)
 STORED AS ORC
 TBLPROPERTIES (
"orc.compress"="ZLIB"
);

truncate table STAGE.LOCATION_TABLE;

insert into STAGE.LOCATION_TABLE(location_id, location_descr, country_id)
select row_number() over (order by location) -1, coalesce(location,'UNKNOWN'), country_id
from 
(
  select distinct usr.location, cast(cntry.country_id as int) as country_id
  from raw.users usr
  join raw.country cntry
    on 1=1
 where locate(upper(cntry.country_name),upper(usr.location)) >0
  union all
  select NULL as location, cast(0 as int) as country_id
) a 
;


insert into STAGE.LOCATION_TABLE(location_id, location_descr, country_id)
select row_number() over (order by location) -1 + max_location_id, coalesce(location,'UNKNOWN'), 0 as country_id
from 
(
  select distinct location 
  from raw.users
   where location not in (select location_descr from STAGE.LOCATION_TABLE where location_descr is not null)
   and location is not null
  
) a 
join (select max(location_id) as max_location_id from stage.location_table) location
on 1=1 ;








 select  usr.location, cast(cntry.country_id as int) as country_id
  from raw.users usr
  join raw.country cntry
    on 1=1
 where locate(upper(cntry.country_name),upper(usr.location)) >0
 
 and locate('SWEDEN', UPPER(location))>0
 
  union all
  select NULL as location, cast(0 as int) as country_id




 







