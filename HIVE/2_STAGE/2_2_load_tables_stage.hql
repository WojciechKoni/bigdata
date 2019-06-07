
set hive.enforce.bucketing=true;
set hive.variable.substitute=true;
set hive.exec.dynamic.partition.mode=nonstrict;

add jars hdfs:/user/hive/unnamed.jar;


DROP TABLE IF EXISTS TEMP.STAGE1_LOAD_ETL_TIME_LOG;

CREATE TABLE TEMP.STAGE1_LOAD_ETL_TIME_LOG
AS 
select 
 cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name 
,cast('LOAD' as STRING) as etl_step_name      
,cast('STAGE' as STRING) as database_name      
,cast('ALL' as STRING) table_name         
,CURRENT_TIMESTAMP as start_timestamp    
;



alter table STAGE.posts_cleansed drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert into STAGE.posts_cleansed partition(forum_name)
select  
 postid               
,body                
,upper(remove_stopwords(regexp_replace(body, '<.*?>' , "") ))                
,upper(title)                
,substring(creationdatets,1,4)
,CURRENT_TIMESTAMP as start_timestamp     
,cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name       
from raw.posts;       


alter table STAGE.badges drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert into STAGE.badges partition(forum_name)
select  
 coalesce(badgeid,-999) 
,coalesce(userid,-999) 
,upper(name)  
,regexp_replace(timestamp_,'T',' ') 
,class  
,tagbased  
,substring(timestamp_,1,4)
,CURRENT_TIMESTAMP as start_timestamp     
,cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name       
from raw.badges
;


alter table STAGE.comments drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert into STAGE.comments partition(forum_name) 
select
 coalesce(commentid,-999)        
,coalesce(postid,-999)           
,score            
,upper(text)           
,regexp_replace(creationdatets,'T',' ')    
,coalesce(userid,-999)           
,substring(creationdatets,1,4)
,current_timestamp
,cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name  
from raw.comments;

----------------------------------------------
--
--                posts
--
----------------------------------------------


alter table STAGE.posts drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert into stage.posts partition(forum_name) 
select
 postid                     
,posttypeid                 
,parentid                   
,acceptedanswerid           
,regexp_replace(creationdatets,'T',' ')                
,score                      
,viewcount                  
,upper(body)                       
,coalesce(owneruserid,-999)                
,lasteditoruserid           
,lasteditordisplayname      
,lasteditdate               
,lastactivitydatets         
,communityowneddate         
,closeddate                 
,title                      
,tags                       
,answercount                
,commentcount           
,favoritecount          
,substring(creationdatets,1,4)     
,current_timestamp
,cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name  
from raw.posts;

----------------------------------------------
--
--                tags
--
----------------------------------------------

alter table STAGE.tags drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');


insert into  STAGE.tags partition(forum_name) 
select
  coalesce(tagid,-999)                 
, tagname               
, count                 
, current_timestamp       
, cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name 
from raw.tags
;

----------------------------------------------
--
--                users
--
----------------------------------------------

alter table STAGE.users drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert into   STAGE.users  PARTITION (forum_name)
select
 userid  
,reputation  
,creationdate  
,upper(displayname)  
,lastaccessdate  
,location  
,upper(aboutme)  
,href  
,profileimageurl  
,views  
,upvotes  
,downvotes  
,accountid  
,substring(creationdate,1,4) 
,current_timestamp
,cast("${hiveconf:FORUM_NAME}" as  STRING) as forum_name    
from raw.users
;



----------------------------------------------
--
--                votes
--
----------------------------------------------

alter table STAGE.votes drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert  into STAGE.votes  PARTITION (forum_name)
select
  voteid  
, coalesce(postid,-999)  
, coalesce(votetypeid,-999)  
, creationdate  
, substring(creationdate,1,4)  
, userid
, current_timestamp   
, cast("${hiveconf:FORUM_NAME}" as  STRING) as forum_name 
from raw.votes
;

----------------------------------------------
--
--                country
--
----------------------------------------------

insert into STAGE.country 
select
  country_name        
, iso_country_code    
, country_id               
, current_timestamp   
from raw.country
where country_id not in (select country_id as cid from STAGE.country)
;


----------------------------------------------
--
--                closereason
--
----------------------------------------------

insert into STAGE.closereason 
select
  close_reason_id         
, upper(close_reason_name)    
, cast("${hiveconf:FORUM_NAME}" as  STRING) as forum_name       
, current_timestamp   
from raw.closereason
where close_reason_id not in (select close_reason_id as cri from STAGE.closereason)
;


----------------------------------------------
--
--                votetype
--
----------------------------------------------

insert into STAGE.votetype
select
  vote_type_id            
, upper(vote_type_name)       
, cast("${hiveconf:FORUM_NAME}" as  STRING) as forum_name       
, current_timestamp   
from raw.votetype
where vote_type_id not in (select vote_type_id as fti from STAGE.votetype)
;

----------------------------------------------
--
--                posthistory
--
----------------------------------------------


alter table STAGE.posthistory drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert into STAGE.posthistory partition(forum_name)
Select
  rowid                   
, posthistorytypeid       
, coalesce(postid,-999)                  
, revisionguid         
, creationdate         
, userid               
, upper(text)                 
, substring(creationdate,1,4) 
, current_timestamp   
, cast("${hiveconf:FORUM_NAME}" as  STRING) as forum_name 
from raw.posthistory
;
----------------------------------------------
--
--                posthistorytype
--
----------------------------------------------

insert into STAGE.posthistorytype
select
  post_history_type_id          
, upper(post_history_type_name)     
, current_timestamp   
from raw.posthistorytype
;

----------------------------------------------
--
--                postlinks
--
----------------------------------------------

alter table STAGE.postlinks drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert into STAGE.postlinks partition(forum_name)
select
  linkid          
, creationdate    
, coalesce(postid,-999)          
, coalesce(relatedpostid,-999)   
, linktypeid      
, substring(creationdate,1,4)  
, current_timestamp 
, cast("${hiveconf:FORUM_NAME}" as  STRING) as forum_name   
from raw.postlinks
;
----------------------------------------------
--
--                postlinktype
--
----------------------------------------------

insert into STAGE.postlinktype
select
  post_link_type_id         
 ,upper(post_link_type_name)       
, current_timestamp   
from raw.postlinktype
where post_link_type_id not in (select post_link_type_id as plti from STAGE.postlinktype)
;

insert into STAGE.calendar
select 
   cal_date                  
  ,day_in_week               
  ,day_in_month              
  ,day_in_year               
  ,week_in_month             
  ,month_num                 
  ,quarter_num               
  ,year_num                  
  ,day_name                  
  ,month_name                
from raw.calendar
where cal_date not in (select cal_date as cld from STAGE.calendar)
;


alter table STAGE.sites drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert INTO STAGE.sites partition(forum_name)
select row_number() over (order by host_name) -1
     , coalesce(host_name,'UNKNOWN')
     , current_timestamp   
     , cast("${hiveconf:FORUM_NAME}" as  STRING) as forum_name  
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


insert into STAGE.LOCATION PARTITION(forum_name)
select row_number() over (order by location) -1 + max_country_id 
     , coalesce(location,'UNKNOWN')
     ,NULL
     ,NULL
     , country_id
     ,cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name  
from 
(
  select distinct usr.location, cast(cntry.country_id as int) as country_id, 1 as key
  from raw.users usr
  join raw.country cntry
    on 1=1
 where locate(upper(cntry.country_name),upper(usr.location)) >0
  union all
  select NULL as location, cast(0 as int) as country_id, 1 as key
) a 
join (select max(country_id) as max_country_id, 1 as key from STAGE.LOCATION) cid
where a.key = cid.key;



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
from  TEMP.STAGE1_LOAD_ETL_TIME_LOG a
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
select 'STAGE' as database_name ,'BADGES         '    as table_name ,count(*) as records_inserted from STAGE.BADGES            union all 
select 'STAGE' as database_name ,'CALENDAR       '    as table_name ,count(*) as records_inserted from STAGE.CALENDAR          union all
select 'STAGE' as database_name ,'CLOSEREASON    '    as table_name ,count(*) as records_inserted from STAGE.CLOSEREASON       union all
select 'STAGE' as database_name ,'COMMENTS       '    as table_name ,count(*) as records_inserted from STAGE.COMMENTS          union all
select 'STAGE' as database_name ,'COUNTRY        '    as table_name ,count(*) as records_inserted from STAGE.COUNTRY           union all
select 'STAGE' as database_name ,'POSTHISTORY    '    as table_name ,count(*) as records_inserted from STAGE.POSTHISTORY       union all
select 'STAGE' as database_name ,'POSTHISTORYTYPE'    as table_name ,count(*) as records_inserted from STAGE.POSTHISTORYTYPE   union all
select 'STAGE' as database_name ,'POSTLINKS      '    as table_name ,count(*) as records_inserted from STAGE.POSTLINKS         union all
select 'STAGE' as database_name ,'POSTLINKTYPE   '    as table_name ,count(*) as records_inserted from STAGE.POSTLINKTYPE      union all
select 'STAGE' as database_name ,'POSTS          '    as table_name ,count(*) as records_inserted from STAGE.POSTS             union all
select 'STAGE' as database_name ,'POSTS_CLEANSED '    as table_name ,count(*) as records_inserted from STAGE.POSTS_CLEANSED    union all
select 'STAGE' as database_name ,'SITES          '    as table_name ,count(*) as records_inserted from STAGE.SITES             union all
select 'STAGE' as database_name ,'TAGS           '    as table_name ,count(*) as records_inserted from STAGE.TAGS              union all
select 'STAGE' as database_name ,'USERS          '    as table_name ,count(*) as records_inserted from STAGE.USERS             union all
select 'STAGE' as database_name ,'VOTES          '    as table_name ,count(*) as records_inserted from STAGE.VOTES             union all
select 'STAGE' as database_name ,'VOTETYPE       '    as table_name ,count(*) as records_inserted from STAGE.VOTETYPE       
) x;