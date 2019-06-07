
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS TEMP.MAIN1_LOAD_ETL_TIME_LOG;

CREATE TABLE TEMP.MAIN1_LOAD_ETL_TIME_LOG
AS 
select 
 cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name 
,cast('LOAD' as STRING) as etl_step_name      
,cast('MAIN' as STRING) as database_name      
,cast('ALL' as STRING) table_name         
,CURRENT_TIMESTAMP as start_timestamp    
;


INSERT INTO MAIN.D_VOTE_TYPE
SELECT 
 vote_type_id      
,UPPER(vote_type_name)    
,current_timestamp as load_timestamp
FROM STAGE.VOTETYPE
where vote_type_id not in (select vote_type_id as vti from  MAIN.D_VOTE_TYPE)
; 


INSERT INTO MAIN.D_CLOSE_REASON
SELECT
    close_reason_id           
   ,close_reason_name         
   ,current_timestamp as load_timestamp
FROM STAGE.CLOSEREASON
where close_reason_id not in (select close_reason_id as cri FROM MAIN.D_CLOSE_REASON)
;


alter table main.f_comment drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');


INSERT INTO MAIN.F_COMMENT PARTITION(forum_name) 
select
 commentid as comment_id          
,postid as post_id             
,score               
,text                
,SUBSTRING(creationdatets,1,10)  as creationdate       
,from_unixtime(unix_timestamp(creationdatets, "yyyy-MM-dd'T'HH:mm:ss.SSS")) as creation_date_ts    
,userid as user_id             
,SUBSTRING(creationdatets,6,2)  as creation_date_month   
,SUBSTRING(creationdatets,9,2)  as creation_date_day    
,SUBSTRING(creationdatets,12,2) as creation_date_hour          
,SUBSTRING(creationdatets,1,4)  as creation_date_year          
,CURRENT_TIMESTAMP as load_timestamp     
,cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name  
from STAGE.comments;




INSERT INTO MAIN.D_COUNTRY 
SELECT 
 country_id      
,country_name    
,iso_country_code
,current_timestamp as load_timestamp  
from STAGE.country
where country_id not in (select country_id as cid from MAIN.D_COUNTRY )
;


alter table MAIN.D_TAG drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');


INSERT INTO MAIN.D_TAG  PARTITION(forum_name) 
SELECT
  tagid as tag_id        
, tagname       
, count         
, current_timestamp as load_timestamp
,cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name 
FROM STAGE.tags
;


alter table MAIN.D_SITE drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

INSERT INTO MAIN.D_SITE   PARTITION(forum_name) 
SELECT row_number() over (order by host_name) -1 + max_site_id as site_id
     , coalesce(host_name,'UNKNOWN') as site_name
     , current_timestamp as load_timestamp
     ,cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name  
from (
select distinct regexp_replace(host_name,'www.','') as host_name, 1 as key 
from (
        select parse_url(profileimageurl,'HOST') as host_name
          from stage.users
         union all
        select parse_url(href,'HOST') as host_name
          from stage.users
         union all
        select NULL as host_name
     ) a
) a2
join (select 1 as key, max(site_id) as max_site_id from MAIN.D_SITE) as max_id
  on max_id.key = a2.key
;


INSERT INTO MAIN.D_POST_HIST_TYPE
SELECT
  post_history_type_id                        as post_hist_type_id 
 ,trim(split(post_history_type_name,'-')[0]) as post_hist_type_name
 ,trim(split(post_history_type_name,'-')[1]) as post_hist_type_descr   
, current_timestamp as load_timestamp
FROM stage.posthistorytype
where post_history_type_id not in (select post_history_type_id as pht from MAIN.D_POST_HIST_TYPE)
;

 
INSERT INTO MAIN.D_POST_LINK_TYPE
SELECT
  post_link_type_id    
 ,post_link_type_name  
, current_timestamp as load_timestamp
FROM stage.postlinktype
where post_link_type_id not in (select post_link_type_id as plti from  MAIN.D_POST_LINK_TYPE)
;



alter table MAIN.D_VOTE drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');
  
 
INSERT INTO MAIN.D_VOTE PARTITION(forum_name) 
SELECT
  voteid                                                  as vote_id 
, postid                                                  as post_id 
, votetypeid                                              as vote_type_id 
, substring(creationdate,1,10)                            as creation_date 
, userid                                                  as user_id
, substring(creationdate  ,1,4)                           as creation_date_year
, current_timestamp                                       as load_timestamp 
, cast("${hiveconf:FORUM_NAME}" as STRING)                as forum_name 
FROM stage.votes;



INSERT INTO MAIN.D_LOCATION 
SELECT row_number() over (order by location) -1  as location_id
       , coalesce(location,'UNKNOWN')            as location_descr_
       , country_id
       , NULL                                    as part_id
       , NULL                                    as city_id
       , current_timestamp                       as load_timestamp
from 
(
 select location  
        ,min(country_id) as country_id
  from 
  (
  SELECT usr.location
        , cast(cntry.country_id as int) as country_id
  FROM stage.users usr
  JOIN main.d_country cntry
    ON 1=1
 WHERE locate(upper(cntry.country_name),upper(usr.location)) >0
  UNION ALL
  SELECT NULL as location, cast(0 as int) as country_id
 ) b
 group by location
) a 
where coalesce(location,'UNKNOWN') not in (select location_descr as lsr from MAIN.D_LOCATION where location_descr is not null)
;




alter table MAIN.D_USER drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');
 
INSERT INTO MAIN.D_USER PARTITION(forum_name) 
SELECT
 user_id              
,reputation           
,creation_date        
,first_name           
,last_name            
,last_access_date     
,last_access_date_hour
,location_id          
,about_me             
,user_site_id         
,image_site_id        
,views                
,up_votes             
,down_votes           
,account_id           
,activity_status      
,load_timestamp       
,cast("${hiveconf:FORUM_NAME}" as STRING)                 as forum_name 
from 
(
SELECT
  userid                         as user_id 
, reputation             
, substring(creationdate,1,10)   as creation_date 
 ,case when split(displayname,' ')[1] is not null and  split(displayname,' ')[2] is null then UPPER(split(displayname,' ')[0]) else UPPER(displayname) end as first_name
 ,case when split(displayname,' ')[1] is not null and  split(displayname,' ')[2] is null then UPPER(split(displayname,' ')[1]) end as last_name
 ,substring(lastaccessdate,1,10) as last_access_date
 ,substring(lastaccessdate,12,2) as last_access_date_hour
 ,coalesce(locations.location_id,0) as location_id
 ,regexp_replace(aboutme, '<.*?>' , "")                        as about_me
 ,sites1.site_id                  as user_site_id
 ,sites2.site_id                 as image_site_id
 ,views                  
 ,upvotes                        as up_votes  
 ,downvotes                      as down_votes 
 ,accountid                      as account_id 
 ,case when datediff(current_date, creationdate)> 400 then 'INACTIVE' else 'ACTIVE' end as activity_status        
 ,current_timestamp              as load_timestamp         
 ,row_number() over (partition by usr.userid order by usr.userid) as rnum
 
 from stage.users usr
 join stage.sites sites1
   on sites1.site_name = COALESCE(regexp_replace(parse_url(usr.href,'HOST'),'www.',''),'UNKNOWN')
  and sites1.forum_name = '${hiveconf:FORUM_NAME}'
  
join stage.sites sites2
  on sites2.site_name = COALESCE(regexp_replace(parse_url(usr.profileimageurl,'HOST'),'www.',''),'UNKNOWN')
 and sites2.forum_name = '${hiveconf:FORUM_NAME}'

left join main.d_location locations
on locations.location_descr = COALESCE(usr.location,'UNKNOWN')
) data_
where data_.rnum = 1;



alter table MAIN.F_POST_TAG_RELATION drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

INSERT INTO MAIN.F_POST_TAG_RELATION PARTITION(forum_name) 
SELECT 
       posts_tags.post_id       
      ,dtag.tag_id        
      ,current_timestamp as load_timestamp
      ,cast("${hiveconf:FORUM_NAME}" as STRING) as forum_name    
FROM
(
select postid                                              as post_id
     , regexp_replace(regexp_replace(lv.*,'<',''),'>','')  as tag_name
FROM stage.posts p 
LATERAL VIEW EXPLODE(SPLIT(REGEXP_REPLACE(tags,'><','>\,<'),',')) lv 
WHERE tags IS NOT NULL 
) posts_tags
JOIN main.D_TAG dtag
  ON dtag.tag_name = posts_tags.tag_name;
  
  

alter table MAIN.F_POST_ACTIVITY drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');
 

INSERT INTO MAIN.F_POST_ACTIVITY PARTITION(forum_name) 
SELECT  
  all_posts.owneruserid                                                                           as user_id                         
 ,all_posts.PostID                                                                                as post_id                         
 ,cast(from_unixtime(unix_timestamp(all_posts.creationdatets, "yyyy-MM-dd'T'HH:mm:ss.SSS")) as timestamp)            as post_create_timestamp           
 ,cast(from_unixtime(unix_timestamp(first_answers_info.creationdatets, "yyyy-MM-dd'T'HH:mm:ss.SSS")) as timestamp)   as first_answer_create_timestamp   
 ,first_answers_info.owneruserid                                                                  as first_answer_author_id          
 ,cast(from_unixtime(unix_timestamp(first_comments_info.creationdatets, "yyyy-MM-dd'T'HH:mm:ss.SSS")) as timestamp)  as first_comment_create_timestamp  
 ,first_comments_info.userid                                                                      as first_comment_author_id         
 ,cast(from_unixtime(unix_timestamp(accepted_answers.creationdatets, "yyyy-MM-dd'T'HH:mm:ss.SSS")) as timestamp)     as accepted_answer_create_timestamp
 ,accepted_answers.owneruserid                                                                    as accepted_answer_author_id       
 ,substring(all_posts.creationdatets, 1, 4)
 ,current_timestamp                                                                               as load_timestamp
 ,cast("${hiveconf:FORUM_NAME}" as STRING)                                                        as forum_name
 
 from stage.posts all_posts
 
 left join stage.posts accepted_answers
        on accepted_answers.postid = all_posts.acceptedanswerid 
 
 left join 
(
 select min(postid) as postid, ParentID 
   from stage.posts 
  where PostTypeId=2
    and forum_name='${hiveconf:FORUM_NAME}'
  group by ParentID
 ) first_answers_ids
 on first_answers_ids.ParentID = all_posts.postid
 
 left join stage.posts first_answers_info
        on first_answers_info.postid = first_answers_ids.postid
 
 left join 
(
 select min(commentid) as commentid, PostID 
   from stage.comments
  where forum_name='${hiveconf:FORUM_NAME}'    
  group by PostID
 ) first_comments
 on first_comments.postid = all_posts.postid

 left join stage.comments first_comments_info
        on first_comments_info.commentid = first_comments.commentid
        and first_comments_info.forum_name='${hiveconf:FORUM_NAME}'
 
 where all_posts.posttypeid='1';
 
 
----------------------------------------------
--
--             F_POST
--
----------------------------------------------



alter table MAIN.F_POST drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');
 
INSERT INTO MAIN.F_POST PARTITION(forum_name) 
SELECT rp.postid                                                                                as post_id
     , rp.posttypeid                                                                            as post_type_id
     , cast(from_unixtime(unix_timestamp(rp.creationdatets, "yyyy-MM-dd'T'HH:mm:ss.SSS")) as timestamp)            as creation_timestamp   
     , rp.parentid                                                                              as parent_id
     , rp.acceptedanswerid                                                                      as accepted_answer_id 
     , rp.lasteditordisplayname                                                                 as last_editor_display_name 
     , from_unixtime(unix_timestamp(rp.lasteditdate, "yyyy-MM-dd'T'HH:mm:ss.SSS"))              as last_edit_timestamp 
     , from_unixtime(unix_timestamp(rp.lastactivitydatets, "yyyy-MM-dd'T'HH:mm:ss.SSS"))        as last_activity_timestamp 
     , from_unixtime(unix_timestamp(rp.communityowneddate, "yyyy-MM-dd'T'HH:mm:ss.SSS"))        as community_owned_timestamp 
     , from_unixtime(unix_timestamp(rp.closeddate, "yyyy-MM-dd'T'HH:mm:ss.SSS"))                as closed_timestamp
     , rp.score                
     , rp.viewcount                                                                             as view_count           
     , rp.body                                                                                  as body                                                                               
     , size(split(regexp_replace(regexp_replace(rp.body,"\\?",""),"\\.",""),' '))               as body_word_count
     , size(split(regexp_replace(regexp_replace(nps.cleansed_body,"\\?",""),"\\.",""),' '))     as cleansed_body_word_count   
     , rp.owneruserid                                                                           as owner_user_id        
     , substring(rp.lastactivitydatets,1,10)                                                    as last_activity_date                                                                 
     , substring(rp.lastactivitydatets,12,2)                                                    as last_activity_hour
     , rp.lasteditoruserid                                                                      as last_editor_user_id
     , rp.title                                                                                 as title                
     , rp.answercount                                                                           as answer_count         
     , rp.commentcount                                                                          as comment_count        
     , rp.favoritecount                                                                         as favorite_count       
     , substring(rp.creationdatets,1,10)                                                        as creation_date        
     , substring(rp.creationdatets,6,2)                                                         as creation_month       
     , substring(rp.creationdatets,9,2)                                                         as creation_day         
     , substring(rp.creationdatets,12,2)                                                        as creation_hour        
     , substring(rp.creationdatets,1,4)                                                         as creation_date_year
     , current_timestamp                                                                        as load_timestamp 
     , cast("${hiveconf:FORUM_NAME}" as STRING)                                                 as forum_name             
     from stage.posts rp
join stage.posts_cleansed nps
  on rp.postid     = nps.postid
 and rp.forum_name = nps.forum_name
 and rp.forum_name = '${hiveconf:FORUM_NAME}' 
 and nps.forum_name = '${hiveconf:FORUM_NAME}'
;

INSERT INTO MAIN.D_CALENDAR
SELECT   
 cal_date      
,day_in_week    
,day_in_month  
,day_in_year   
,week_in_month 
,month_num     
,quarter_num   
,year_num 
,substring(cal_date,1,7) as year_month     
,day_name      
,month_name    
,current_timestamp as load_timestamp
from STAGE.CALENDAR
where cal_date not in (select cal_date as calendar_date from MAIN.D_CALENDAR)
;


alter table MAIN.D_BADGE  drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert into MAIN.D_BADGE PARTITION(forum_name) 
select
      badgeid         
     ,userid          
     ,name            
     ,substring(timestamp_,1,10)      
     ,class  
     ,current_timestamp as load_timestamp
     , cast("${hiveconf:FORUM_NAME}" as STRING)  as forum_name   
from stage.badges
where forum_name='${hiveconf:FORUM_NAME}';


-- F_POST_LINK


alter table MAIN.F_POST_LINK drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

 
INSERT INTO MAIN.F_POST_LINK PARTITION(forum_name) 
SELECT 
  linkid                       as  link_id                
 ,cast(from_unixtime(
                     unix_timestamp(
                     creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS")) as timestamp) 
                      as  creation_timestamp     
 ,postid                       as  post_id                
 ,relatedpostid                as  related_post_id        
 ,linktypeid                   as  link_type_id           
 ,substring(creationdate,1,10) as  create_date            
 ,current_timestamp            as  load_timestamp    
 , cast("${hiveconf:FORUM_NAME}" as STRING)   as forum_name             
     from stage.postlinks
     where forum_name = '${hiveconf:FORUM_NAME}' 
;


-------------------------

alter table MAIN.F_POST_HISTORY drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

 
INSERT INTO MAIN.F_POST_HISTORY PARTITION(forum_name) 
SELECT 
 rowid                         as row_id  
,posthistorytypeid             as post_history_type_id
,postid                        as post_id  
,revisionguid                  as revision_guid 
,cast(from_unixtime(
                     unix_timestamp(
                     creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS")) as timestamp)  as  creation_timestamp
,userid                        as user_id  
,text                          as text
,substring(creationdate,1,10)  as create_date
,current_timestamp             as load_timestamp
,cast("${hiveconf:FORUM_NAME}" as STRING)   as forum_name  
  from stage.posthistory
     where forum_name = '${hiveconf:FORUM_NAME}' 
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
from  TEMP.MAIN1_LOAD_ETL_TIME_LOG a
;



