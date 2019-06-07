
insert into STAGE.badges
select  
 coalesce(badgeid,-999) 
,userid,-999) 
,upper(name)  
,timestamp_ 
,class  
,tagbased  
,cast("${env:FORUM_NAME}" as STRING) as forum_name       
,CURRENT_TIMESTAMP as start_timestamp     
from raw.badges;



insert into STAGE.comments  
select
 coalesce(commentid,-999)        
,coalesce(postid,-999)           
,score            
,upper(text)           
,creationdatets   
,coalesce(userid,-999)           
,cast("${env:FORUM_NAME}" as STRING) as forum_name       
,current_timestamp
from raw.comments;

----------------------------------------------
--
--                posts
--
----------------------------------------------

insert into stage.posts  
select
 postid                     
,posttypeid                 
,parentid                   
,acceptedanswerid           
,creationdatets             
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
,cast("${env:FORUM_NAME}" as STRING) as forum_name       
,current_timestamp
from raw.posts;

----------------------------------------------
--
--                tags
--
----------------------------------------------


insert into  STAGE.tags
select
  coalesce(tagid,-999)                 
, tagname               
, count                 
, cast("${env:FORUM_NAME}" as STRING) as forum_name       
, current_timestamp       
from raw.tags
;

----------------------------------------------
--
--                users
--
----------------------------------------------

insert into   STAGE.users  
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
, cast("${env:FORUM_NAME}" as  STRING) as forum_name       
, current_timestamp   
)
from raw.users;



----------------------------------------------
--
--                votes
--
----------------------------------------------

insert  into STAGE.votes (
select
  voteid  
, coalesce(postid,-999)  
, coalesce(votetypeid,-999)  
, creationdate  
, cast("${env:FORUM_NAME}" as  STRING) as forum_name       
, current_timestamp   
from raw.votes;

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
, cast("${env:FORUM_NAME}" as  STRING) as forum_name       
, current_timestamp   
from raw.country
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
, cast("${env:FORUM_NAME}" as  STRING) as forum_name       
, current_timestamp   
from raw.closereason
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
, cast("${env:FORUM_NAME}" as  STRING) as forum_name       
, current_timestamp   
from raw.votetype
;

----------------------------------------------
--
--                posthistory
--
----------------------------------------------

insert into STAGE.posthistory
Select
  rowid                   
, posthistorytypeid       
, coalesce(postid,-999)                  
, revisionguid         
, creationdate         
, userid               
, upper(text)                 
, cast("${env:FORUM_NAME}" as  STRING) as forum_name       
, current_timestamp   
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
, cast("${env:FORUM_NAME}" as  STRING) as forum_name       
, current_timestamp   
from raw.posthistorytype
;

----------------------------------------------
--
--                postlinks
--
----------------------------------------------
insert into STAGE.postlinks
select
  linkid          
, creationdate    
, coalesce(postid,-999)          
, coalesce(relatedpostid,-999)   
, linktypeid      
, cast("${env:FORUM_NAME}" as  STRING) as forum_name       
, current_timestamp   
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
, cast("${env:FORUM_NAME}" as  STRING) as forum_name       
, current_timestamp   
from raw.postlinktype
;

truncate table STAGE.calendar;

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
from raw.calendar;