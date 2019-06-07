
----------------------------------------------
--
--                   BADGES
--
----------------------------------------------

DROP TABLE IF EXISTS RAW.BADGES;

CREATE TABLE IF NOT EXISTS RAW.BADGES
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
LOCATION '/user/hive/xml/Badges' 
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
LOCATION '/user/hive/xml/' 
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
LOCATION '/user/hive/xml/Comments' 
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
LOCATION '/user/hive/xml/Posts' 
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
"orc.compress.size"="262144",
"orc.create.index"="true",
"orc.stripe.size"="268435456",
"orc.row.index.stride"="3000"
)       ;

truncate table RAW.NPOSTS;

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
LOCATION '/user/hive/xml/Tags' 
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
LOCATION '/user/hive/xml/Users' 
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
LOCATION '/user/hive/xml/Votes' 
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
LOCATION '/user/hive/xml/PostLinks' 
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
LOCATION '/user/hive/xml/PostHistory' 
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




DROP TABLE IF EXISTS RAW.FORUM;

CREATE EXTERNAL TABLE RAW.FORUM
(
  FORUM_ID             STRING,
  FORUM_NAME           STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;' 
STORED AS TEXTFILE
LOCATION 'hdfs:///user/hive/txt/forumid';

















/*
DROP TABLE IF EXISTS RAW.COUNTRY;

CREATE EXTERNAL TABLE RAW.COUNTRY
(
  COUNTRY_NAME             STRING,
  ISO_COUNTRY_CODE         STRING,
  COUNTRY_ID               STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\u003B" STORED AS TEXTFILE
LOCATION "/user/hive/txt/kraje";

*/












/*
row format delimited    
    FIELDS TERMINATED BY ','
    stored as textfile
    LOCATION '/Data';

CREATE EXTERNAL TABLE IF NOT EXISTS RAW.COUNTRY
(
  country_id         INT
, country           STRING 
, iso_country_code  STRING
)
 COMMENT 'COUNTRIES';
 */
 
 
 
