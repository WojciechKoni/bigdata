
--te 4 do wypelnienia hqlem 
-- reszta sparkiem


DROP TABLE IF EXISTS AGGR.CURRENT_USERS_AGGR;

CREATE TABLE AGGR.CURRENT_USERS_AGGR
(
   period_date                  date,
   num_of_all_users            int,
   num_of_inactive_users       int,
   num_of_active_users         int,
   num_of_commenting_users     int,
   num_of_asking_users         int,
   num_of_answering_users      int,
   max_questions_asked_by_user int,
   max_answered_questions      int,
   max_single_u_comments_num   int,
   max_user_reputation         int,
   total_words_in_posts        int,      
   max_words_posts_in_one_day  int,
   load_timestamp              timestamp
) 
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(period_date) INTO 2 BUCKETS
STORED AS ORC
TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;



DROP TABLE IF EXISTS AGGR.CURRENT_POSTS_AGGR;

CREATE TABLE AGGR.CURRENT_POSTS_AGGR
(
   period_date                   date,
   post_life_len_in_days        int,
   num_of_posts                 int,
   num_of_posts_with_no_answer  int,
   num_of_posts_with_no_comm    int,    
   num_of_posts_answered        int,
   num_of_posts_answ_commented  int,  
   max_num_of_words_in_post     int,
   max_to_answer_time_in_days   int,
   max_number_of_tags_per_post  int,
   load_timestamp               timestamp
) 
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(period_date) INTO 2 BUCKETS
STORED AS ORC
TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;



DROP TABLE IF EXISTS AGGR.USERS_AGGR_TIMESERIES;

CREATE TABLE AGGR.USERS_AGGR_TIMESERIES
(
   period_date                 date,
   num_of_new_users            int,
   num_of_users_csum           int,
   num_of_asking_users         int,
   num_of_answering_users      int,
   num_of_commenting_users     int,
   num_of_receiving_badge      int,
   num_of_cl_1_badges_given    smallint,
   num_of_cl_2_badges_given    smallint,
   num_of_cl_3_badges_given    smallint,
   load_timestamp              timestamp
) 
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(period_date) INTO 2 BUCKETS
STORED AS ORC
TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) 
;



DROP TABLE IF EXISTS AGGR.POSTS_AGGR_TIMESERIES;

CREATE TABLE AGGR.POSTS_AGGR_TIMESERIES
(
   period_date                  date,
   num_of_posts_csum            int,
   num_of_new_posts             int,
   num_of_closed_posts          int,
   num_of_posts_with_no_answer  int,
   num_of_new_answers           int,
   num_of_new_comments          int,
   avg_tag_per_post             smallint,
   avg_num_of_words_in_question int,
   avg_num_of_words_in_answer   int,
   avg_to_answer_time_in_days   smallint,
   max_to_answer_time_in_days   smallint,
   max_number_of_tags_per_post  smallint,
   num_of_words_in_posts        int,
   load_timestamp               timestamp
)  
PARTITIONED BY (forum_name VARCHAR(100))
CLUSTERED BY(period_date) INTO 2 BUCKETS
STORED AS ORC
TBLPROPERTIES (
"orc.compress"="SNAPPY",
"orc.create.index"="true",
"orc.row.index.stride"="1000"
) ;




DROP TABLE IF EXISTS AGGR.POSTS_CURRENT_RANKING;

CREATE TABLE AGGR.POSTS_CURRENT_RANKING
(
   period_date                  date,
   post_id                      int,
   most_linked_rank             smallint,
   links_count                  int,
   most_viewed_rank             smallint,
   views_count                  int,
   most_favourite_rank          smallint,           
   favorite_count               int,  
   owner_user_id                int,
   title                        varchar(1000),
   load_timestamp               timestamp
)  
PARTITIONED BY (forum_name VARCHAR(100))
STORED AS ORC
;



DROP TABLE IF EXISTS AGGR.USERS_POPULARITY_CURRENT_RANKING;

CREATE TABLE AGGR.USERS_POPULARITY_CURRENT_RANKING
(
   period_date                  date
  ,user_id                      int
  ,view_count                   int
  ,view_rank_value              smallint 
  ,vote_count                   int 
  ,vote_rank_value              smallint
  ,name                         varchar(100)
  ,location                     varchar(200)
  ,country_name                 varchar(100)
  ,load_timestamp               timestamp
)  
PARTITIONED BY (forum_name VARCHAR(100))
STORED AS ORC
;


DROP TABLE IF EXISTS AGGR.USERS_VALUE_BY_LINKS_CURRENT_RANKING;

CREATE TABLE AGGR.USERS_VALUE_BY_LINKS_CURRENT_RANKING
(
   period_date                  date
  ,user_id                      int
  ,link_count                   int
  ,link_rank_value              smallint 
  ,name                         varchar(100) 
  ,location                     varchar(200)
  ,country_name                 varchar(100)
  ,load_timestamp               timestamp
)  
PARTITIONED BY (forum_name VARCHAR(100))
STORED AS ORC;

DROP TABLE IF EXISTS AGGR.POST_BODY_WORDS_CURRENT_RANKING;

CREATE TABLE AGGR.POST_BODY_WORDS_CURRENT_RANKING
(
   period_date                  date
  ,word                         varchar(200)
  ,use_count                    int
  ,use_rank_value               smallint
  ,load_timestamp               timestamp
)  
PARTITIONED BY (forum_name VARCHAR(100))
STORED AS ORC
;


DROP TABLE IF EXISTS AGGR.POST_TITLE_WORDS_CURRENT_RANKING;

CREATE TABLE AGGR.POST_BODY_TITLE_CURRENT_RANKING
(
   period_date                  date
  ,word                         varchar(200)
  ,use_count                    int
  ,use_rank_value               smallint 
  ,load_timestamp               timestamp
)  
PARTITIONED BY (forum_name VARCHAR(100))
STORED AS ORC
;


DROP TABLE IF EXISTS AGGR.TAGS_POPULARITY_CURRENT_RANKING;

CREATE TABLE AGGR.TAGS_POPULARITY_CURRENT_RANKING
(
   period_date                  date
  ,tag_id                       int
  ,tag_name                     varchar(200)
  ,use_count                    int
  ,use_rank_value               smallint 
  ,load_timestamp               timestamp
)  
PARTITIONED BY (forum_name VARCHAR(100))
STORED AS ORC
;



DROP TABLE IF EXISTS AGGR.COLUMN_STATS;

CREATE TABLE AGGR.COLUMN_STATS
(
   database_name     string
  ,table_name        string
  ,column_name       string
  ,min_value         string
  ,max_value         string
  ,unique_values     string
  ,count             string
  ,nulls             string
  ,avg_value         string 
  ,load_timestamp    timestamp
  ,forum_name        VARCHAR(100)
)  
STORED AS ORC;

--PARTITIONED BY (forum_name VARCHAR(100))


DROP TABLE IF EXISTS AGGR.TABLE_STATISTICS;

CREATE TABLE IF NOT EXISTS AGGR.TABLE_STATISTICS
(
 raw_table_name     VARCHAR(30)       
,stage_table_name   VARCHAR(30)       
,main_table_name    VARCHAR(30)       
,raw_recs           int
,stage_recs         int
,main_recs          int
,load_timestamp     TIMESTAMP
)
PARTITIONED BY (forum_name VARCHAR(100))
STORED AS ORC;

--ile czasu trzeba czekać na odpowiedź na posta w zależności od jakiego taga
--ile czasu trzeba czekać na odpowiedź na posta
--liczba słów w poscie
--
--suma słów zapisanych przez najlepiej ocenianych użytkowników top 10
--suma słów zapisanych w postach podpiętych do tagów top 10
--aktywność dniowa, godzinowa, w jakie dni tygodnia
--jaka część użytkowników pisze największą część pytań
--jaka część użytkowników pisze największą część odpowiedzi
--jaka część użytkowników pisze największą część komentarzy
--ile tagów przypada średnio na post
--w których tagach jest najwięcej postów? top 10 tagów?
--posty z których tagów są najczęściej wyświetlane?
--stan posta do komentarzy dodatkowy wymiar?
--stan posta w stos do odpowiedzi dodatkowy wymiar?

