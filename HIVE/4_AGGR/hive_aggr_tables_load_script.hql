

set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;


alter table AGGR.CURRENT_USERS_AGGR drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');


insert into AGGR.CURRENT_USERS_AGGR PARTITION(forum_name) 
select 
start.period_dte 
,coalesce(a.inactive_counter + a.active_counter,-1)  as num_of_all_users
,coalesce(a.inactive_counter                  ,-1)   as num_of_inactive_users
,coalesce(a.active_counter                    ,-1)   as num_of_active_users
,coalesce(b.unique_users_counter              ,-1)   as num_of_commenting_users
,coalesce(d.unique_users_counter              ,-1)   as num_of_asking_users
,coalesce(e.unique_users_counter              ,-1)   as num_of_answering_users
,coalesce(f.max_counter                       ,-1)   as max_questions_asked_by_user 
,coalesce(g.max_counter                       ,-1)   as max_answered_questions    
,coalesce(h.max_counter                       ,-1)   as max_single_u_comments_num 
,coalesce(i.max_reputation                    ,-1)   as max_user_reputation       
,coalesce(j.sum_of_words_in_posts             ,-1)   as total_sum_of_words_in_forum     
,coalesce(k.max_num                           ,-1)   as max_sum_words_in_one_day  
,current_timestamp                                   as load_timestamp               
,'${hiveconf:FORUM_NAME}'                            as forum_name

from (select max(cal_date) as period_dte, 1 as artificial_key from temp.posts_ts_dates) start

left join temp.users_existing_current a
on a.artificial_key = start.artificial_key      

left join temp.users_commenting_by_date b
on b.artificial_key = a.artificial_key
and b.creation_date = start.period_dte

left join temp.users_commenting_total c
on c.artificial_key = a.artificial_key

left join temp.users_asking_total d
on d.artificial_key = a.artificial_key

left join temp.users_answering_total e
on e.artificial_key = a.artificial_key
and e.creation_date = start.period_dte

left join temp.questions_by_user f
on f.artificial_key = a.artificial_key

left join temp.answers_by_user g
on g.artificial_key = a.artificial_key

left join temp.comments_by_user h
on h.artificial_key = a.artificial_key

left join temp.user_reputation i
on i.artificial_key = a.artificial_key

left join temp.total_words j
on j.artificial_key = a.artificial_key

left join temp.words_in_one_day k
on k.artificial_key = a.artificial_key
;

alter table AGGR.CURRENT_POSTS_AGGR drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert into AGGR.CURRENT_POSTS_AGGR PARTITION(forum_name) 
select 
  start.period_dte
, d.time_to_close_in_days                            as post_life_len_in_days      
, a.counter                                          as num_of_posts               
, b.cnt_not_answered                                 as num_of_posts_with_no_answer
, c.cnt_not_commented                                as num_of_posts_with_no_comm  
, b.cnt_answered                                     as num_of_posts_answered      
, c.cnt_commented                                    as num_of_posts_answ_commented
, a.max_num_of_cl_words                              as max_num_of_words_in_post   
, d.time_to_answer_in_days                           as max_to_answer_time_in_days 
, e.counter                                          as max_number_of_tags_per_post
, current_timestamp                                  as load_timestamp
,'${hiveconf:FORUM_NAME}'                            as forum_name

from (select max(cal_date) as period_dte, 1 as artificial_key from temp.posts_ts_dates) start

left join temp.total_posts_type_1 a
  on a.artificial_key = start.artificial_key 

left join temp.question_vs_answer_num b
  on b.artificial_key = a.artificial_key
  
left join temp.question_vs_comment_num c
  on c.artificial_key = a.artificial_key

left join temp.posts_vs_time d
  on d.artificial_key = a.artificial_key 

left join temp.posts_vs_tags e
  on d.artificial_key = a.artificial_key 
;



alter table AGGR.USERS_AGGR_TIMESERIES drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert into AGGR.USERS_AGGR_TIMESERIES PARTITION(forum_name) 
select 
 a.cal_date                              as period_date
,coalesce(b.counter             ,0)      as num_of_new_users   
,coalesce(b.num_of_users_csum   ,0)      as num_of_users_csum     
,coalesce(c.unique_users_counter,0)      as num_of_asking_users     
,coalesce(e.unique_users_counter,0)      as num_of_answering_users  
,coalesce(d.unique_users_counter,0)      as num_of_commenting_users 
,coalesce(f.counter             ,0)      as num_of_receiving_badge      
,coalesce(f.class_1_badges_given,0)      as num_of_cl_1_badges_given  
,coalesce(f.class_2_badges_given,0)      as num_of_cl_2_badges_given  
,coalesce(f.class_3_badges_given,0)      as num_of_cl_3_badges_given  
,current_timestamp           as load_timestamp 
,'${hiveconf:FORUM_NAME}'    as forum_name   

from temp.users_ts_dates a

left join temp.new_users b
  on b.artificial_key = a.artificial_key
 and b.period_dte    = a.cal_date
 
left join temp.users_asking_by_date c
  on c.artificial_key = a.artificial_key
 and c.creation_date  = a.cal_date
 
left join temp.users_commenting_by_date d
  on d.artificial_key = a.artificial_key
 and d.creation_date  = a.cal_date

left join temp.users_answering_by_date e
  on e.artificial_key = a.artificial_key
 and e.creation_date  = a.cal_date
 
left join temp.users_vs_badges f
  on f.artificial_key = a.artificial_key
 and f.period_dte  = a.cal_date;
 


alter table AGGR.POSTS_AGGR_TIMESERIES drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert into AGGR.POSTS_AGGR_TIMESERIES PARTITION(forum_name) 
select 
 a.cal_date                          as period_date
,coalesce(c.counter                   ,0)        as num_of_posts_csum 
,coalesce(d.questions_created_counter ,0)        as num_of_new_posts           
,coalesce(b.counter                   ,0)        as num_of_closed_posts
,coalesce(e.counter                   ,0)        as num_of_posts_with_no_answer
,coalesce(d.answers_created_counter   ,0)        as num_of_new_answers         
,coalesce(h.counter                   ,0)        as num_of_new_comments         
,coalesce(f.avg_tpp                   ,0)        as avg_tag_per_post         
,coalesce(g.avg_words_in_q            ,0)        as avg_num_of_words_in_question
,coalesce(g.avg_words_in_a            ,0)        as avg_num_of_words_in_answer   
,coalesce(i.avg_time_to_answer_in_days,0)        as avg_to_answer_time_in_days 
,coalesce(i.max_time_to_answer_in_days,0)        as max_to_answer_time_in_days 
,coalesce(f.max_tpp                   ,0)        as max_number_of_tags_per_post 
,coalesce(g.num_of_words_in_posts     ,0)        as num_of_words_in_posts

,current_timestamp                   as load_timestamp             
,'${hiveconf:FORUM_NAME}'            as forum_name

from temp.posts_ts_dates a

left join temp.posts_closed b
  on b.artificial_key = a.artificial_key
 and b.period_date    = a.cal_date

left join temp.posts_existing c 
 on c.artificial_key = a.artificial_key
and c.period_date    = a.cal_date

left join temp.posts_asked_vs_answered d
 on d.artificial_key = a.artificial_key
and d.period_date    = a.cal_date

left join temp.posts_with_no_answer e
 on e.artificial_key = a.artificial_key
and e.period_date    = a.cal_date

left join temp.tag_per_post f
 on f.artificial_key = a.artificial_key
and f.period_date    = a.cal_date

left join temp.words_per_post g
 on g.artificial_key = a.artificial_key
and g.period_date    = a.cal_date

left join temp.new_comments h
 on h.artificial_key = a.artificial_key
and h.period_date    = a.cal_date

left join temp.posts_vs_answer_by_date i
 on i.artificial_key = a.artificial_key
and i.period_date    = a.cal_date;




alter table AGGR.POSTS_CURRENT_RANKING drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert into AGGR.POSTS_CURRENT_RANKING PARTITION(forum_name) 
select 
 start.period_dte 
,ranks.post_id
,ranks.rank_value
,ranks.link_count
,ranks.view_count
,ranks.most_viewed_rank
,ranks.favorite_count
,ranks.most_favorite_rank
,posts.owner_user_id
,posts.title
,current_timestamp                                   as load_timestamp               
,'${hiveconf:FORUM_NAME}'                            as forum_name

from (select max(cal_date) as period_dte, 1 as artificial_key from temp.posts_ts_dates) start
left join 
(
select  coalesce(vf_ranks.post_id , link_ranks.post_id) as post_id
      , vf_ranks.view_count
      , vf_ranks.most_viewed_rank
      , vf_ranks.favorite_count
      , vf_ranks.most_favorite_rank
      , link_ranks.rank_value
      , link_ranks.link_count
      ,1                               as artificial_key 
      
from (
select  coalesce(view_ranks.post_id , favorite_ranks.post_id) as post_id
       ,view_ranks.view_count  
       ,view_ranks.rank_value           as most_viewed_rank
       ,favorite_ranks.favorite_count
       ,favorite_ranks.rank_value       as most_favorite_rank

from
( select * from (
select post_id, view_count, rank_value from (select post_id, view_count, rank() over (order by view_count desc) rank_value 
from main.f_post where forum_name='${hiveconf:FORUM_NAME}' ) a ) b where rank_value<101) view_ranks

full outer join 

(select * from (
select post_id, favorite_count, rank_value from (select post_id, favorite_count, rank() over (order by favorite_count desc) rank_value
 from main.f_post where forum_name='${hiveconf:FORUM_NAME}' ) a ) b where rank_value<101) favorite_ranks

 on view_ranks.post_id = favorite_ranks.post_id
) vf_ranks  

full outer join 
(
select post_id, link_count,  rank_value
from 
(
select * from (
select  post_id, link_count, rank() over (order by link_count desc) rank_value, row_number
from (select related_post_id as post_id, count(*) over (partition by related_post_id) link_count
            ,row_number() over (partition by related_post_id order by 1) row_number
        from main.f_post_link
        where link_type_id=1
         and forum_name='${hiveconf:FORUM_NAME}' 
     ) a ) b
     where row_number=1
) c
where rank_value<=100
) link_ranks    
on link_ranks.post_id = vf_ranks.post_id

) ranks
on ranks.artificial_key = start.artificial_key

left join main.f_post posts
       on ranks.post_id = posts.post_id
      and posts.forum_name='${hiveconf:FORUM_NAME}' ;
