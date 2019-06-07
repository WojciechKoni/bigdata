drop table if exists temp.users_created_by_date;

create table temp.users_created_by_date
as
select substring(creation_date, 1,10) creation_date
     , cast(count(*) as int) counter
  from main.d_user
 group by substring(creation_date, 1,10)
    ;



drop table if exists temp.users_answering_total;

create table temp.users_answering_total
as 
select creation_date
,count(distinct owner_user_id) unique_users_counter
     , 1 as artificial_key
 from main.f_post 
 where post_type_id = 2 
  and forum_name = '${hiveconf:FORUM_NAME}'
group by creation_date;



drop table if exists temp.users_existing_current;

create table temp.users_existing_current
as select 
       current_date as today_date
     , cast(sum(case when activity_status='INACTIVE' then 1 else 0 end) as int) inactive_counter
     , cast(sum(case when activity_status='ACTIVE' then 1 else 0 end) as int) active_counter
     , 1 as artificial_key
  from main.d_user
 where forum_name = '${hiveconf:FORUM_NAME}'
  group by current_date
;


drop table if exists temp.users_commenting_total;

create table temp.users_commenting_total
as 
select count(distinct user_id) unique_users_counter
     , 1 as artificial_key
 from main.f_comment 
where forum_name = '${hiveconf:FORUM_NAME}'
;


drop table if exists temp.users_commenting_by_date;

create table temp.users_commenting_by_date
as 
select creation_date
,count(distinct user_id) unique_users_counter
     , 1 as artificial_key
 from main.f_comment 
where forum_name = '${hiveconf:FORUM_NAME}' 
group by creation_date;

drop table if exists temp.users_asking_by_date;

create table temp.users_asking_by_date
as 
select creation_date 
,count(distinct owner_user_id) unique_users_counter
     , 1 as artificial_key
 from main.f_post 
 where forum_name = '${hiveconf:FORUM_NAME}'
  and post_type_id = 1 
group by creation_date;

drop table if exists temp.users_asking_total;

create table temp.users_asking_total
as 
select count(distinct owner_user_id) unique_users_counter
     , 1 as artificial_key
 from main.f_post 
where forum_name = '${hiveconf:FORUM_NAME}' 
  and post_type_id = 1 
;

drop table if exists temp.users_answering_by_date;

create table temp.users_answering_by_date
as 
select creation_date
,count(distinct owner_user_id) unique_users_counter
     , 1 as artificial_key
 from main.f_post 
 where post_type_id = 2 
   and forum_name = '${hiveconf:FORUM_NAME}'
group by creation_date;

drop table if exists temp.most_active_users_comments;

create table temp.most_active_users_comments
as 
select count(*) unique_users_counter
     ,user_id
     , 1 as artificial_key
 from main.f_comment
where forum_name = '${hiveconf:FORUM_NAME}' 
  group by user_id  
;



drop table if exists temp.users_image_sites_current;

create table temp.users_image_sites_current
as 
select 
  dsite.site_name
  ,count(*) aaa
       , 1 as artificial_key
  from main.d_user duser
  join main.d_site dsite
    on duser.image_site_id = dsite.site_id
  group by dsite.site_name
;



drop table if exists temp.questions_by_user;

create table temp.questions_by_user
as 
select owner_user_id as user_id
      ,counter as max_counter
      , 1 as artificial_key       
from (
       select owner_user_id
            , counter
            , row_number() over (order by counter desc) as row_number
         from
         ( 
         select count(*) as counter
              , owner_user_id
          from main.f_post 
         where forum_name = '${hiveconf:FORUM_NAME}' 
           and post_type_id = 1 
         group by owner_user_id
        ) a
    ) b
where row_number=1
;

drop table if exists temp.answers_by_user;

create table temp.answers_by_user
as 
select owner_user_id as user_id
      ,counter as max_counter
      , 1 as artificial_key       
from (
       select owner_user_id
            , counter
            , row_number() over (order by counter desc) as row_number
         from
         ( 
         select count(*) as counter
              , owner_user_id
          from main.f_post 
         where forum_name = '${hiveconf:FORUM_NAME}' 
           and post_type_id = 2 
         group by owner_user_id
        ) a
    ) b
where row_number=1
;

drop table if exists temp.comments_by_user;

create table temp.comments_by_user
as 
select user_id 
      ,counter as max_counter
      , 1 as artificial_key       
from (
       select user_id
            , counter
            , row_number() over (order by counter desc) as row_number
         from
         ( 
         select count(*) as counter
              , user_id
          from main.f_comment  
         where forum_name = '${hiveconf:FORUM_NAME}' 
 
         group by user_id
        ) a
    ) b
where row_number=1
;



drop table if exists temp.user_reputation;

create table temp.user_reputation
as 
select max(reputation) max_reputation
      , 1 as artificial_key      
from main.d_user 
 where forum_name = '${hiveconf:FORUM_NAME}' 
;

drop table if exists temp.total_words;

create table temp.total_words
as 
select sum(cleansed_body_word_count) sum_of_words_in_posts
      , 1 as artificial_key      
from main.f_post 
 where forum_name = '${hiveconf:FORUM_NAME}' 
;

drop table if exists temp.words_in_one_day;

create table temp.words_in_one_day
as 
select max(words_sum) max_num
      , 1 as artificial_key      
from (
select substring(creation_timestamp,1,10) 
     , sum(cleansed_body_word_count) words_sum

  from main.f_post
 where forum_name = '${hiveconf:FORUM_NAME}'
 group by substring(creation_timestamp,1,10)
) x;

---------------
--- tabele do postow
---

drop table if exists temp.total_posts_type_1;

create table temp.total_posts_type_1
as 
select count(post_id) counter
     ,max(cleansed_body_word_count) max_num_of_cl_words
     , 1 as artificial_key
 from main.f_post 
where forum_name = '${hiveconf:FORUM_NAME}' 
  and post_type_id = 1 
;

drop table if exists temp.question_vs_answer_num;

create table temp.question_vs_answer_num
as 
select sum(case when first_answer_create_timestamp is null then 1 else 0 end) as cnt_not_answered
      ,sum(case when first_answer_create_timestamp is null then 0 else 1 end) as cnt_answered
      ,1 as artificial_key
 from main.f_post_activity 
where forum_name = '${hiveconf:FORUM_NAME}' 
;

drop table if exists temp.question_vs_comment_num;

create table temp.question_vs_comment_num
as 
select sum(case when first_comment_create_timestamp is null then 1 else 0 end) cnt_not_commented
      ,sum(case when first_comment_create_timestamp is null then 0 else 1 end) cnt_commented
      ,1 as artificial_key
 from main.f_post_activity 
where forum_name = '${hiveconf:FORUM_NAME}' 
;


drop table if exists temp.posts_vs_time;

create table temp.posts_vs_time
as 
select max(datediff(fpa.first_answer_create_timestamp , fpa.post_create_timestamp)) as time_to_answer_in_days 
     ,max(datediff(posts.closed_timestamp , fpa.post_create_timestamp)) as time_to_close_in_days
     ,1 as artificial_key
 from main.f_post_activity fpa
 join main.f_post posts
   on fpa.post_id = posts.post_id
  and fpa.forum_name = posts.forum_name
  and posts.forum_name = '${hiveconf:FORUM_NAME}' 
  and fpa.forum_name = '${hiveconf:FORUM_NAME}' 
;

drop table if exists temp.posts_vs_tags;

create table temp.posts_vs_tags
as 
    select max(counter) as counter
from 
(
select count(distinct tag_id) as counter
      ,post_id
 from MAIN.F_POST_TAG_RELATION
 where forum_name = '${hiveconf:FORUM_NAME}' 
group by post_id
) a
;

----------
----------
-- tabele timesries klucz 1 + data
----
--- tabela pomocnicza z datami
--- 

drop table if exists temp.users_ts_dates;

create table temp.users_ts_dates
as select
      cal.cal_date
     , 1 as artificial_key
 from main.D_CALENDAR cal
 join (
       select min(creation_date) as analysis_start_date
            , max(creation_date) as analysis_end_date
            , 1 as artificial_key 
        from main.D_USER 
       where forum_name = '${hiveconf:FORUM_NAME}'
      ) duser
  on duser.artificial_key = 1
where cal.cal_date >= analysis_start_date 
  and cal.cal_date <= analysis_end_date 
;

drop table if exists temp.new_users;

create table temp.new_users
as 
select counter
     ,sum(counter) over (order by period_dte rows between unbounded preceding and current row )  num_of_users_csum
     ,sum(counter) over (partition by substring(period_dte,1,7))  num_of_new_users_mth
      ,period_dte
      ,artificial_key
from (
      select coalesce(count(*),0) as counter
           , 1 as artificial_key
           , user_dates.cal_date as period_dte
        from temp.users_ts_dates user_dates
       left join main.d_user duser
         on user_dates.cal_date = duser.creation_date
      where duser.forum_name = '${hiveconf:FORUM_NAME}'  
      group by artificial_key, user_dates.cal_date 
) a
;


drop table if exists temp.users_vs_badges;

create table temp.users_vs_badges
as 
select coalesce(count(distinct dbadge.user_id),0) as counter
      ,coalesce(sum(case when dbadge.class='1' then 1 else 0 end),0) as class_1_badges_given 
      ,coalesce(sum(case when dbadge.class='2' then 2 else 0 end),0) as class_2_badges_given
      ,coalesce(sum(case when dbadge.class='3' then 3 else 0 end),0) as class_3_badges_given
     , 1 as artificial_key
     , user_dates.cal_date as period_dte
 
 from temp.users_ts_dates user_dates
 left join main.d_badge dbadge
   on user_dates.cal_date = substring(dbadge.badge_date,1,10)
  and user_dates.artificial_key = 1
where dbadge.forum_name = '${hiveconf:FORUM_NAME}'  
group by artificial_key,  user_dates.cal_date
;


drop table if exists temp.posts_ts_dates;

create table temp.posts_ts_dates
as select
      cal.cal_date
     , 1 as artificial_key
 from main.D_CALENDAR cal
 join (
       select min(creation_date) as analysis_start_date
            , max(creation_date) as analysis_end_date
            , 1 as artificial_key 
        from main.F_POST
       where forum_name = '${hiveconf:FORUM_NAME}' 
      ) duser
  on duser.artificial_key = 1
where cal.cal_date >= analysis_start_date 
  and cal.cal_date <= analysis_end_date 
     
;



drop table if exists temp.posts_closed;

create table temp.posts_closed
as 
select  count(*) as counter 
      , 1 as artificial_key
      , posts_dates.cal_date as period_date

 from temp.posts_ts_dates posts_dates
 left join main.f_post fp
   on substring(fp.closed_timestamp,1,10)= posts_dates.cal_date
  and fp.forum_name = '${hiveconf:FORUM_NAME}'
  and posts_dates.artificial_key = 1 
  where post_type_id = 1
group by artificial_key, posts_dates.cal_date
;

drop table if exists temp.posts_existing;

create table temp.posts_existing
as 
select  count(*) as counter 
      , 1 as artificial_key
      , posts_dates.cal_date as period_date

 from temp.posts_ts_dates posts_dates
 left join (
       select post_type_id
             ,substring(creation_timestamp,1,10) creation_date
         from main.f_post 
        where forum_name = '${hiveconf:FORUM_NAME}'
          and post_type_id = 1
      ) fp
   on 1=1
  where posts_dates.artificial_key = 1
    and coalesce(cast(fp.creation_date as date), cast('1900-01-01' as date)) between cast('1900-01-01' as date) and  posts_dates.cal_date 
group by artificial_key, posts_dates.cal_date
;

drop table if exists temp.posts_asked_vs_answered;

create table temp.posts_asked_vs_answered
as 
select  sum(case when post_type_id = 1 then 1 else 0 end) as questions_created_counter 
      , sum(case when post_type_id = 2 then 1 else 0 end) as answers_created_counter
      , 1 as artificial_key
      , posts_dates.cal_date as period_date

 from temp.posts_ts_dates posts_dates
 left join (
       select post_type_id
             ,substring(creation_timestamp,1,10) creation_date
         from main.f_post 
        where forum_name = '${hiveconf:FORUM_NAME}'
      ) fp
   on fp.creation_date = posts_dates.cal_date 
 where posts_dates.artificial_key = 1
 -- and coalesce(cast(fp.creation_date as date), cast('1900-01-01' as date)) between cast('1900-01-01' as date) and  posts_dates.cal_date 
group by artificial_key, posts_dates.cal_date
;





drop table if exists temp.posts_with_no_answer;

create table temp.posts_with_no_answer
as 
select count(*) as counter 
      , 1 as artificial_key
      , posts_dates.cal_date as period_date

 from temp.posts_ts_dates posts_dates
left join (
       select fp.post_type_id
             ,substring(fp.creation_timestamp,1,10) creation_date
             ,fpa.first_answer_create_timestamp 
         from main.f_post_activity fpa
         join main.f_post fp
           on fp.post_id = fpa.post_id 
        where fp.forum_name = '${hiveconf:FORUM_NAME}'
          and fpa.forum_name = '${hiveconf:FORUM_NAME}'
          and fpa.forum_name = fp.forum_name
      ) fpa
  on 1=1
 where posts_dates.artificial_key = 1
 and coalesce(cast(fpa.creation_date as date), cast('1900-01-01' as date)) between cast('1900-01-01' as date) and  posts_dates.cal_date 
 and coalesce(cast(fpa.first_answer_create_timestamp as date), cast('1900-01-01' as date)) > posts_dates.cal_date 
 
group by artificial_key, posts_dates.cal_date
;


drop table if exists temp.tag_per_post;

create table temp.tag_per_post
as 
select counters.avg_tpp
      ,counters.max_tpp
      , 1 as artificial_key
      , posts_dates.cal_date as period_date

 from temp.posts_ts_dates posts_dates

 left join (
            select avg(counter) as avg_tpp
                  ,max(counter) as max_tpp
                  ,substring(fp.creation_timestamp,1,10) as create_date
            from (
                   select  post_id
                          ,count(*) as counter
                     from main.f_post_tag_relation 
                    where forum_name = '${hiveconf:FORUM_NAME}'                   
                   group by post_id
                 ) fptr
            join  main.f_post fp            
              on fptr.post_id = fp.post_id
             and fp.forum_name   = '${hiveconf:FORUM_NAME}'
            group by substring(fp.creation_timestamp,1,10)
          ) counters
      on counters.create_date = posts_dates.cal_date
;     

drop table if exists temp.words_per_post;

create table temp.words_per_post
as 
select sum(case when post_type_id = 1 then cleansed_body_word_count else 0 end)/count(fp.post_id) as avg_words_in_q 
      ,sum(case when post_type_id = 2 then cleansed_body_word_count else 0 end)/count(fp.post_id) as avg_words_in_a
      ,sum(cleansed_body_word_count)                                                              as num_of_words_in_posts
      , 1 as artificial_key
      , posts_dates.cal_date as period_date

 from temp.posts_ts_dates posts_dates

 left join main.f_post fp
   on fp.forum_name   = '${hiveconf:FORUM_NAME}'
   
where fp.creation_date<= posts_dates.cal_date
  
  group by artificial_key, posts_dates.cal_date
;


drop table if exists temp.new_comments;

create table temp.new_comments
as 
select  coalesce(count(*),0) as counter
      , 1 as artificial_key
      , posts_dates.cal_date as period_date

 from temp.posts_ts_dates posts_dates

 left join main.f_comment fc
   on substring(fc.creation_date_timestamp,1,10)= posts_dates.cal_date
  and fc.forum_name   = '${hiveconf:FORUM_NAME}'

   group by artificial_key, posts_dates.cal_date
;


drop table if exists temp.posts_vs_answer_by_date;

create table temp.posts_vs_answer_by_date
as 
select fpa.avg_time_to_answer_in_days 
      ,fpa.max_time_to_answer_in_days 
      ,fpa.time_to_close_in_days
      ,1 as artificial_key
      , posts_dates.cal_date as period_date

 from temp.posts_ts_dates posts_dates

left join (
       select substring(fp.creation_timestamp,1,10) creation_date
             ,avg((unix_timestamp(fpa.first_answer_create_timestamp) - unix_timestamp(fp.creation_timestamp))/60/60/24)  as avg_time_to_answer_in_days 
             ,max((unix_timestamp(fpa.first_answer_create_timestamp) - unix_timestamp(fp.creation_timestamp))/60/60/24)  as max_time_to_answer_in_days   
             ,avg( (unix_timestamp(fp.closed_timestamp) -  unix_timestamp(fp.creation_timestamp))/60/60/24 )            as time_to_close_in_days

         from main.f_post_activity fpa
         join main.f_post fp
           on fp.post_id = fpa.post_id 
          and fp.post_type_id = 1
        where fp.forum_name = '${hiveconf:FORUM_NAME}'
          and fpa.forum_name = '${hiveconf:FORUM_NAME}'
          and fpa.forum_name = fp.forum_name
          group by substring(fp.creation_timestamp,1,10)
 
      ) fpa
  on fpa.creation_date = posts_dates.cal_date 
 where posts_dates.artificial_key = 1
 --and coalesce(cast(fpa.creation_date as date), cast('1900-01-01' as date)) between cast('1900-01-01' as date) 
 ;

select 'users_created_by_date' as table_name, count(*) from temp.users_created_by_date                union all
select 'users_existing_current' as table_name, count(*) from temp.users_existing_current              union all
select 'users_commenting_total' as table_name, count(*) from temp.users_commenting_total              union all
select 'users_commenting_by_date' as table_name, count(*) from temp.users_commenting_by_date          union all
select 'users_asking_by_date' as table_name, count(*) from temp.users_asking_by_date                  union all
select 'users_asking_total' as table_name, count(*) from temp.users_asking_total                      union all
select 'users_answering_by_date' as table_name, count(*) from temp.users_answering_by_date            union all
select 'users_answering_total' as table_name, count(*) from temp.users_answering_total                union all
select 'most_active_users_comments' as table_name, count(*) from temp.most_active_users_comments      union all
select 'users_image_sites_current' as table_name, count(*) from temp.users_image_sites_current        union all
select 'questions_by_user' as table_name, count(*) from temp.questions_by_user                        union all
select 'answers_by_user' as table_name, count(*) from temp.answers_by_user                            union all
select 'comments_by_user' as table_name, count(*) from temp.comments_by_user                          union all
select 'user_reputation' as table_name, count(*) from temp.user_reputation                            union all
select 'total_words' as table_name, count(*) from temp.total_words                                    union all
select 'words_in_one_day' as table_name, count(*) from temp.words_in_one_day                          union all
select 'total_posts_type_1' as table_name, count(*) from temp.total_posts_type_1                      union all
select 'question_vs_answer_num' as table_name, count(*) from temp.question_vs_answer_num              union all
select 'question_vs_comment_num' as table_name, count(*) from temp.question_vs_comment_num            union all
select 'posts_vs_time' as table_name, count(*) from temp.posts_vs_time                                union all
select 'posts_vs_tags' as table_name, count(*) from temp.posts_vs_tags                                union all
select 'users_ts_dates' as table_name, count(*) from temp.users_ts_dates                              union all
select 'new_users' as table_name, count(*) from temp.new_users                                        union all
select 'users_vs_badges' as table_name, count(*) from temp.users_vs_badges                            union all
select 'posts_ts_dates' as table_name, count(*) from temp.posts_ts_dates                              union all
select 'posts_closed' as table_name, count(*) from temp.posts_closed                                  union all
select 'posts_existing' as table_name, count(*) from temp.posts_existing                              union all
select 'posts_asked_vs_answered' as table_name, count(*) from temp.posts_asked_vs_answered            union all
select 'posts_with_no_answer' as table_name, count(*) from temp.posts_with_no_answer                  union all
select 'tag_per_post' as table_name, count(*) from temp.tag_per_post                                  union all
select 'words_per_post' as table_name, count(*) from temp.words_per_post                              union all
select 'new_comments' as table_name, count(*) from temp.new_comments                                  union all
select 'posts_vs_answer_by_date' as table_name, count(*) from temp.posts_vs_answer_by_date;
