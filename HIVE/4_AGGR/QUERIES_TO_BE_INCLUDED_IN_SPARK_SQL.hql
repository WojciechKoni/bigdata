QUERIES TO BE INCLUDED IN SPARK SQL


alter table AGGR.users_value_by_links_current_ranking drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');

insert into aggr.users_value_by_links_current_ranking partition (forum_name)
select 
  start.period_dte
  ,ranking.user_id
  ,coalesce(ranking.links_per_user_count,0)
  ,coalesce(ranking.rank_value,300)
  ,concat(coalesce(user_.first_name,'') , coalesce(user_.last_name,''))
  ,location_.location_descr 
  ,country.country_name
 ,current_timestamp                                   as load_timestamp               
,'${hiveconf:FORUM_NAME}'                            as forum_name
from (select max(cal_date) as period_dte, 1 as artificial_key from temp.posts_ts_dates) start
join 
(
select 
b.*
, 1 as artificial_key
from (
 select  user_id, links_per_user_count, row_number() over (order by links_per_user_count desc) rank_value
   from (select  owner_user_id as user_id
             ,count(*)    over (partition by owner_user_id) links_per_user_count
             ,row_number() over (partition by owner_user_id order by 1) row_number
        from main.f_post_link fpl
        join main.f_post post
          on fpl.related_post_id = post.post_id
         and fpl.link_type_id=1
         and fpl.forum_name = post.forum_name
         and fpl.forum_name = '${hiveconf:FORUM_NAME}'
       ) a 
     where row_number=1
) b
where rank_value<=100
)
ranking 
on ranking.artificial_key = start.artificial_key

left join main.d_user user_
on user_.user_id = ranking.user_id
left join main.d_location location_
  on location_.location_id = user_.location_id
  left join main.d_country country
       on country.country_id = location_.country_id  ; 


alter table AGGR.users_popularity_current_ranking drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');


INSERT INTO aggr.users_popularity_current_ranking partition (forum_name)
select 
   start.period_dte
  ,ranking.user_id 
  ,ranking.view_count
  ,ranking.view_rank_value
  ,ranking.vote_count
  ,ranking.vote_rank_value
  ,concat(coalesce(user_.first_name,'') , coalesce(user_.last_name,''))
  ,location_.location_descr 
  ,country.country_name
 ,current_timestamp                                   as load_timestamp               
,'${hiveconf:FORUM_NAME}'                            as forum_name
from 


(select  coalesce(view_ranks.user_id, vote_ranks.user_id) as user_id 
        ,coalesce(view_ranks.view_count, 0)               as view_count
        ,coalesce(view_ranks.rank_value,300)              as view_rank_value
        ,coalesce(vote_ranks.vote_count, 0)               as vote_count
        ,coalesce(vote_ranks.rank_value,300)              as vote_rank_value
        ,1 as artificial_key
        
from 
(select * from (select user_id, views as view_count, row_number() over (order by views desc) rank_value 
                 from main.d_user 
                where forum_name = '${hiveconf:FORUM_NAME}'
               ) a
   where rank_value<101 ) view_ranks

full outer join 
(select * from (select user_id, vote_count, row_number() over (order by vote_count desc) rank_value 
                 from (select user_id, up_votes - down_votes as vote_count from main.d_user where forum_name = '${hiveconf:FORUM_NAME}' ) x 
               ) a
   where rank_value<101 ) vote_ranks

 on view_ranks.user_id = vote_ranks.user_id
) ranking    

join (select max(cal_date) as period_dte, 1 as artificial_key from temp.posts_ts_dates) start
  on start.artificial_key = ranking.artificial_key

left join main.d_user user_
  on user_.user_id = ranking.user_id
 and user_.forum_name = '${hiveconf:FORUM_NAME}'
  
left join main.d_location location_
  on location_.location_id = user_.location_id
  
left join main.d_country country
       on country.country_id = location_.country_id ;


alter table AGGR.tags_popularity_current_ranking drop if exists partition (forum_name='${hiveconf:FORUM_NAME}');
    
insert into  aggr.tags_popularity_current_ranking partition (forum_name)
select 
       period_dte               
      ,tag_id                   
      ,tag_name                 
      ,use_count                
      ,rank_value as use_rank_value           
      ,current_timestamp                         
      ,'${hiveconf:FORUM_NAME}'       

from (select max(cal_date) as period_dte, 1 as artificial_key from temp.posts_ts_dates) start

join  (  select tag_id, tag_name, use_count, rank_value, 1 as artificial_key
         from  (select tag_id, tag_name, count as use_count, rank() over (order by count desc) rank_value 
                 from main.d_tag 
                where forum_name = '${hiveconf:FORUM_NAME}') a where rank_value<101
       ) ranking
on ranking.artificial_key = start.artificial_key
;   

