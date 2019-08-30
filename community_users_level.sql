select user_id,
       user_name,
       level,
       fan * 0.3 +(share_count*2+likes_count+comment_count*2+bookmark_count*3+sent_comments+sent_likes+sent_mark)/2943866 * 40 + (hot*50+best*300+year_enteries+operation_entries*5) /43460 * 60 +verify*33*0.1
        -(bad*10+publish *300)/36090 *60 +weekly_data as final,
       fan*0.3,
       (share_count*2+likes_count+comment_count*2+bookmark_count*3+sent_comments+sent_likes+sent_mark)/2943866 * 40 as hudong,
       -(bad*10+publish *300)/36090 *60 as bad_score,
       (hot*50+best*300+year_enteries+operation_entries*5) /43460 * 60 as content_score,
       verify*33*0.1 as verify_score,
       weekly_data,
       fans,
       year_enteries,
       operation_entries,
       share_count,
       likes_count,
       comment_count,
       bookmark_count,
       sent_comments,
       sent_mark,
       sent_likes,
       hot,
       best,
       publish,
       verify,
       bad
       
    
from
(select level.user_id,
       level.user_name,
       level.fans,
       level.fan,
       level.level,
       if (entity.week_active =0,-10,0) AS weekly_data,
       if(entity.year_enteries>3000,3000,entity.year_enteries) as year_enteries,
       if(entity.operation_entries>400,400,entity.operation_entries)as operation_entries,
       entity.share_count,
       entity.likes_count,
       entity.comment_count,
       entity.bookmark_count,
       coalesce(comments.sent_comments,0) as sent_comments,
       coalesce(mark.bookmark,0) as sent_mark,
       case when sent_like.sent_likes is null then 0 
            when sent_like.sent_likes >253509 then 253509 
            else sent_like.sent_likes end as sent_likes,
       COALESCE(hot_entries.hot,0) as hot,
       coalesce(best.best,0) as best,
       coalesce(report.publish,0) as publish,
       coalesce(identity.verify,0) as verify,
       coalesce(bad_label.bad,0) as bad
       from 
(select sum(if (statevalue <>-20 and entry_date >="2019-08-19" and entry_date<="2019-08-25"and unmark = 1,1,0)) as week_active,
        sum(if (statevalue <>-20 and entry_date >="2018-08-26" and entry_Date <="2019-08-25"and unmark = 1 , 1, 0)) as year_enteries,
        sum(if (statevalue <>-20 and entry_date >="2018-08-26" and entry_Date <="2019-08-25"and unmark = 1  and hashtag_type = "operation", 1, 0)) as operation_entries,
        sum(if (statevalue <>-20 and entry_date >="2018-08-26" and entry_date <="2019-08-25"  ,internalsharecount+externalsharecount,0)) as share_count,
        sum(if (statevalue <>-20 and entry_date >="2018-08-26" and entry_date <="2019-08-25"  ,likes,0)) as likes_count,
        sum(if (statevalue <>-20 and entry_date >="2018-08-26" and entry_date <="2019-08-25" ,comments,0)) as comment_count,
        sum(if (statevalue <>-20 and entry_date >="2018-08-26" and entry_date <="2019-08-25" ,favoritecount,0)) as bookmark_count,
       author
from keep_dw.dwd_su_entries
group by author)entity
inner join
(select user_name,user_id,level,
        case when fans <500 then 1
           when fans >=500 and fans <1000 then 10
           when fans >=1000 and fans <5000 then 20
           when fans >= 5000 and fans <10000 then 30
           when fans >=10000 and fans <50000 then 50
           when fans >=50000 and fans <100000 then 60
           when fans >=100000 and fans <250000 then 70
           when fans >=250000 and fans <500000 then 80
           when fans >=500000 and fans <1000000 then 90
           else 100 end as fan,fans
 from keep_dm_su.dm_su_user_level
 where p_date = "2019-08-19")level
 on level.user_id = entity.author
left outer join
(select author,sum(1) as sent_comments
from keep_ods.ods_su_comments
where p_date = '2019-08-28'
and refe_type = 'entry' 
and from_unixtime(int(conv(substr(id,0,8), 16, 10)), 'yyyy-MM-dd HH:mm:ss') >="2018-08-26" 
and from_unixtime(int(conv(substr(id,0,8), 16, 10)), 'yyyy-MM-dd HH:mm:ss') <="2019-08-25" 
group by author)comments
on comments.author = entity.author
left outer join
(select user,sum(1) as bookmark
from keep_ods.ods_su_bookmark
where p_date = "2019-08-28" and type = "entry" and createtime >="2018-08-26" and createtime <="2019-08-25"
group by user)mark
on mark.user = entity.author
left outer join
(select `from` as author, sum(count) as sent_likes
from keep_dw.dwd_su_likerelations 
where p_date = "2019-08-28" and create_time >="2018-08-26" and create_time <="2019-08-25" and type = "Entry"
group by `from`)sent_like
on sent_like.author = entity.author
left outer join
(select author,sum(1) as hot
from keep_ods.ods_su_hot_entry_qualified
where p_Date = "2019-08-28" and state_value >=0 and date >="2018-08-26" and date <="2019-08-25"
group by author)hot_entries
on hot_entries.author = entity.author
left outer join
(select user,sum(1) as best
from keep_ods.ods_su_hotentries
where p_date = "2019-08-28" and id_to_time >="2018-08-26" and id_to_time <="2019-08-25"
group by user)best
on best.user = entity.author
left outer join
(select reported_user, sum(1) as publish
from keep_ods.ods_su_report_task
where p_date = "2019-08-28"  and decision = "punish"
group by reported_user)report
on report.reported_user = entity.author
left outer join
(select user_id,count(id) as verify
from keep_ods.ods_su_verifyusers where p_Date = "2019-08-28" and state = 20 and from_unixtime(int(pass_time/1000), 'yyyy-MM-dd HH:mm:ss') >="2018-08-26" and from_unixtime(int(pass_time/1000),"yyyy-mm-dd hh:mm:ss") <="2019-08-25" and verify_subtype_id <>"5ca312aabcd5d56e7b4e1319"
group by user_id)identity
on identity.user_id = entity.author
left outer join
(select author,count(labelid) as bad
from
(select author,id
from keep_dw.dwd_su_entries where entry_date >="2018-08-26" and entry_date <="2019-08-25")label1
left outer join
(select entityid,labelid
  from keep_dw.dwd_entity_tag
  where labelid in (37,59,322,386,388.389,417))bad
  on bad.entityid = label1.id
  group by author
  )bad_label
on bad_label.author = entity.author)raw
order by final desc










