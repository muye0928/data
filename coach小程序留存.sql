-- coach小程序复购率全量跑

select current1.schedulenum as current_shedulenum,
       current1.usernum as current_users,
       current1.coachuserid,
       retention.last_cnt as last_users,
       retention.retain_cnt as retain_users,
       retention.retain_rate as retain_rate,
       current1.monthly as monthly
from
  (select count(distinct course.schedule_id) as schedulenum,
        count(distinct user_id) as usernum,
        monthly,
        coachuserid
  from
    (select schedule_id,
            last_day(date) as monthly
    from keep_dw.dwd_kl_schedule_new
         where schedule_status = 2 
    )course
  left outer join
   (select scheduleid,coachuserid
    from keep_ods.ods_kl_schedule_coach_relation
    where p_date = "2019-08-19"
    )relation
  on course.schedule_id = relation.scheduleid
   left outer join
   (select user_id,
           schedule_id
    from keep_dw.dwd_kl_order_new
    where status in (2,3,4,5,6) 
    )users
  on users.schedule_id = course.schedule_id
  group by monthly,coachuserid
  )current1
left outer join
  (select add_months(lastmonth.monthly,1) as monthly,
     count(lastmonth.user_id) as last_cnt,
     lastmonth.coachuserid,
     count(now.user_id) as retain_cnt,
     if (count(lastmonth.user_id) = 0,null,count(now.user_id)/count(lastmonth.user_id) )as retain_rate
     from      
    (select user_id,
            coachuserid,
            monthly
    from
         (select user_id,last_day(date) as monthly,schedule_id
          from keep_dw.dwd_kl_order_new
          where status in(2,3,4,5,6)
          )last
          left outer join
         (select scheduleid,
                 coachuserid
         from keep_ods.ods_kl_schedule_coach_relation
         where p_date = "2019-08-19"
         )relation1
         on last.schedule_id = relation1.scheduleid
         group by user_id,coachuserid,monthly
          )lastmonth
     left outer join
     (select user_id,
             coachuserid,
             monthly
   from
  (select user_id,last_day(date) as monthly,schedule_id
  from keep_dw.dwd_kl_order_new
  where status in(2,3,4,5,6))recent
  left outer join
  (select scheduleid,coachuserid
   from keep_ods.ods_kl_schedule_coach_relation
   where p_date = "2019-08-19")relation1
  on recent.schedule_id = relation1.scheduleid
  group by user_id,coachuserid,monthly
   )now
   on now.user_id = lastmonth.user_id and 
   now.monthly = add_months(lastmonth.monthly,1) 
   and now.coachuserid = lastmonth.coachuserid
   group by lastmonth.monthly,lastmonth.coachuserid
   )retention
on current1.coachuserid = retention.coachuserid and current1.monthly = retention.monthly