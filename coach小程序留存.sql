-- coach小程序复购率全量跑

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table keep_app.rpt_kl_tmp1 partition (month_last_day)

  select current1.schedulenum as class_count,
       current1.usernum as users_count,
       current1.coachuserid,
       retention.retain_cnt as retention_count,
       retention.retain_rate as retention_rate,
       current1.monthly as month_last_day
from
  (select count(distinct course.schedule_id) as schedulenum,
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
  group by monthly,coachuserid
  )current1
  left outer join
  (select count(distinct user_id)as usernum,coach_user_id,last_day(date) as monthly
    from keep_dw.dwd_kl_order_new
    where status in (2,3,4,5,6)
    group by last_day(date),coach_user_id 
    )users 
  on users.coach_user_id = current1.coachuserid and users.monthly = current1.monthly
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
