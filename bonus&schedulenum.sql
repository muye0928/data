# 结算课时

select schedulenum,
       abnormalnum,
       gym_name_all,
       keep_name,
       name,
       occupation_type,
       region,employee_id, 
       schedulenum - abnormalnum as total_num
       case when schedulenum - abnormal_bonus >=84 and occupation_type ="fulltime" then 20
            when schedulenum - abnormal_bonus >=72 and schedulenum - abnormal_bonus <84 and occupation_type ="fulltime" then 12
            when schedulenum  - abnormal_bonus>=40 and occupation_type ="parttime" then 4
            when schedulenum - abnormal_bonus >=24 and schedulenum - abnormal_bonus <= 39 and occupation_type = "parttime" then 2
            else 0 end as schedule_bonus
from
(select count(distinct scheduleid) as schedulenum,
	    sum(case when abnormal_type  in ("late","absent") then 1 else 0 end ) as abnormalnum,
	    sum(case when abnormal_type = "absent"then 1 else 0 end ) as abnormal_bonus,
	    coachuserid,
	    gym_name_all
from
(select scheduleid,coachuserid
from keep_ods.ods_kl_schedule_coach_relation
where p_date = "2019-07-24" and to_date(schedulestarttime) >="2019-06-26" and to_date(schedulestarttime)<="2019-07-25")a
inner join
(select schedule_id,gym_name_all,abnormal_type
 from keep_dw.dwd_kl_schedule_new
 LATERAL view explode(array('all', gym_name)) TB_A as gym_name_all
 where schedule_status = 2
 )b
 on a.scheduleid = b.schedule_id
 group by coachuserid,gym_name_all
 )schedule_info

left outer join

(select keep_name,name,occupation_type,region,employee_id,user_id
 from keep_dw.dwd_kl_coach_info
 where status = "上线"
 )coach
 on schedule_info.coachuserid = coach.user_id
 


# 薪酬奖金

  select count(distinct relation1.scheduleid) as schedulenum,
         name.schedulename,
         coach1.keep_name,
         coach1.name,
         coach1.region,
         coach1.employee_id
  	from
  	(select user_id,keep_name,name,region,employee_id
  	from keep_dw.dwd_kl_coach_info
  	where status = "上线" and occupation_type = "fulltime" )coach1
  	inner join
  	(select scheduleid,coachuserid
  	from keep_ods.ods_kl_schedule_coach_relation
  	where p_date = "2019-07-24" and to_date(schedulestarttime) >="2019-06-26" and to_date(schedulestarttime)<="2019-07-25")relation1
    on coach1.user_id = relation1.coachuserid
    left outer join
    (select schedule_id,base_id_all
     from keep_dw.dwd_kl_schedule_new
     LATERAL  view explode(array('all', base_id)) TB_A as base_id_all
     where schedule_status = 2 and base_id in ("5a5611e27b6e200fcd63eed8","5a7496ef7b6e204cf1813d57","5ba3123163d2c45c56a37c1c","5a9925cb7b6e206b06f2b12f","5ab36f197b6e20044a9d9fe0","5c107cba63d2c453addc12fc","5caac69c7b6e2072278d2213"
     	,"5c9c32437b6e20167cad484c","5c9aedda939a8b6db25a5020","5cf6411d92b28f0bb1f3559d")
     and abnormal_type is null or abnormal_type = "late")schedule1
        on schedule1.schedule_id = relation1.scheduleid
     left outer join
     (select base_id,schedulename
     from keep_app.rpt_kl_schedulename)name
     on name.base_id = schedule1.base_id_all
     group by name.schedulename,coach1.keep_name,coach1.name,coach1.region,coach1.employee_id




