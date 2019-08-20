   select a.user_id,a.user_name,a.level,a.fans,a.entry_read,b.hot,c.best,d.low,e.operation,f.tag1,g.all_tags,h.cheer11,
h.bookmark11,h.comment11,h.share11,i.type1,j.publish
from
(select user_id,user_name,level,fans,entry_read
from keep_dm_su.dm_su_user_level
where p_date = "2019-07-08" and level !="C")a

left outer join
(select author,count(entry_id) as hot
from keep_ods.ods_su_hot_entry_qualified 
where p_date = "2019-07-17"
group by author)b
on a.user_id = b.author

left outer join
(select user,count(entry)as best
 from keep_ods.ods_su_hotentries
 where p_date = "2019-07-17" and id_to_time >="2018-01-01"
 group by user)c
 on a.user_id = c.user
 
 left outer join
(select author,count(id) as low
     from 
     (select author,id 
        from keep_dw.dwd_su_entries
        where p_date>='2018-01-01' and p_date<='2019-07-17')aaa
     join
     (select entityid,name
        from keep_dw.dwd_entity_tag
        where name in ('令人不适','医疗建议','事实错误','产品负面','敏感','广告','低质')and p_date = "2019-07-17")bbb
        on aaa.id=bbb.entityid
     group by author
     )d 
     on a.user_id=d.author
 
 left outer join
 (select author,count(id) as operation
 from keep_dw.dwd_su_entries
 where p_date <="2019-07-17" and p_date >="2018-01-01"
 and hashtag_type = "operation"
  group by author
  )e
  on e.author = a.user_id
  
  left outer join
  (select author,count(id) as tag1
   from keep_dw.dwd_su_entries
   where p_date >="2019-07-08" and p_date <="2019-07-17"
   group by author
   )f
   on f.author = a.user_id
   
   left outer join
    (select author,count(id) as all_tags
   from keep_dw.dwd_su_entries
   where p_date >="2018-01-01" and p_date <="2019-07-17"
   group by author
   )g
   on g.author = a.user_id
   
left outer join
(select author,
       sum(if(cheer1 is null,0,cheer1))as cheer11,
       sum(if(comment1 is null,0,comment1)) as comment11,
       sum(if(bookmark1 is null,0,bookmark1)) as bookmark11,
       sum(if(share1 is null,0,share1)) as share11
from                                
(select id,author
 from keep_dw.dwd_su_entries
 where p_date <="2019-07-17" and p_date >="2018-01-01" and statevalue != -20)ss
 left outer join
 (select entry_id,sum(cheer)as cheer1,sum(comment)as comment1,sum(share)as share1,sum(bookmark) as bookmark1
  from keep_dm_su.dm_su_entry_show_base
  where p_date >= "2018-01-01" and p_date <="2019-07-17"
  group by entry_id)s2
  on ss.id = s2.entry_id
  group by author)h
  on a.user_id = h.author
  
left outer join
(select user_id,count(type) as type1
 from keep_ods.ods_su_verifieduser
where p_date='2019-07-17'  AND STATE = "pass"
group by user_id )i 
on a.user_id = i.user_id

left outer join
(select count(id)as publish,reported_user
 from keep_ods.ods_su_report_task
 where decision = "punish" and state = 10 and p_date = "2019-07-17"
group by reported_user)j
 on j.reported_user = a.user_id

 
 
 
  

  