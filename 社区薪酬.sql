#社区薪酬（作者向）
add jar hdfs://dins1:8020/common/jar/hive-udf-ParseChinese.jar;
create temporary function parse_chinese as 'com.keep.udf.ParseChinese';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ***


select user_name,user_id,id,type,
       revenue
from
(
select user_name,user_id,id,type,revenue,row_number()over(partition by user_name order by revenue desc) as rank
from
(select user_id,user_name
from keep_ods.ods_su_verifyusers
where p_date = "2019-08-25" and verify_subtype_id ="5d1f0205bcd5d509eb383534" and state = 20)author
left outer join
(select author,
        id,
        if (array_contains(contenttype,'video'),"video","text") as type,
        case when favoritecount >=40 and favoritecount <80 then 180
             when favoritecount >=80 and favoritecount <120 then 260
             when favoritecount >=120 and favoritecount <160 then 280
             when favoritecount >=160 and favoritecount <200 then 340
             when favoritecount >= 200 and favoritecount <400 then 400
             when favoritecount >=400 then 500
             else 100 end as revenue
      from keep_dw.dwd_su_entries
      where entry_date >="2019-07-26" and entry_date <="2019-08-25" 
      and statevalue <>-20 
      and (videolength >=60 or (parse_chinese(content) >=400 and array_contains(contenttype,'photo'))))entity
on entity.author = author.user_id
inner join 
(select entityid
 from keep_dw.dwd_entity_tag
 where p_date = "2019-08-25" and labelid not in (37,322,386,387,388,389)
group by entityid)label
on label.entityid = entity.id)raw
where rank <=30 


#社区薪酬（平台向）
add jar hdfs://dins1:8020/common/jar/hive-udf-ParseChinese.jar;
create temporary function parse_chinese as 'com.keep.udf.ParseChinese';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ***


select raw1.user_name,
       raw1.user_id,
       count(case when  type = "video" then id end) as video_cnt,
       count(case when  type = "text"then id end) as text_cnt,
       count(case when  type = "video" and revenue = 180 then id end) as video_1,
       count(case when  type ="video" and revenue = 260 then id end) as video_2,
       count(case when  type = "video" and revenue = 280 then id end) as video_3,
       count(case when  type ="video" and revenue = 340 then id end) as video_4,
       count(case when  type = "video" and revenue = 400 then id end) as video_5,
       count(case when  type = "text" and revenue = 500 then id end) as video_6,
       count(case when  type = "text" and revenue = 180 then id end ) as text_1,
       count(case when  type = "text" and revenue = 260 then id end ) as text_2,
       count(case when  type = "text" and revenue = 280 then id end ) as text_3,
       count(case when  type = "text" and revenue = 340 then id end ) as text_4,
       count(case when  type = "text" and revenue = 400 then id end) as text_5,
       count(case when  type = "text" and revenue = 500 then id end ) as text_6,
       sum(revenue) as total_revenue
       from
(select id,user_name,user_id,revenue,type
from
(
select user_name,user_id,id,type,revenue,row_number()over(partition by user_name order by revenue desc) as rank
from
(select user_id,user_name
from keep_ods.ods_su_verifyusers
where p_date = "2019-08-25" and verify_subtype_id ="5d1f0205bcd5d509eb383534" and state = 20)author
left outer join
(select author,
        id,
        if (array_contains(contenttype,'video'),"video","text") as type,
        case when favoritecount >=40 and favoritecount <80 then 180
             when favoritecount >=80 and favoritecount <120 then 260
             when favoritecount >=120 and favoritecount <160 then 280
             when favoritecount >=160 and favoritecount <200 then 340
             when favoritecount >= 200 and favoritecount <400 then 400
             when favoritecount >=400 then 500
             else 100 end as revenue
      from keep_dw.dwd_su_entries
      where entry_date >="2019-07-26" and entry_date <="2019-08-25" 
      and statevalue <>-20 
      and (videolength >=60 or (parse_chinese(content) >=400 and array_contains(contenttype,"photo"))))entity
on entity.author = author.user_id
inner join 
(select entityid
 from keep_dw.dwd_entity_tag
 where p_date = "2019-08-25" and labelid not in (37,322,386,387,388,389)
group by entityid)label
on label.entityid = entity.id)raw
where rank <=30)raw1
group by raw1.user_name,raw1.user_id



