with tmp_file as (
  select t1.file,
      t1.file_ls,
      (pg_stat_file(t1.file)).size as size,
      (pg_stat_file(t1.file)).access as access,
      (pg_stat_file(t1.file)).modification as last_update_time,
      (pg_stat_file(t1.file)).change as change,
      (pg_stat_file(t1.file)).creation as creation,
      (pg_stat_file(t1.file)).isdir as isdir
   from (select dir||'/'||pg_ls_dir(t0.dir) as file,
          pg_ls_dir(t0.dir) as file_ls
       from ( select '/home/postgres/15data/pg_wal'::text as dir
           --需要修改这个物理路径
          ) t0
      ) t1 
   where 1=1
   order by (pg_stat_file(file)).modification desc
) 
select to_char(date_trunc('day',tf0.last_update_time),'yyyymmdd') as day_id,
    sum(case when date_part('hour',tf0.last_update_time) >=0 and date_part('hour',tf0.last_update_time) <24 then 1 else 0 end) as wal_num_all,
    sum(case when date_part('hour',tf0.last_update_time) >=0 and date_part('hour',tf0.last_update_time) <1 then 1 else 0 end) as wal_num_00_01,
    sum(case when date_part('hour',tf0.last_update_time) >=1 and date_part('hour',tf0.last_update_time) <2 then 1 else 0 end) as wal_num_01_02,
    sum(case when date_part('hour',tf0.last_update_time) >=2 and date_part('hour',tf0.last_update_time) <3 then 1 else 0 end) as wal_num_02_03,
    sum(case when date_part('hour',tf0.last_update_time) >=3 and date_part('hour',tf0.last_update_time) <4 then 1 else 0 end) as wal_num_03_04,
    sum(case when date_part('hour',tf0.last_update_time) >=4 and date_part('hour',tf0.last_update_time) <5 then 1 else 0 end) as wal_num_04_05,
    sum(case when date_part('hour',tf0.last_update_time) >=5 and date_part('hour',tf0.last_update_time) <6 then 1 else 0 end) as wal_num_05_06,
    sum(case when date_part('hour',tf0.last_update_time) >=6 and date_part('hour',tf0.last_update_time) <7 then 1 else 0 end) as wal_num_06_07,
    sum(case when date_part('hour',tf0.last_update_time) >=7 and date_part('hour',tf0.last_update_time) <8 then 1 else 0 end) as wal_num_07_08,
    sum(case when date_part('hour',tf0.last_update_time) >=8 and date_part('hour',tf0.last_update_time) <9 then 1 else 0 end) as wal_num_08_09,
    sum(case when date_part('hour',tf0.last_update_time) >=9 and date_part('hour',tf0.last_update_time) <10 then 1 else 0 end) as wal_num_09_10,
    sum(case when date_part('hour',tf0.last_update_time) >=10 and date_part('hour',tf0.last_update_time) <11 then 1 else 0 end) as wal_num_10_11,
    sum(case when date_part('hour',tf0.last_update_time) >=11 and date_part('hour',tf0.last_update_time) <12 then 1 else 0 end) as wal_num_11_12,
    sum(case when date_part('hour',tf0.last_update_time) >=12 and date_part('hour',tf0.last_update_time) <13 then 1 else 0 end) as wal_num_12_13,
    sum(case when date_part('hour',tf0.last_update_time) >=13 and date_part('hour',tf0.last_update_time) <14 then 1 else 0 end) as wal_num_13_14,
    sum(case when date_part('hour',tf0.last_update_time) >=14 and date_part('hour',tf0.last_update_time) <15 then 1 else 0 end) as wal_num_14_15,
    sum(case when date_part('hour',tf0.last_update_time) >=15 and date_part('hour',tf0.last_update_time) <16 then 1 else 0 end) as wal_num_15_16,
    sum(case when date_part('hour',tf0.last_update_time) >=16 and date_part('hour',tf0.last_update_time) <17 then 1 else 0 end) as wal_num_16_17,
    sum(case when date_part('hour',tf0.last_update_time) >=17 and date_part('hour',tf0.last_update_time) <18 then 1 else 0 end) as wal_num_17_18,
    sum(case when date_part('hour',tf0.last_update_time) >=18 and date_part('hour',tf0.last_update_time) <19 then 1 else 0 end) as wal_num_18_19,
    sum(case when date_part('hour',tf0.last_update_time) >=19 and date_part('hour',tf0.last_update_time) <20 then 1 else 0 end) as wal_num_19_20,
    sum(case when date_part('hour',tf0.last_update_time) >=20 and date_part('hour',tf0.last_update_time) <21 then 1 else 0 end) as wal_num_20_21,
    sum(case when date_part('hour',tf0.last_update_time) >=21 and date_part('hour',tf0.last_update_time) <22 then 1 else 0 end) as wal_num_21_22,
    sum(case when date_part('hour',tf0.last_update_time) >=22 and date_part('hour',tf0.last_update_time) <23 then 1 else 0 end) as wal_num_22_23, 
    sum(case when date_part('hour',tf0.last_update_time) >=23 and date_part('hour',tf0.last_update_time) <24 then 1 else 0 end) as wal_num_23_24
from tmp_file tf0
where 1=1
 and tf0.file_ls not in ('archive_status')
group by to_char(date_trunc('day',tf0.last_update_time),'yyyymmdd')
order by to_char(date_trunc('day',tf0.last_update_time),'yyyymmdd') desc;
