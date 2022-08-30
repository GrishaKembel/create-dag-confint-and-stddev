from airflow import DAG
from operators.impala_operator import ImpalaOperator
from airflow.models.dag import DAG
from datetime import datetime, timedelta
from gpmdata.airflow.notificator.notifications import Notificator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 3, 28),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
}
notificator = Notificator(["my_telegram_nickname"])
dag = DAG(
        default_args=default_args,
        schedule_interval='0 0 * * 0',
        dag_id = 'old_profile_stddev',
        sla_miss_callback=notificator.sla_callback

)

old_profile_stddev_age_stmt =f'''
insert into sendbox.old_profile_stddev_age_20230101
select 
    age,
    cast (stddev(count_mac) as int) as stddev,
    to_date(now()) as update_date
from (
        select 
        case 
            when prediction is null then 'indefined'
            else prediction 
            end as age,
        count(distinct mac) as count_mac, 
        datet
        from (
            select prediction, to_date(to_timestamp(concat(cast(year as string),'/', cast(month as string), '/', cast(day as string)), 'yyyy/M/d')) as datet, mac
            from stg.profile_dns_age
            ) as dns_age
        group by age, datet
    ) as group_age
group by age
'''

old_profile_stddev_gender_stmt =f'''
insert into sendbox.old_profile_stddev_gender_20230101
select 
    gender,
    cast (stddev(count_mac) as int) as stddev,
    to_date(now()) as update_date
from (
    select
    case
        when ismale = 1 then 'male'
        when ismale = 0 then 'female'
        when ismale is null then 'indefined'
        end as gender,
    count(distinct mac) as count_mac,
    datet
    from (
        select 
            ismale,
            to_date(to_timestamp(concat(cast(year as string),'/', cast(month as string), '/', cast(day as string)), 'yyyy/M/d')) as datet,
            mac
        from stg.profile_dns_dmp_gender 
        where year > 2020
        ) as dns_gender 
    group by gender, datet
    ) as group_gender
group by gender
'''

invalidate = f'''
invalidate metadata default.profiles_full
'''

old_profile_stddev_incomlvl_stmt =f'''
insert into sendbox.old_profile_stddev_incomlvl_20230101
select 
    income_lvl,
    cast (stddev(count_mac) as int) as stddev,
    to_date(now()) as update_date
from (
    select
        case
            when income_level is null then 'indefined' else income_level
            end as income_lvl,
        count(distinct mac) as count_mac,
        datet
        from (
            select 
                prof_full.income_level,
                to_date(to_timestamp(concat(cast(dns_age.year as string),'/', cast(dns_age.month as string), '/', cast(dns_age.day as string)), 'yyyy/M/d')) as datet,
                prof_full.mac
            from stg.profile_dns_age dns_age
            JOIN default.profiles_full prof_full on dns_age.mac=regexp_replace(prof_full.mac,':','-')
            where dns_age.year > 2020
                and dns_age.mac != 'empty'
            ) as join_dns_age_profiles_full
        group by income_lvl, datet
    ) as group_income
group by income_lvl
'''

insert_old_profile_stddev_age = ImpalaOperator(
    task_id='insert_old_profile_stddev_age',
    dag=dag,
    sla=timedelta(minutes=60),
    on_failure_callback=notificator.on_failure_callback,
    sql_statement=(old_profile_stddev_age_stmt)
)

insert_old_profile_stddev_gender = ImpalaOperator(
    task_id='insert_old_profile_stddev_gender',
    dag=dag,
    sla=timedelta(minutes=60),
    on_failure_callback=notificator.on_failure_callback,
    sql_statement=(old_profile_stddev_gender_stmt)
)

insert_old_profile_stddev_incomlvl = ImpalaOperator(
    task_id='insert_old_profile_stddev_incomlvl',
    dag=dag,
    sla=timedelta(minutes=60),
    on_failure_callback=notificator.on_failure_callback,
    sql_statement=(invalidate, old_profile_stddev_incomlvl_stmt)
)
insert_old_profile_stddev_age >> insert_old_profile_stddev_gender >> insert_old_profile_stddev_incomlvl