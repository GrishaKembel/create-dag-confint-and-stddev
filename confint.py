from airflow.models.dag import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from datetime import datetime, timedelta
from gpmdata.airflow.notificator.notifications import Notificator
import pendulum

dag_name = 'new_profile_stddev'


local_tz = pendulum.timezone('Europe/Moscow')

default_args = {'owner': 'airflow'
        , 'start_date': datetime(2022, 6, 5, 0, 10, tzinfo=local_tz)
        , 'depends_on_past': True
        , 'retries': 3
        , 'retry_delay': timedelta(seconds=30)}

dag = DAG(dag_name,
         schedule_interval='0 0 * * 0',
         default_args=default_args,
         on_failure_callback=notificator.on_failure_callback,
         sla_miss_callback=notificator.sla_callback
        )

new_profile_stddev_age_stmt='''
insert into sandbox.new_profile_stddev_age_20230101
select
    age,
    cast (stddevSampStable(count_mac) as int) as stddev,
    today() as update_date
from (
    select
    case
        when age = 0 then 'undefined'
        else toString(age)
        end as age,
    uniq(mac_addr) as count_mac
    from (
            select toUInt64OrZero(MSISDN) as msisdn, mac_addr
            from ref.v_mac_phones_versionned
            ) msisdn
        inner join (
             select age, msisdn, max(partition_date) as datet
             from profile.profile_history
             where age in (17, 24, 34, 44, 99)
             group by age, msisdn
             ) profile
        using msisdn
        group by age, datet
        ) as group_age
group by age
'''

new_profile_stddev_gender_stmt='''
insert into sandbox.new_profile_stddev_gender_20230101
select
    gender,
    cast (stddevSampStable(count_mac) as int) as stddev,
    today() as update_date
from (
    select
    gender,
    uniq(mac_addr) as count_mac
    from (
            select toUInt64OrZero(MSISDN) as msisdn, mac_addr
            from ref.v_mac_phones_versionned
            ) msisdn
        inner join (
             select gender, msisdn, max(partition_date) as datet
             from profile.profile_history
             where gender not in ('UNDEFINED')
             group by gender, msisdn
             ) profile
        using msisdn
    group by gender, datet
    ) as group_gender
group by gender
'''

new_profile_stddev_income_stmt='''
insert into sandbox.new_profile_stddev_income_20230101
select
    income,
    cast (stddevSampStable(count_mac) as int) as stddev,
    today() as update_date
from (
    select
    income,
    uniq(mac_addr) as count_mac
    from (
            select toUInt64OrZero(MSISDN) as msisdn, mac_addr
            from ref.v_mac_phones_versionned
            ) msisdn
        inner join (
             select income, msisdn, max(partition_date) as datet
             from profile.profile_history
             where income not in ('UNDEFINED')
             group by income, msisdn
             ) profile
        using msisdn
    group by income, datet
    ) as group_income
group by income
'''

insert_new_profile_stddev_age = ClickHouseOperator(
        dag=dag ,
        sla=timedelta(minutes=60),
        task_id='insert_new_profile_stddev_age',
        database='sandbox',
        sql=(new_profile_stddev_age_stmt),
        clickhouse_conn_id='clickhouse_prod',
        pool='clickhouse_pool')

insert_new_profile_stddev_gender = ClickHouseOperator(
        dag=dag ,
        sla=timedelta(minutes=60),
        task_id='insert_new_profile_stddev_gender',
        database='sandbox',
        sql=(new_profile_stddev_gender_stmt),
        clickhouse_conn_id='clickhouse_prod',
        pool='clickhouse_pool')

insert_new_profile_stddev_income = ClickHouseOperator(
        dag=dag ,
        sla=timedelta(minutes=60),
        task_id='insert_new_profile_stddev_income',
        database='sandbox',
        sql=(new_profile_stddev_income_stmt),
        clickhouse_conn_id='clickhouse_prod',
        pool='clickhouse_pool')

insert_new_profile_stddev_age >> insert_new_profile_stddev_gender >> insert_new_profile_stddev_income