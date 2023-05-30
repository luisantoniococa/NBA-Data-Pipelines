# please disregard this file,
# this is a testing file that I need to modify to work with the current datapipelines sql queries




from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_sql_operator import SparkSQLOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
default_args = {
    'owner': 'Luis Antonio Coca',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10),
    'email': [''],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id='scd_pipeline_query',
          default_args=default_args,
          schedule_interval="@daily")

hours = list(range(1, 24))
products = ['website', 'app']
merge_tasks = {}
wait_for_tasks = {}

for product_name in products:
    product_str = f'product_name={product_name}'
    for hour in hours:
        hour_str = f'hour={str(hour)}'
        ds_str = "ds='{{ ds }}'"
        task_key = f'merge_{product_name}_{hour}'
        partition = '/'.join([ds_str, hour_str , product_str])
        wait_for_tasks[f'wait_for_{product_name}_{hour}'] = HivePartitionSensor(table='event_source', partition=partition)
        insert_spec = f"INSERT OVERWRITE TABLE daily_scd_actors PARTITION ({ds_str}, {hour_str}, {product_str})"
        merge_tasks[task_key] = SparkSQLOperator(
                                         task_id=task_key,
                                         conn_id='spark_local',
                                         query=f"""
                                             {insert_spec}
                                            WITH last_season AS (
                                                SELECT * FROM players
                                                WHERE current_season = 1997

                                            ), this_season AS (
                                                SELECT * FROM player_seasons
                                                WHERE season = 1998
                                            )
                                            INSERT INTO players
                                            SELECT
                                                    COALESCE(ls.player_name, ts.player_name) as player_name,
                                                    COALESCE(ls.height, ts.height) as height,
                                                    COALESCE(ls.college, ts.college) as college,
                                                    COALESCE(ls.country, ts.country) as country,
                                                    COALESCE(ls.draft_year, ts.draft_year) as draft_year,
                                                    COALESCE(ls.draft_round, ts.draft_round) as draft_round,
                                                    COALESCE(ls.draft_number, ts.draft_number)
                                                        as draft_number,
                                                    COALESCE(ls.seasons,
                                                        ARRAY[]::season_stats[]
                                                        ) || CASE WHEN ts.season IS NOT NULL THEN
                                                            ARRAY[ROW(
                                                            ts.season,
                                                            ts.pts,
                                                            ts.ast,
                                                            ts.reb, ts.weight)::season_stats]
                                                            ELSE ARRAY[]::season_stats[] END
                                                        as seasons,
                                                    CASE
                                                        WHEN ts.season IS NOT NULL THEN
                                                            (CASE WHEN ts.pts > 20 THEN 'star'
                                                                WHEN ts.pts > 15 THEN 'good'
                                                                WHEN ts.pts > 10 THEN 'average'
                                                                ELSE 'bad' END)::scorer_class
                                                        ELSE ls.scoring_class
                                                    END as scoring_class,
                                                    ts.season IS NOT NULL as is_active,
                                                    1998 AS current_season

                                                FROM last_season ls
                                                FULL OUTER JOIN this_season ts
                                                ON ls.player_name = ts.player_name

                                         """,
                                         name=task_key,
                                         dag=dag
                                         )
        merge_tasks[task_key].set_upstream([wait_for_tasks[f'wait_for_{product_name}_{hour}']])
