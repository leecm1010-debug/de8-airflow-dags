"""
과제 03 - 부동산 실거래가 Gold (집계 + PostgreSQL 적재)
silver parquet -> PySpark SQL 5종 집계 -> PostgreSQL 5개 테이블 (write.jdbc)

- ExternalTaskSensor 로 silver_realestate_transform 완료 대기
- BashOperator + spark-submit 으로 Spark Job 실행
- 적재 후 PostgresHook 으로 각 테이블 row count > 0 검증
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

GOLD_TABLES = [
    "gold_realestate_district_avg",
    "gold_realestate_top10",
    "gold_realestate_size_dist",
    "gold_realestate_age_avg",
    "gold_realestate_mom_change",
]


def check_row_count():
    hook = PostgresHook(postgres_conn_id="realestate_dw")
    for table in GOLD_TABLES:
        count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        print(f"{table}: {count} rows")
        if count == 0:
            raise ValueError(f"{table} 에 데이터가 없습니다.")
    print("=== 5개 gold 테이블 row count 검증 완료 ===")


with DAG(
    dag_id="gold_realestate_aggregate",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 6, 30),
    schedule="@monthly",          # silver 와 같은 스케줄
    catchup=True,
    max_active_runs=1,
    tags=["gold", "realestate", "spark", "postgres", "이찬민"],
    default_args={"owner": "이찬민"},
) as dag:

    wait_silver = ExternalTaskSensor(
        task_id="wait_silver",
        external_dag_id="silver_realestate_transform",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60 * 2,
    )

    spark_gold = BashOperator(
        task_id="spark_gold",
        bash_command=(
            "spark-submit "
            "--master local[*] "
            "--name gold_realestate_aggregate "
            "/opt/airflow/scripts/q3/gold_spark_sql.py"
        ),
    )

    check_tables = PythonOperator(
        task_id="check_tables",
        python_callable=check_row_count,
    )

    wait_silver >> spark_gold >> check_tables
