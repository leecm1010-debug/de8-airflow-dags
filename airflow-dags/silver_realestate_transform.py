"""
과제 02 - 부동산 실거래가 Silver (PySpark 정제 + UDF)
bronze XML -> PySpark 정제 / IQR 이상치 제거 / UDF 2개 -> silver parquet(snappy)

- ExternalTaskSensor 로 bronze_realestate_collect 완료 대기
- BashOperator + spark-submit 으로 Spark Job 실행
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="silver_realestate_transform",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 6, 30),
    schedule="@monthly",          # bronze 와 같은 스케줄 -> 같은 logical_date 로 매칭
    catchup=True,
    max_active_runs=1,
    tags=["silver", "realestate", "spark", "q2"],
    default_args={"owner": "이찬민"},
) as dag:

    wait_bronze = ExternalTaskSensor(
        task_id="wait_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id=None,        # DAG 전체 완료를 기다림
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    spark_silver = BashOperator(
        task_id="spark_silver",
        bash_command=(
            "spark-submit "
            "--master local[*] "
            "--name silver_realestate_transform "
            "/opt/airflow/scripts/q2/silver_spark.py {{ logical_date.strftime('%Y%m') }}"
        ),
    )

    wait_bronze >> spark_silver
