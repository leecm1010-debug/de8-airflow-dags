"""
과제 04 - git-sync 동작 확인용 테스트 DAG (BashOperator 만 사용)
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_gitsync",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gitsync", "q4", "이찬민"],
    default_args={"owner": "이찬민"},
) as dag:

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "hello from git-sync - 이찬민 (데엔 8기)" && date && hostname',
    )
