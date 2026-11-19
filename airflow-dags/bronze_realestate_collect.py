"""
과제 01 - 부동산 실거래가 Bronze (API 수집)
국토교통부 아파트 매매 실거래가 API -> 원본 XML 그대로 S3 bronze 저장

- 6개 시군구를 TaskGroup 으로 병렬 수집
- BranchPythonOperator 로 응답 0건 / XML 파싱 실패 분기
- S3 경로: s3://realestate-leechanmin/bronze/{yyyymm}/{LAWD_CD}.xml
- 키는 docker-compose.yml 의 환경변수(DATA_GO_KR_API_KEY, AWS_*)에서 읽음 (제출본은 빈 문자열)
"""
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import unquote

import boto3
import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup

SERVICE_KEY = os.environ.get("DATA_GO_KR_API_KEY", "")   # data.go.kr API 키 (제출 시 빈 문자열)
BUCKET = os.environ.get("S3_BUCKET", "realestate-leechanmin")
API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"

# 시군구코드 5자리 (강남구, 서초구, 송파구, 마포구, 용산구, 성동구)
LAWD_CODES = ["11680", "11650", "11710", "11440", "11170", "11200"]


def collect(lawd_cd, **context):
    print(f"collector=이찬민, time={datetime.now()}, lawd={lawd_cd}")

    deal_ymd = context["logical_date"].strftime("%Y%m")   # @monthly -> 해당 월
    params = {
        # 인코딩 키를 넣어도 동작하도록 한 번 디코딩
        "serviceKey": unquote(SERVICE_KEY),
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "pageNo": 1,
        "numOfRows": 1000,
    }
    response = requests.get(API_URL, params=params, timeout=30)
    print(f"HTTP status = {response.status_code}")
    xml_text = response.text

    # XML 파싱 검사
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print(f"[parse_fail] {lawd_cd} {deal_ymd} : {e}")
        print(xml_text[:300])
        return "parse_fail"

    result_code = root.findtext(".//resultCode")
    total_count = root.findtext(".//totalCount")
    item_count = len(root.findall(".//item"))
    print(f"resultCode={result_code}, totalCount={total_count}, items={item_count}")

    if result_code not in ("00", "000"):
        print(f"[parse_fail] API 오류 응답: {xml_text[:300]}")
        return "parse_fail"
    if item_count == 0:
        print(f"[empty] {lawd_cd} {deal_ymd} 거래 0건")
        return "empty"

    # 원본 XML 그대로 S3 bronze 에 저장
    key = f"bronze/{deal_ymd}/{lawd_cd}.xml"
    s3 = boto3.client("s3", region_name="ap-northeast-2")
    s3.put_object(Bucket=BUCKET, Key=key, Body=xml_text.encode("utf-8"),
                  ContentType="application/xml")
    print(f"S3 저장 완료: s3://{BUCKET}/{key} ({item_count}건)")
    return "ok"


def choose_branch(**context):
    ti = context["ti"]
    task_ids = [f"collect_group.collect_{code}" for code in LAWD_CODES]
    results = ti.xcom_pull(task_ids=task_ids)
    print(dict(zip(LAWD_CODES, results)))

    if all(r == "ok" for r in results):
        return "summary_done"
    return "skip_upload"


def report_problem(**context):
    ti = context["ti"]
    task_ids = [f"collect_group.collect_{code}" for code in LAWD_CODES]
    results = ti.xcom_pull(task_ids=task_ids)
    for code, r in zip(LAWD_CODES, results):
        if r != "ok":
            print(f"[{r}] {code} -> S3 업로드 건너뜀")


with DAG(
    dag_id="bronze_realestate_collect",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 6, 30),
    schedule="@monthly",
    catchup=True,
    max_active_runs=2,
    tags=["bronze", "realestate", "q1"],
    default_args={"owner": "이찬민", "retries": 1},
) as dag:

    with TaskGroup("collect_group") as collect_group:
        for code in LAWD_CODES:
            PythonOperator(
                task_id=f"collect_{code}",
                python_callable=collect,
                op_kwargs={"lawd_cd": code},
            )

    branch_after_collect = BranchPythonOperator(
        task_id="branch_after_collect",
        python_callable=choose_branch,
    )

    skip_upload = PythonOperator(
        task_id="skip_upload",
        python_callable=report_problem,
    )

    summary_done = EmptyOperator(task_id="summary_done")

    collect_group >> branch_after_collect >> [skip_upload, summary_done]
