import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

# Redshift 연결 함수
def get_redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

# 데이터 추출
@task
def extract_country_data():
    url = "https://restcountries.com/v3.1/all"
    response = requests.get(url)
    data = response.json()
    
    records = []
    for country in data:
        name = country.get("name", {}).get("official", "")
        population = country.get("population", 0)
        area = country.get("area", 0)
        records.append([name, population, area])
    return records

# Redshift 적재
@task
def load_to_redshift(records):
    cur = get_redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute("DROP TABLE IF EXISTS h2k9971.country_info;")
        cur.execute("""
            CREATE TABLE h2k9971.country_info (
                country VARCHAR,
                population BIGINT,
                area FLOAT
            );
        """)
        for r in records:
            cur.execute(
                f"INSERT INTO h2k9971.country_info VALUES (%s, %s, %s);", r
            )
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)

# DAG 정의
with DAG(
    dag_id="country_info_dag",
    start_date=datetime(2025, 5, 1),
    schedule="30 6 * * 6",  # 매주 토요일 6:30 UTC
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)}
) as dag:
    records = extract_country_data()
    load_to_redshift(records)
