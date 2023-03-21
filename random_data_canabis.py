from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

CONABIS_END_POINT = 'https://random-data-api.com/api/cannabis/random_cannabis'

def extract_canabis_data(conn_id: str, **context) -> None:
    """Извлекает и сохраняет данные"""
    import requests

    pg_hook = PostgresHook(conn_id)
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    sql_insert = (
        f"""INSERT INTO canabis (id, uid, strain, cannabinoid_abbreviation, cannabinoid,
                                 terpene, medical_use, health_benefit, category, type,
                                 buzzword, brand)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id)
                DO NOTHING
        """
    )
    respospons = requests.get(url=CONABIS_END_POINT, params={'size': 10}).json()

    for row in respospons:
        cur.execute(sql_insert, [value  for value in row.values()])
    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
        default_args=default_args,
        dag_id='random_data_canabis',
        start_date=datetime(2023, 3, 21),
        schedule_interval='0 */12 * * *',
        catchup=False
) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_canabis_data,
        op_kwargs={'conn_id': 'Postgres'}
    )

    extract_data
