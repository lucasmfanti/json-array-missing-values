import json
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def extract_missing_values():

    today = str(datetime.now().date())

    query = ("""
    WITH events AS (

    SELECT
      id
    FROM `database.bronze.event_history`
    WHERE DATE(created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY 1)

    SELECT 
      eh.id AS to_eh_id,
      eh.event_id,
      JSON_EXTRACT_ARRAY(eh.values) AS values
    FROM `database.bronze.event_history` eh
    INNER JOIN events e
      ON eh.id = e.id
    WHERE eh.items != '[]'
    ORDER BY eh.event_id, id
    """)

    conn = BigQuery()
    df_raw = conn.query(query)

    df_schema = df_raw
    df_schema['missing'] = [[]] * df_raw.shape[0]
    df_schema['from_eh_id'] = [[]] * df_raw.shape[0]

    df_values_id = df_schema
    for i in range(len(df_values_id)):
        for j in range(len(df_values_id['values'][i])):
            df_values_id['values'][i][j] = json.loads(df_values_id['values'][i][j])['id']
    
    df_missing_values = df_values_id
    for i in range(len(df_missing_values) - 1):
        if df_missing_values['routing_id'][i] == df_missing_values['routing_id'][i + 1]:
            s = set(df_missing_values['values'][i + 1])
            missing = [x for x in df_missing_values['values'][i] if x not in s]
            df_missing_values['missing'][i + 1] = missing
            df_missing_values['from_rh_id'][i + 1] = df_missing_values['to_rh_id'][i]

    df_final = df_missing_values[~df_missing_values.missing.str.len().eq(0)]
    df_final = df_final.drop('values', axis = 1)
    df_final = df_final.explode('missing')

    df_final.reset_index(inplace = True, drop = True)
    df_final['date_extraction'] = pd.Series([today for x in range(len(df_final.index))])

    filename = "missing_values (%s).csv" % datetime.today().strftime("%Y %m %d")

    df_final.to_csv(filename, index = False)

    s = Storage()
    s.upload_file(filename, 'bucket', 'folder/' + filename)

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 1, 1, 0, 0, 0),
    'concurrency': 1,
    'email': 'youremail@email.com.br',
    'email_on_failure': True,
    'email_on_retry': False
}

with DAG('missing_values_dag',
         default_args=default_args,
         schedule_interval='0 0 * * *',
         catchup=False
         ) as dag:
    opr_run = PythonOperator(task_id='extract_missing_values', python_callable=extract_missing_values)
    