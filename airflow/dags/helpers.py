import datetime
import json
import logging
import sys
from time import time

import pandas as pd
import requests
from sqlalchemy import create_engine

# change None values to NULL


def change_none(data):
    for d in data:
        for key in d:
            if d[key] is None:
                d[key] = 0

    return data


def get_utc_from_unix_time(unix_ts, second=1000):
    return (
        datetime.datetime.utcfromtimestamp(int(unix_ts) / second)
        if unix_ts
        else None
    )


def get_data_callable(url, ti):
    try:
        r = requests.get(url)
        logging.info('getting data')
    except requests.ConnectionError as ce:
        logging.error(f'There was an error with the request, {ce}')
        sys.exit(1)
    data = [r.json().get('data', []), r.json().get('timestamp', [])]
    ti.xcom_push(key='data', value=data)


def transform_callable(ti):
    pulled_data = ti.xcom_pull(key='data', task_ids=['wget'])  # pull xcom
    data = change_none(pulled_data[0][0])
    for d in data:
        d['update_dt'] = get_utc_from_unix_time(pulled_data[0][1])
    data_json = json.dumps(data, default=str)
    ti.xcom_push(key='data_tranformed', value=data_json)


def ingest_callable(user, password, host, port, db, table_name, execution_date, ti):
    print(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')
    t_start = time()
    data = ti.xcom_pull(key='data_tranformed', task_ids=['transform'])[0]
    df_iter = pd.DataFrame(json.loads(data))

    print(len(df_iter))

    df_iter.head(n=0).to_sql(name=table_name, con=engine, if_exists='append', index=False)

    df_iter.to_sql(name=table_name, con=engine, if_exists='append', index=False)

    t_end = time()
    print('inserted the first chunk, took %.3f second' % (t_end - t_start))
