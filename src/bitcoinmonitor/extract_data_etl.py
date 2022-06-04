import requests
import logging
import sys
import datetime
import psycopg2.extras as p
from bitcoinmonitor.utils.db import WarehouseConnection
from bitcoinmonitor.utils.sde_config import get_warehouse_creds

def get_utc_from_unix_time(unix_ts, second = 1000):
    return (
        datetime.datetime.utcfromtimestamp(int(unix_ts) / second)
        if unix_ts
        else None
    )

def _get_exchange_insert_query():
    return '''
    INSERT INTO bitcoin.price (
        id,
        symbol,
        name,
        rank,
        supply,
        maxSupply,
        marketCapUsd,
        volumeUsd24Hr,
        priceUsd,
        changePercent24Hr,
        vwap24Hr,
        updated_utc
    )
    VALUES (
        %(id)s,
        %(symbol)s,
        %(name)s,
        %(rank)s,
        %(supply)s,
        %(maxSupply)s,
        %(marketCapUsd)s,
        %(volumeUsd24Hr)s,
        %(priceUsd)s,
        %(changePercent24Hr)s,
        %(vwap24Hr)s,
        %(update_dt)s
    );
    '''


#change None values to NULL
def change_none(data):
    for d in data:
        for key in d:
            if d[key] == '':
                d[key] = 0
    return data

def get_extract_data():
    url = 'https://api.coincap.io/v2/assets/'
    try:
        r = requests.get(url)
        logging.info("getting data")
    except requests.ConnectionError as ce:
        logging.error(f"There was an error with the request, {ce}")
        sys.exit(1)
    return [r.json().get('data', []),r.json().get('timestamp', [])]
    # return r.json().get('data', [])



def run():
    [data,t] = get_extract_data()
    # t='1654198131919'
    # data = get_extract_data()
    data = change_none(data)
    # print(data)
    # print(get_extract_data())
    # print(t)
    for d in data:
        d['update_dt'] = get_utc_from_unix_time(t)
    try:
        with WarehouseConnection(get_warehouse_creds()).managed_cursor() as curr:
            p.execute_batch(curr, _get_exchange_insert_query(), data)
    except Exception as e:
        logging.error(f"There was an error getting data to db: {e}")
        sys.exit(1)


if __name__ == '__main__':
    run()
    