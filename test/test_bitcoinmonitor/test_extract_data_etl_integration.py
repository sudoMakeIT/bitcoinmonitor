import csv

import psycopg2

from bitcoinmonitor.extract_data_etl import run
from bitcoinmonitor.utils.db import WarehouseConnection
from bitcoinmonitor.utils.sde_config import get_warehouse_creds

from decimal import Decimal

t='1654198131919'

class TestBitcoinMonitor:
    def teardown_method(self, test_extract_data_etl_run):
        with WarehouseConnection(
            get_warehouse_creds()
        ).managed_cursor() as curr:
            curr.execute("TRUNCATE TABLE bitcoin.price;")

    def get_extract_data(self):
        
        with WarehouseConnection(get_warehouse_creds()).managed_cursor(
            cursor_factory=psycopg2.extras.DictCursor
        ) as curr:
            curr.execute(
                '''SELECT id,rank,symbol,name,supply,maxSupply,marketCapUsd,volumeUsd24Hr,priceUsd,changePercent24Hr,vwap24Hr FROM bitcoin.price;'''
            )
            table_data = [dict(r) for r in curr.fetchall()]
        return table_data

    def test_extract_data_etl_run(self, mocker):
        mocker.patch(
            'bitcoinmonitor.extract_data_etl.get_extract_data',
            return_value=[[
                r
                for r in csv.DictReader(
                    open('test/fixtures/sample_raw_bitcoin_data.csv')
                )
            ],t],
        )
        run()
        expected_result = [
            
            {
                "id": "bitcoin",
                "rank": 1,
                "symbol": "BTC",
                "name": "Bitcoin",
                "supply": Decimal(17193925),
                "maxsupply": Decimal(21000000),
                "marketcapusd": Decimal(119150835874.4699281625807300),
                "volumeusd24hr": Decimal(2927959461.1750323310959460),
                "priceusd": Decimal(6929.8217756835584756),
                "changepercent24hr": Decimal(-0.8101417214350334905503814297844655811786651611328125),
                "vwap24hr": Decimal(7175.0663247679233209)
            }
        ]
        result = self.get_extract_data()

        assert expected_result == result
