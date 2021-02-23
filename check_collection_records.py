import csv
from myPackage import crawler
from myPackage import mongoServer as mon
from myPackage import write_to_csv as wcsv
from myPackage import all_stock_id as allStockID

"""
check 202012 data of each stock collections is exist or not
"""


def check_records_exist():
    client = mon.mongo_connection('linode1', 'mongo')
    for content in allStockID.all_stock_id():
        stock_id = content['_id']
        # print(stock_id)
        coll_stock = mon.mongo_collection(client, 'stocks', f"stock{stock_id}")
        # stocks_con = list(coll_stock.find(
        #     {"trade_date": {"$regex": "202012"}}, {"trade_date": 1}))
        records_count = stocks_con = coll_stock.find(
            {"trade_date": {"$regex": "202012"}}, {"trade_date": 1}).count()
        if records_count < 5:
            print(stock_id, records_count)
            wcsv.writeToCsv("double_check_stock", [stock_id])


if __name__ == '__main__':
    check_records_exist()
