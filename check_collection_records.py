import time

from myPackage import crawler
from myPackage import mongoServer as mon
from myPackage import write_to_csv as wcsv

"""
check amount of history records in every stock collections
"""


def check_records_amount():
    client = mon.mongo_connection('linode1', 'mongo')
    coll_stockInfo = mon.mongo_collection(client, 'stocks', 'stockInfo')
    contents = coll_stockInfo.find({}, {'_id': 1})
    for content in contents:
        stock_id = content['_id']
        coll_stock = mon.mongo_collection(client, 'stocks', f"stock{stock_id}")
        count = coll_stock.count()
        stock_amount = 0
        if count == 2710:
            print(stock_id, ':', count)
            stock_amount += 1
            if stock_amount == 938:
                return
        else:
            for year in range(2010, 2021):
                for month in range(1, 13):
                    retry = 0
                    while retry < 3:
                        try:
                            url = f"""https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={year}{month}01&stockNo={stock_id}"""
                            docs = crawler.crawler(url)
                            for doc in docs:
                                mon.insert_document(coll_stock, doc)
                            time.sleep(10)
                            stock_amount += 1
                            if stock_amount == 938:
                                return
                            break
                        except Exception as e:
                            print(e)
                            time.sleep(10)
                            retry += 1
                            continue


if __name__ == '__main__':
    check_records_amount()
