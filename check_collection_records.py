import time
import csv
from myPackage import crawler
from myPackage import mongoServer as mon
from myPackage import write_to_csv as wcsv

"""
check amount of history records in every stock collections
"""

def get_all_trage_date():
    client = mon.mongo_connection('linode1', 'mongo')
    coll_sample1101 = mon.mongo_collection(client, 'stocks', 'stock1101')
    contents = list(coll_sample1101.find({}, {'trade_date':1}))
    for item in contents:
        trade_date = item['trade_date']
        print(trade_date)
        wcsv.writeToCsv('trade_dates', [trade_date])
    print(len(contents))
    return
    

def check_records_amount():
    client = mon.mongo_connection('linode1', 'mongo')
    coll_stockInfo = mon.mongo_collection(client, 'stocks', 'stockInfo')
    contents = coll_stockInfo.find({}, {'_id': 1})
    for content in contents:
        stock_id = content['_id']
        coll_stock = mon.mongo_collection(client, 'stocks', f"stock{stock_id}")
        count = coll_stock.count()
        if count != 2710:
            with open('myPackage/trade_dates.csv', newline='') as csvfile:
                rows = csv.reader(csvfile)
                for trade_date in rows:
                    trade_date = ''.join(trade_date)
                    if coll_stock.find({"trade_date":trade_date}).count() == 1:
                        print(stock_id, trade_date, 'OK')
                    else:
                        url = f"""https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={trade_date}&stockNo={stock_id}"""
                        print(url)
                        check_retry = 0
                        while check_retry < 3:
                            try:
                                documents = crawler.crawler(url)
                                for doc in documents:
                                    # 記錄爬取的股票資料並寫入mongo
                                    mon.insert_document(coll_stock, doc)
                                time.sleep(10)
                                break
                            except IndexError as e:  # list out o range: mean the duration does not exist
                                print(e)
                                time.sleep(10)
                                break
                            except Exception as e:
                                print(e)
                                time.sleep(10)
                                check_retry += 1
                                continue
        else:
            print(stock_id, 'check OK!')


if __name__ == '__main__':
    check_records_amount()
