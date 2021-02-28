import os
import time
from typing import Counter

from pymongo.uri_parser import parse_uri

from myPackage import crawler
from myPackage import mongoServer as mon
from myPackage import redisServer as red
from myPackage import write_to_csv as wcsv

"""
In url public-subscribe system, use db=0 in redis
"""


def main():  # redis subscriber index:0-3
    redisConnect = red.redis_connection('linode1', 'redis', db=0)
    client = mon.mongo_connection('linode1', 'mongo')
    coll_stockInfo = mon.mongo_collection(client, 'stocks', 'stockInfo')
    while True:
        # get keys and values from mongo
        keys = red.redis_get_all_kv(redisConnect)
        for key in keys:
            amount = int(os.environ.get("amount"))  # amount of subscriber
            index = int(os.environ.get("index"))  # subscriber num
            num = int(key.split('No_')[-1])  # redis key
            # 決定subscriber要取用哪筆資料
            if num % int(amount) == int(index):
                stock_id = red.redis_get_value(redisConnect, key)
                print(f"get stock {stock_id}")
                red.redis_delete_key(redisConnect, key)  # 取出stock_id就從redis刪掉
                coll_stockInfo.update({'_id': stock_id}, {
                                      '$set': {'crawlerStatus': 2}})  # 表示已經從redis刪掉
                coll_stock = mon.mongo_collection(
                    client, 'stocks', f"stock{stock_id}")
                for year in range(2010, 2022):
                    for month in range(1, 13):
                        if year == 2021 and month > 2:
                            break
                        url = f"""https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={str(year)}{str(month).zfill(2)}01&stockNo={stock_id}"""
                        print(f"-- Crawler >>> {url}")
                        documents = crawler.crawler(url)
                        if documents:
                            for item in documents:
                                # 記錄爬取的股票資料並寫入mongo
                                mon.insert_document(coll_stock, item)
                            coll_stockInfo.update_one(
                                {'_id': stock_id}, {'$set': {'monthStatus': str(year)+str(month).zfill(2)}})  # 當月爬完
                            print(
                                f'stock: {stock_id} in {year}{month} insert done.')
                        time.sleep(10)
                        print(
                            f'stock: {stock_id} in {year}{month} crawl done.')
                    coll_stockInfo.update_one(
                        {'_id': stock_id}, {'$set': {'yearStatus': year}})  # 當年爬完
                coll_stockInfo.update({'_id': stock_id}, {
                                      '$set': {'crawlerStatus': 3}})  # 表示這支股票已經都爬完


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
