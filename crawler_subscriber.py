from re import T
from typing import Counter
from pymongo.uri_parser import parse_uri
from myPackage import mongoServer as mon
from myPackage import redisServer as red
from myPackage import crawler
import random
import time
from myPackage import write_to_csv as wcsv
import os

"""
In url public-subscribe system, use db=0 in redis
"""


def main():  # index:0-3
    redisConnect = red.redis_connection('linode1', 'redis', db=0)
    client = mon.mongo_connection('linode1', 'mongo')
    coll_stockInfo = mon.mongo_collection(client, 'stocks', 'stockInfo')
    while True:
        # get keys and values from mongo
        keys = red.redis_get_all_kv(redisConnect)
        for key in keys:
            amount = int(os.environ.get("amount")) # amount of subscriber
            index = int(os.environ.get("index")) # subscriber num
            num = int(key.split('No_')[-1]) # redis key
            # 決定subscriber要取用哪筆資料
            if num % int(amount) == int(index):
                stock_id = red.redis_get_value(redisConnect, key)
                print(f"get stock {stock_id}")
                red.redis_delete_key(redisConnect, key)  # 取出url就從redis刪掉
                coll_stock = mon.mongo_collection(client, 'stocks', f"stock{stock_id}")
                for year in range(2010, 2021):
                    for month in range(1, 13):
                        m_retry = 0
                        while m_retry < 3:
                            try:
                                url = f"""https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={str(year)}{str(month).zfill(2)}01&stockNo={stock_id}"""
                                print(f"-- Crawler >>> {url}")
                                documents = crawler.crawler(url)
                                # print(documents)
                                for item in documents:
                                    # 記錄爬取的股票資料並寫入mongo
                                    mon.insert_document(coll_stock, item)
                                coll_stockInfo.update_one(
                                    {'_id': stock_id}, {'$set': {'monthStatus': year+month}})  # 當月爬完
                                time.sleep(10)
                                break
                            except Exception as e:
                                print(e)
                                time.sleep(10)
                                m_retry += 1
                                if m_retry ==3:
                                    wcsv.writeToCsv('CrawlerException', [stock_id, year, month])
                                continue
                    coll_stockInfo.update_one(
                        {'_id': stock_id}, {'$set': {'yearStatus': year}})  # 當年爬完


if __name__ == '__main__':
    try:
        # main(args.amount, args.index)
        main()
    except Exception as e:
        print(e)
