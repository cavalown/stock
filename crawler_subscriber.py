from re import T
from pymongo.uri_parser import parse_uri
from myPackage import mongoServer as mon
from myPackage import redisServer as red
from myPackage import crawler
import random
import time
import argparse

"""
In url public-subscribe system, use db=0 in redis
"""


def main(amount: int, index: int):  # index:0-4
    redisConnect = red.redis_connection('linode1', 'redis', db=0)
    client = mon.mongo_connection('linode1', 'mongo')
    coll_stockInfo = mon.mongo_collection(client, 'stocks', 'stockInfo_copy')
    while True:
        # get keys and values from mongo
        keys = red.redis_get_all_kv(redisConnect)
        for key in keys:
            num = int(key.split('No_')[-1])
            if num % int(amount) == int(index):
                stock_id = red.redis_get_value(redisConnect, key)
                print(f"get stock {stock_id}")
                red.redis_delete_key(redisConnect, key)  # 取出url就從redis刪掉
                coll_stock = mon.mongo_collection(
                    client, 'stocks', f"stock{stock_id}")
                for year in range(2010, 2021):
                    for month in range(1, 13):
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
                    coll_stockInfo.update_one(
                        {'_id': stock_id}, {'$set': {'yearStatus': year}})  # 當年爬完


parser = argparse.ArgumentParser(description='Subscriber No.')
parser.add_argument('--amount', '-a', help='must, amount of subscribers', required=True)
parser.add_argument('--index', '-i', help='must, appoint the subscriber a number', required=True)
args = parser.parse_args()


if __name__ == '__main__':
    try:
        main(args.amount, args.index)
    except Exception as e:
        print(e)
