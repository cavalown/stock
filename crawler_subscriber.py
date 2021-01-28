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

parser = argparse.ArgumentParser(description='Subscriber No.')
parser.add_argument('--subno', '-sn', help='must, appoint the subscriber a number', required=True)
args = parser.parse_args()

def main(num):
    redisConnect = red.redis_connection('linode1', 'redis', db=0)
    client = mon.mongo_connection('linode1', 'mongo')
    coll_stockInfo = mon.mongo_collection(client, 'stocks', 'stockInfo')
    key = ''
    # while True:
    #     key = f'ip{random.randint(1, 20)}'
    #     url = redis.redis_get_value(redisConnect, key)
    #     if url:
    #         print(url)
    #         stock_id = url.split('stockNo=')[-1]
    #         stock_month = url.split('&date=')[-1].split('&stockNo=')[0][:-2]
    #         url_id = stock_id + stock_month
    #         documents = crawler.crawler(url)
    #         print(documents)
    #         coll_stock = mongo.mongo_collection(client, 'stocks', f"stock{stock_id}")
    #         for item in documents:
    #             mongo.insert_document(coll_stock, item)
    #         # mongo.insert_many_document(coll_stock, documents)  # 記錄爬取的股票資料，一次單股一個月的資料
    #         coll_crawlerURL.update_one({'_id': url_id}, {'$set': {'crawlerStatus': 2}})  # 更新crawlerURL的狀態，2表示爬取完成
    #         redis.redis_delete_key(redisConnect, key)  # 取出url就從redis刪掉
    #         time.sleep(10)


if __name__ == '__main__':
    main()
