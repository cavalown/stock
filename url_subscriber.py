import sys
sys.path.append(r'/home/cavalown/stock_project/stock')

from databaseServer import mongoServer as mongo
from databaseServer import redisServer as redis
import crawler
import random
import time

"""
In url public-subscribe system, use db=0 in redis
"""


def subscribe_urls():
    redisConnect = redis.redis_connection('linode1', 'redis', db=0)
    client = mongo.mongo_connection('linode1', 'mongo')
    coll_crawlerURL = mongo.mongo_collection(client, 'stocks', 'crawlerURL')
    while True:
        key = f'ip{random.randint(1, 20)}'
        url = redis.redis_get_value(redisConnect, key)
        if url:
            print(url)
            stock_id = url.split('stockNo=')[-1]
            stock_month = url.split('&date=')[-1].split('&stockNo=')[0][:-2]
            url_id = stock_id + stock_month
            documents = crawler.crawler(url)
            print(documents)
            coll_stock = mongo.mongo_collection(client, 'stocks', f"stock{stock_id}")
            for item in documents:
                mongo.insert_document(coll_stock, item)
            # mongo.insert_many_document(coll_stock, documents)  # 記錄爬取的股票資料，一次單股一個月的資料
            coll_crawlerURL.update_one({'_id': url_id}, {'$set': {'crawlerStatus': 2}})  # 更新crawlerURL的狀態，2表示爬取完成
            redis.redis_delete_key(redisConnect, key)  # 取出url就從redis刪掉
            time.sleep(10)


if __name__ == '__main__':
    subscribe_urls()
