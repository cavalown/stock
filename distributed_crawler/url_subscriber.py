from databaseServer import mongoServer as mongo
from databaseServer import redisServer as redis
import crawler
import sys

"""
In url public-subscribe system, use db=0 in redis
"""


def subscribe_urls(ip_group):
    redisConnect = redis.redis_connection('linode1', 'redis', db=0)
    client = mongo.mongo_connection('linode1', 'mongo')
    coll_crawlerURL = mongo.mongo_collection(client, 'stocks', 'crawlerURL')
    for index in [0, 4, 8]:
        key = f'ip{index + ip_group}'
        url = redisConnect.redis_get_value(key)
        stock_id = url.split('stockNo=')[-1]
        stock_month = url.split('&date=')[-1].split('&stockNo=')[0][:-2]
        url_id = stock_id + stock_month
        documents = crawler.crawler(url)
        coll_stock = mongo.mongo_collection(client, 'stock', f"stock{stock_id}")
        coll_stock.insert_many(documents)  # 記錄爬取的股票資料，一次單股一個月的資料
        redis.redis_delete_key(redisConnect, key)  # 爬完的從redis刪掉
        coll_crawlerURL.update_one({'_id': url_id}, {'$set': {'crawlerStatus': 2}})  # 更新crawlerURL的狀態，2表示爬取完成


if __name__ == '__main__':
    subscribe_urls(1)
