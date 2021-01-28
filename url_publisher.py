# import sys
# sys.path.append(r'/home/cavalown/stock_project/stock')

from databaseServer import mongoServer as mongo
from databaseServer import redisServer as redis

"""
In url public-subscribe system, use db=0 in redis
"""


def publish_urls():
    client = mongo.mongo_connection('linode1', 'mongo')
    collection = mongo.mongo_collection(client, 'stocks', 'crawlerURL')
    redisConnect = redis.redis_connection('linode1', 'redis', db=0)
    for index in range(1, 21):
        key = f'ip{index}'
        if not redisConnect.exists(key):
            content = collection.find_one({'crawlerStatus': 0}, {'url': 1})
            url = content['url']
            redis.redis_set_key_value(redisConnect, key, url)
            collection.update_one({'_id': content['_id']},
                                  {"$set": {'crawlerStatus': 1}})  # crawlerStatus=1 表示已經set到redis
    return redis.redis_get_all_kv(redisConnect)
    # client = mongo.mongo_connection('linode1', 'mongo')
    # collection = mongo.mongo_collection(client, 'stocks', 'crawlerURL')
    # contents = collection.find({'crawlerStatus': 0}, {'url': 1}).limit(20)  # crawlerStatus=0 表示完全沒動過
    # redisConnect = redis.redis_connection('linode1', 'redis', db=0)
    # num = 1
    # for item in contents:
    #     url = item['url']
    #     key = f'ip{num}'
    #     if not redisConnect.exists(key):
    #         redis.redis_set_key_value(redisConnect, key, url)
    #         collection.update_one({'_id': item['_id']}, {"$set": {'crawlerStatus': 1}})  # crawlerStatus=1 表示已經set到redis
    #     num += 1
    # return redis.redis_get_all_kv(redisConnect)


if __name__ == '__main__':
    while True:
        publish_urls()
