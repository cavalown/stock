from databaseServer import mongoServer as mongo
from databaseServer import redisServer as redis

"""
In url public-subscribe system, use db=0 in redis
"""


def publish_urls():
    client = mongo.mongo_connection('linode1', 'mongo')
    collection = mongo.mongo_collection(client, 'stocks', 'crawlerURL')
    contents = collection.find({'crawlerStatus': 0}, {'url': 1}).limit(12)  # crawlerStatus=0 表示完全沒動過
    redisConnect = redis.redis_connection('linode1', 'redis', db=0)
    num = 1
    for item in contents:
        url = item['url']
        key = f'ip{num}'
        if not redisConnect.exists(key):
            redis.redis_set_key_value(redisConnect, key, url)
            collection.update_one({'_id': item['_id']}, {"$set": {'crawlerStatus': 1}})  # crawlerStatus=1 表示已經set到redis
        num += 1
    return redis.redis_get_all_kv(redisConnect)


if __name__ == '__main__':
    while True:
        redis_data = publish_urls()
        # print(redis_data)
