from databaseServer import mongoServer as mongo
import requests
from databaseServer import redisServer as redis

"""
In proxy public-subscribe system, use db=1 in redis
"""


def pick_proxy(amount=10):
    client = mongo.mongo_connection('linode1', 'mongo')
    collection = mongo.mongo_collection(client, 'proxy', 'proxyPool_1')
    contents = collection.find({}, {'ip': 1, 'port': 1}).limit(amount)
    redisConnect = redis.redis_connection('linode1', 'redis', db=1)  # proxy use db=1
    for item in contents:
        proxy = item['ip'] + ':' + item['port']
        try:
            validate_proxy(proxy)
            for index in range(1, 21):
                key = f'proxy{index}'
                if not redisConnect.exists(key):
                    redis.redis_set_key_value(redisConnect, key, proxy)
        except Exception:
            pass


def validate_proxy(proxy):
    test_ipv4 = 'https://api.ipify.org?format=json'
    try:
        res_test = requests.get(test_ipv4, proxies={'https': proxy, 'http': proxy}, timeout=5)
        print(f"{proxy} OK")
    except Exception:
        print(f"{proxy} Fail")


if __name__ == '__main__':
    while True:
        pick_proxy()
