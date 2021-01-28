from myPackage import redisServer as red


connect = red.redis_connection('linode1', 'mongo', 0)
kvs = red.flush_all_in_one_db(connect)
print(len(kvs))