from myPackage import redisServer as red
import os


def flush_redis():
    connect = red.redis_connection('linode1', 'mongo', int(os.environ.get("db")))
    kvs = red.flush_all_in_one_db(connect)
    print(len(kvs))
    return


if __name__ == '__main__':
    flush_redis()
