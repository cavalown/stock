import re

import redis

from myPackage import read_yaml as ryaml
# import read_yaml as ryaml

credential_path = 'credential/db.yaml'

"""
redis has 16 dbs: 0-15
"""


def redis_connection(machine, db_class, db):
    credential = ryaml.read_yaml(credential_path)
    db_info = credential[machine][db_class]
    host = db_info['host']
    port = db_info['port']
    password = db_info['pswd']
    connection = redis.StrictRedis(
        host=host, port=6379, password=password, db=db, decode_responses=True)
    return connection


def redis_set_key_value(connection, key, value):
    connection.set(key, value)
    print(f'set key:{key}, vale:{value} success.')
    return


def redis_get_value(connection, key):
    value = connection.get(key)
    return value


def redis_delete_key(connection, key):
    connection.delete(key)
    print(f'delete key:{key} success.')
    return


def redis_get_all_kv(connection):
    contents = connection.keys()
    return contents


def flush_all_in_one_db(connection):
    connection.flushall()
    print('Flush done.')
    return redis_get_all_kv(connection)


if __name__ == '__main__':
    redisConnection = redis_connection('linode1', 'redis', db=0)
    # redisConnection.flushall() # delete all
    contents = redis_get_all_kv(connection=redisConnection)
    for i in contents:
        print(i)
