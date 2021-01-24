import redis
from read_file import read_yaml as ryaml

credential_path = '/Users/huangyiling/.credential/.db.yaml'  # mbp
# credential_path = '/home/cavalown/.credential/.db.yaml'  # linode

"""
redis has 16 dbs: 0-15
"""


def redis_connection(machine, db_class, db):
    credential = ryaml.read_yaml(credential_path)
    db_info = credential[machine][db_class]
    host = db_info['host']
    port = db_info['port']
    password = db_info['pswd']
    connection = redis.StrictRedis(host=host, port=6379, password=password, db=db, decode_responses=True)
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


if __name__ == '__main__':
    redisConnection = redis_connection('linode1', 'redis', db=0)
    # redis_set_key_value(redisConnection, 'RRR', 'rrr')
    # value = redis_get_value(redisConnection, 'QOO')
    # print(value)
    contents = redis_get_all_kv(redisConnection)
    print(contents)
    # redis_delete_key(redisConnection, 'QOO')
    # contents = redis_get_all_kv(redisConnection)
    # print(contents)
    # ex = redisConnection.exists('RRR')
    # print(ex)
    redisConnection.flushall() # delete all
    contents = redis_get_all_kv(redisConnection)
    print(contents)

