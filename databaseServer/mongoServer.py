# import sys
# sys.path.append(r'/home/cavalown/stock_project/stock')

import pymongo
from pymongo.errors import DuplicateKeyError

from read_file import read_yaml as ryaml

credential_path = '/Users/huangyiling/.credential/.db.yaml'  # mbp
# credential_path = '/home/cavalown/.credential/.db.yaml'  # linode

'''
mongodb://username:password@host:port/dbname
'''


# Make the Mongo connection
def mongo_connection(machine, db_class):
    credential = ryaml.read_yaml(credential_path)
    db_info = credential[machine][db_class]
    host = db_info['host']
    port = db_info['port']
    dbName = db_info['database']
    user = db_info['user']
    password = db_info['pswd']
    client = pymongo.MongoClient('mongodb://{}:{}@{}:{}/{}'.format(user, password, host, port, dbName))
    print('Success connecting to client!')
    return client


# Choose a Mongo collection
def mongo_collection(mongo_client, database, collection):
    db = mongo_client[database]
    collection = db[collection]
    print(f'Database:{database}, Connecting success!')
    return collection


# Create a new mongo database
# In MongoDB, a database is not created until it gets content!
def create_database(mongo_client, database_name):
    database_list = mongo_client.list_database_names()
    print('databases:', database_list)
    if database_name not in database_list:
        new_database = mongo_client[database_name]
        print(f'>> Create new database: {database_name} success!')
        return database_name
    else:
        print('***', database_name, 'already exists!')
        return


# Create a new collection
# In MongoDB, a collection is not created until it gets content!
def create_collection(mongo_client, database, collection_name):
    collection_list = mongo_client[database].list_collection_names()
    print('collections:', collection_list)
    if collection_name not in collection_list:
        print(f'>> Create new collection: {collection_name} success!')
        return mongo_client[database][collection_name]
    else:
        print('***', collection_name, 'already exists in', database)
        return mongo_client[database][collection_name]


# Insert a document to collection
def insert_document(collection, document_dict):
    try:
        collection.insert_one(document_dict)
        print('>> Insert success!')
    except DuplicateKeyError:
        print('This document already exists!')
    return


# Find one from mongo
def find_one_mongo(collection):
    content = collection.find_one()
    return content


# Find all from mongo
def find_all_mongo(collection):
    contents = collection.find()
    return contents


# Find some fields from mongo
def find_some_fields_mongo(collection, columns_list):
    projection = dict()
    for i in columns_list:
        projection[i] = 1
    contents = collection.find({}, projection)
    return contents


if __name__ == '__main__':
    mongo_client = mongo_connection('linode1', 'mongo')
    coll_stockIndustry = mongo_collection(mongo_client, 'stocks', 'stockIndustry')
    contents = find_some_fields_mongo(coll_stockIndustry, ['stocks_list'])
    for i in contents:
        print(i['stocks_list'])
        break
