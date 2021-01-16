import sys
sys.path.append(r'/home/cavalown/stock_project')

import pymongo
from read_file import read_yaml as ryaml

yaml_file_path = '/home/cavalown/.credential/.db.yaml'

"""
steps:

"""

'''
mongodb://username:password@host:port/dbname
'''


# Make the Mongo connection
def mongo_connection(machine, db_class):
    db_yaml = ryaml.read_yaml(yaml_file_path)
    db_info = db_yaml[machine][db_class]
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
        return


# Insert a document to collection
def insert_document(collection, document_dict):
    insert_obj = collection.insert_one(document_dict)
    print('>> Insert success!')
    return insert_obj.inserted_id


if __name__ == '__main__':
    mongo_client = mongo_connection('linode1', 'mongo')
    # coll_stockIndustry = mongo_collection(mongo_client, 'stocks', 'stockIndustry')
    # create_database(mongo_client, 'stocks')
    # create_collection(mongo_client, 'stocks', 'stock2330')
    # doc = {'product_name': 'milk', 'price': 35}
    # db = create_database(mongo_client, 'test_db')
    # test_coll = create_collection(mongo_client, db, 'test_collection')
    # insert_document(test_coll, doc)
