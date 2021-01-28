from databaseServer import mongoServer as mongo


def count_stocks():
    stock_count = 0
    client = mongo.mongo_connection('linode1', 'mongo')
    collection = mongo.mongo_collection(client, 'stocks', 'stockIndustry')
    contents = collection.find({}, {'stocks_count': 1})
    for item in contents:
        id_list_count = item['stocks_count']
        stock_count += id_list_count
    print('stocks_count :', stock_count)


def count_url_check():
    expected_count = 938 * 11 * 12
    collection = mongo.mongo_collection(mongo.mongo_connection('linode1', 'mongo'), 'stocks', 'crawlerURL')
    # reality_count = collection.find({}).count()
    reality_count = collection.count_documents
    print("Expected :", expected_count)
    print("Reality :", reality_count)


if __name__ == '__main__':
    count_stocks()
    # stocks_count : 938
    count_url_check()
    # Expected: 123816
    # Reality: 123816
