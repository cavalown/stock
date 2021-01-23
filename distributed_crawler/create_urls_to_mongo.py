from databaseServer import mongoServer as mongo

year_list = [str(i) for i in range(2010, 2021)]
month_list = [str(j) for j in range(1, 13)]


def get_stock_ids():
    client = mongo.mongo_connection('linode1', 'mongo')
    collection = mongo.mongo_collection(client, 'stocks', 'stockIndustry')
    contents = collection.find({}, {'stocks_list': 1})
    return contents


def create_urls(stock_ids):
    mongoClient = mongo.mongo_connection('linode1', 'mongo')
    mongoCollection = mongo.mongo_collection(mongoClient, 'stocks', 'crawlerURL')
    for stock_id in stock_ids:
        for year in year_list:
            for month in month_list:
                url = f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={year + month.zfill(2)}04&stockNo={stock_id}'
                doc = {'_id': stock_id + year + month.zfill(2), 'url': url, 'crawlerStatus': 0}
                mongo.insert_document(mongoCollection, doc)


if __name__ == '__main__':
    contents = get_stock_ids()
    for item in contents:
        ids = item['stocks_list']
        create_urls(ids)
