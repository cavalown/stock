# import sys
# sys.path.append(r'/home/cavalown/stock_project/stock')

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from databaseServer import mongoServer as mongo

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'}


def get_stock_ids():
    mongo_client = mongo.mongo_connection('linode1', 'mongo')
    coll_stockIndustry = mongo.mongo_collection(mongo_client, 'stocks', 'stockIndustry')
    contents = mongo.find_some_fields_mongo(coll_stockIndustry, ['stocks_list'])
    return contents


def stock_crawler(stock_ids_list):
    for stocks in stock_ids_list:
        stock_ids = stocks['stocks_list']
        client = mongo.mongo_connection('linode1', 'mongo')
        for stock_id in stock_ids:
            collection = mongo.create_collection(client, 'stocks', f'stock{stock_id}')
            for year in range(10, 21):
                for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
                    stock_url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date=20{str(year)}{month}01&stockNo={stock_id}"
                    res = requests.get(stock_url, headers=headers)
                    soup = BeautifulSoup(res.text, "lxml")
                    table = soup.find_all('table')[0]
                    df = pd.read_html(str(table))[0]
                    for index in range(len(df)):
                        date = df.iat[index, 0]  # 交易日
                        date_ad = str(1911 + int(date.split('/')[0])) + ''.join(date.split('/')[1:])
                        volume = int(df.iat[index, 1])  # 交易量(股數)
                        price = float(df.iat[index, 2])  # 成交金額
                        open_ = float(df.iat[index, 3])  # 開盤價
                        high = float(df.iat[index, 4])  # 最高價
                        low = float(df.iat[index, 5])  # 最低價
                        close_ = float(df.iat[index, 6])  # 收盤價
                        change_ori = df.iat[index, 7]  # 高低價差
                        if change_ori == 'X0.00':
                            change = float(0.00)
                        else:
                            change = float(change_ori)
                        trades = int(df.iat[index, 8])  # 成交筆數
                        doc = {'_id': stock_id + date_ad,
                               'trade_date': date_ad,
                               'volume': volume,
                               'price': price,
                               'open': open_,
                               'high': high,
                               'low': low,
                               'close': close_,
                               'change': change,
                               'trades': trades}
                        print(doc)
                        mongo.insert_document(collection, doc)
                    # df.to_csv(f'/Users/huangyiling/Desktop/stock/2330/stock{stock_id}_20{str(i)}{j}.csv')
                    time.sleep(20)
                time.sleep(60)
            time.sleep(300)


if __name__ == '__main__':
    stock_id_list = get_stock_ids()
    stock_crawler(stock_id_list)
