import requests
from bs4 import BeautifulSoup
import pandas as pd
from databaseServer import redisServer as redis
import random
import time

headers = {
    'user-agent': 'Googlebot'}


def get_proxies():
    connect = redis.redis_connection('linode1', 'redis', db=1)
    key = f'proxy{random.randint(1, 20)}'
    content = redis.redis_get_value(connect, key)
    redis.redis_delete_key(connect, key)
    return content


def crawler(url):
    # picked_proxy = get_proxies()
    # print(picked_proxy)
    # proxies = {'http': 'http://'+picked_proxy,
    #            'https': 'https://'+picked_proxy}
    try:
        stock_id = url.split('stockNo=')[-1]
        res = requests.get(url, headers=headers, timeout=8)#, proxiex=proxies)
        soup = BeautifulSoup(res.text, "lxml")
        table = soup.find_all('table')[0]
        df = pd.read_html(str(table))[0]
        documents = []
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
            change = float(0.00) if change_ori == 'X0.00' else float(change_ori)
            trades = int(df.iat[index, 8])  # 成交筆數
            document = {'_id': stock_id + date_ad,
                        'trade_date': date_ad,
                        'volume': volume,
                        'price': price,
                        'open': open_,
                        'high': high,
                        'low': low,
                        'close': close_,
                        'change': change,
                        'trades': trades}
            documents.append(document)
        return documents
    except Exception:
        time.sleep(10)
        crawler(url)


if __name__ == '__main__':
    crawler('https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date=20170401&stockNo=1101')
