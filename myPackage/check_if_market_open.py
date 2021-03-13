import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup


def test_open():
    # get today
    today = datetime.datetime.today()  # .strftime("%Y/%m/%d")
    year = str(today.year-1911)
    month = today.strftime("%m")
    day = today.strftime("%d")
    today_format = f"{year}/{month}/{day}"
    # print(type(today_format), today_format)

    # get the last trade_day
    headers = {'user-agent': 'Googlebot'}
    stock_id = '2330'
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date=20210301&stockNo=2330"
    res = requests.get(url, headers=headers, timeout=8)
    soup = BeautifulSoup(res.text, "lxml")
    table = soup.find_all('table')[0]
    df = pd.read_html(str(table))[0]
    last_day = df.iloc[-1][0]

    return today_format == last_day


if __name__ == '__main__':
    open_status = test_if_open()
    print(open_status)
