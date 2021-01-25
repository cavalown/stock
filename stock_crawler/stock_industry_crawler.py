import sys

sys.path.append(r'/home/cavalown/stock_project/stock')

import requests
from bs4 import BeautifulSoup
from databaseServer import mongoServer as mongo
import time

industry_kv = {'水泥': 'cement', '食品': 'food', '塑膠': 'plastic', '紡織': 'textile', '電機': 'motor',
               '電器': 'electrical_appliance',
               '玻璃': 'glass', '造紙': 'papermaking', '鋼鐵': 'steel', '橡膠': 'rubber', '汽車': 'car', '營造': 'build',
               '運輸': 'transport', '觀光': 'sightseeing', '金融': 'finance', '百貨': 'department_store', '其他': 'others',
               '化學': 'chemistry', '生技': 'biological_technology', '油電燃氣': 'oil_electricity_gas',
               '半導體': 'semiconductor', '電腦週邊': 'computer', '光電': 'photoelectric', '通信網路': 'communication_network',
               '電子零件': 'electronic_component', '電子通路': 'electronic_access', '資訊服務': 'information_service',
               '其他電子': 'other_electronics'}


def industry_crawler():
    url = 'https://www.cnyes.com/twstock/stock_astock.aspx?ga=nav'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'}

    res = requests.get(url, headers=headers)
    # print(res.status_code)
    soup = BeautifulSoup(res.text, 'html.parser')

    industries = soup.select('div[id="kinditem_0"]>ul[class="kdlist"]>li')
    # get all industries
    for industry in industries:
        industry_name = industry.a.text
        print(industry_name)
        industry_url = 'https://www.cnyes.com/twstock/' + industry.a["href"]
        print(industry_url)
        industry_id = industry_url.split('groupId=')[-1].split('&stitle')[0]
        # get all stocks from the industry
        res_stock = requests.get(industry_url, headers=headers)
        # print(res_stock.status_code)
        soup_stock = BeautifulSoup(res_stock.text, 'html.parser')
        stocks = soup_stock.select('div[class="TableBox"]>table>tr')
        stock_list = []
        stock_dict = dict()
        for stock in stocks[1:]:
            stock_info = stock.find_all('td')
            stock_id = stock_info[1].text
            # print(stock_id)
            stock_name = stock_info[2].text
            # print(stock_name)
            stock_list.append(stock_id)
            stock_dict[stock_id] = stock_name

        industry_key_id = 'industry_' + industry_id
        doc = {'_id': industry_key_id,
               'industry': industry_kv[industry_name],
               'industry_name': industry_name,
               'stocks_list': stock_list,
               'stocks_count': len(stock_list),
               'stocks': stock_dict}
        # print(doc)
        mongo_client = mongo.mongo_connection('linode1', 'mongo')
        mongo_collection = mongo.mongo_collection(mongo_client, 'stocks', 'stockIndustry')
        mongo.insert_document(mongo_collection, doc)
        time.sleep(20)


if __name__ == '__main__':
    industry_crawler()
