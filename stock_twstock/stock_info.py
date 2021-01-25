"""
使用twstock獲取台股資料
https://twstock.readthedocs.io/zh_TW/latest/
pip install twstock

stock_id|datetime|category
"""

import twstock


# 股票歷史資料查詢
def stock_info(stock_id, search_list):
    stock = twstock.Stock(stock_id)
    name = stock.fetcher
    if search_list != 'OK':
        # 查詢單月
        if search_list[-1] == '1month':
            year = search_list[0]
            month = search_list[1]
            output = stock.fetch(int(year), int(month))
            return output
        # 距現在31天的資料
        elif search_list[-1] == '31days':
            output = stock.fetch_31()
            return output
        # 指定時間至今的所有資料
        elif search_list[-1] == 'since':
            year = search_list[0]
            month = search_list[1]
            output = stock.fetch_from(int(year), int(month))
            return output
    else:
        # 距當下最近的一筆資料
        output = stock.data[-1]
        return output, name

# 即時股票資料查詢
def stock_instance(stock_id):
    r_stock = twstock.realtime.get(stock_id)
    

if __name__ == '__main__':
    """"
    id|yearmonthdate|A
    """
    user_input = str(input('請輸入股票代號和需要查詢的內容：'))
    user_input_list = user_input.split('|')
    stock_id = user_input_list[0]
    search_list = user_input_list[1:]
    print(search_list)
    if len(user_input_list) > 1:
        try:
            search_result = stock_info(stock_id, search_list)
            print(search_result)
        except KeyError as e:
            print(f'代碼不存在，請重新確認後輸入正確代碼。')
    else:
        try:
            search_result = stock_info(stock_id, 'OK')[0]
            yyy = stock_info(stock_id, 'OK')[1]
            result_output=f"""【查詢結果】:
    日期時間：{search_result.date}
    股票名稱:{yyy}
    股票代碼：{stock_id}
    總成交股數：{search_result.capacity}
    總成交金額：{search_result.turnover}
    開盤價：{search_result.open}
    盤中最高價：{search_result.high}
    盤中最低價：{search_result.low}
    收盤價：{search_result.close}
    漲跌價差：{search_result.change}
    成交筆數：{search_result.transaction}"""
            print(result_output)
        except KeyError as e:
            print(f'代碼不存在，請重新確認後輸入正確代碼。')
