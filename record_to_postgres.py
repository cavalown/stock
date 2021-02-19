import re
from os import PRIO_PGRP

from myPackage import mongoServer as mon
from myPackage import postgresServer as pos


def get_month_record_mongo(stock_id, year, month):
    client = mon.mongo_connection('linode1', 'mongo')
    collection = mon.mongo_collection(client, 'stocks', f"stock{stock_id}")
    contents = list(collection.find(
        {'_id': re.compile(f"{stock_id}{year}{month}")}))
    return contents


def get_3_month_records(collection, stock_id, year, month_list):
    contents = list(collection.find(
        {'$or': [{'_id': re.compile(f"{stock_id}{year}{month_list[0]}")},
                 {'_id': re.compile(f"{stock_id}{year}{month_list[1]}")},
                 {'_id': re.compile(f"{stock_id}{year}{month_list[2]}")}]
         }))
    return contents


def get_season_record_mongo(stock_id, year, season):
    client = mon.mongo_connection('linode1', 'mongo')
    collection = mon.mongo_collection(client, 'stocks', f"stock{stock_id}")
    print(collection.count_documents({}))
    if season == '1':
        contents = get_3_month_records(
            collection, stock_id, year, ['01', '02', '03'])
        return contents
    elif season == '2':
        contents = get_3_month_records(
            collection, stock_id, year, ['04', '05', '06'])
        return contents
    elif season == '3':
        contents = get_3_month_records(
            collection, stock_id, year, ['07', '08', '09'])
        return contents
    elif season == '4':
        contents = get_3_month_records(
            collection, stock_id, year, ['10', '11', '12'])
        return contents
    else:
        return


def get_year_record_mongo(stock_id, year):
    client = mon.mongo_connection('linode1', 'mongo')
    collection = mon.mongo_collection(client, 'stocks', f"stock{stock_id}")
    contents = list(collection.find({'_id': re.compile(f"{stock_id}{year}")}))
    return contents


def check_if_num(item):
    item = str(item)
    # float(0.00) if change_ori == 'X0.00' else float(change_ori)
    if item in ['X0.00', 'x0.00']:
        return False
    elif len(item.split('.')) > 1:
        left = item.split('.')[0]
        right = item.split('.')[1]
        if left.isdigit() and right.isdigit():
            return True
        return False
    else:
        return False


def compute_records(duration_records):
    doc = dict()
    columns = ['price', 'open', 'high', 'low', 'close', 'volume', 'trades']
    # average
    for column in columns[:5]:
        print('Column:', column)
        sum = 0
        count = 0
        for record in duration_records:
            print(record)
            item = record[column]
            print(item)
            if check_if_num(item):
                sum += float(item)
                count += 1
        print('SUM:', sum)
        print('COUNT:', count)
        average = sum / count
        print('Column:', column, 'Average:', average)
        doc[f'avg{column.title()}'] = average
    # sum
    for column in columns[5:]:
        print('Column:', column)
        sum = 0
        for record in duration_records:
            item = record[column]
            if check_if_num(item):
                sum += float(item)
        print('SUM:', sum)
        doc[f'sum{column.title()}'] = sum
    print(doc)
    return doc


def store_to_postgres():
    connect = pos.postgres_connection('linode1', 'postgres')
    cursor = pos.make_cursor(connect)
    return cursor

def record_computed_month_to_post(stock_id,month_records):
    contents = compute_records(duration_records=month_records)
    query = f"""INSERT INTO 'month{stock_id}' ('avg_price', 'avg_open', 'avg_high', 'avg_low', 'avg_close', 'sum_volume', 'sum_trades') 
                                        VALUES({contents[0]},{contents[1]},{contents[2]},{contents[3]},{contents[4]},{contents[5]},{contents[6]})"""
    connect = pos.postgres_connection('linode1', 'postgres')
    cursor = pos.make_cursor(connect)
    pos.insertTable(query, cursor, connect)
    return

def record_computed_season_to_post(stock_id, season_records):
    contents = compute_records(duration_records=season_records)
    query = f"""INSERT INTO 'season{stock_id}' ('avg_price', 'avg_open', 'avg_high', 'avg_low', 'avg_close', 'sum_volume', 'sum_trades') 
                                        VALUES({contents[0]},{contents[1]},{contents[2]},{contents[3]},{contents[4]},{contents[5]},{contents[6]})"""
    connect = pos.postgres_connection('linode1', 'postgres')
    cursor = pos.make_cursor(connect)
    pos.insertTable(query, cursor, connect)

def record_computed_year_to_post(stock_id, year_records):
    contents = compute_records(duration_records=year_records)
    query = f"""INSERT INTO 'year{stock_id}' ('avg_price', 'avg_open', 'avg_high', 'avg_low', 'avg_close', 'sum_volume', 'sum_trades') 
                                        VALUES({contents[0]},{contents[1]},{contents[2]},{contents[3]},{contents[4]},{contents[5]},{contents[6]})"""
    connect = pos.postgres_connection('linode1', 'postgres')
    cursor = pos.make_cursor(connect)
    pos.insertTable(query, cursor, connect)


if __name__ == '__main__':
    # contents = get_month_record_mongo('1110', '2010', '01')
    # print(contents)
    # compute_month(contents)
    # print('='*10)
    # contents_year = get_year_record_mongo('stock1101', '2010')
    # compute_year(contents_year)
    contents_season = get_season_record_mongo('1110', '2010', '1')
    print(contents_season)
