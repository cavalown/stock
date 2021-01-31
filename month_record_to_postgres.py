from os import PRIO_PGRP
from myPackage import postgresServer as pos
from myPackage import mongoServer as mon


def get_month_record_mongo(collection_name, year, month):
    client = mon.mongo_connection('linode1', 'mongo')
    collection = mon.mongo_collection(client, 'stocks', collection_name)
    contents = list(collection.find({'year': year, 'month': month}))
    return contents

def season_record_mongo(collection_name, year, season):
    if season == 1
    return

def get_year_record_mongo(collection_name, year):
    client = mon.mongo_connection('linode1', 'mongo')
    collection = mon.mongo_collection(client, 'stocks', collection_name)
    contents = list(collection.find({'year': year}))
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


def compute_month(month_records):
    doc = dict()
    columns = ['price', 'open', 'high', 'low', 'close', 'volume', 'trades']
    # average
    for column in columns[:5]:
        print('Column:', column)
        sum = 0
        count = 0
        for record in month_records:
            item = record[column]
            if check_if_num(item): 
                sum += float(item)
                count += 1
        print('SUM:',sum)
        print('COUNT:',count)
        average = sum / count
        print( 'Column:',column,'Average:',average)
        doc[f'avg{column.title()}'] = average
    # sum
    for column in columns[5:]:
        print('Column:', column)
        sum = 0
        for record in month_records:
            item = record[column]
            if check_if_num(item):
                sum += float(item)
        print('SUM:', sum)
        doc[f'sum{column.title()}'] = sum
    print(doc)
    return doc

def compute_season(season_records):
    doc = dict()
    columns = ['price', 'open', 'high', 'low', 'close', 'volume', 'trades']
    # average
    for column in columns[:5]:
        print('Column:', column)
        sum = 0
        count = 0
        for record in season_records:
            item = record[column]
            if check_if_num(item): 
                sum += float(item)
                count += 1
        print('SUM:',sum)
        print('COUNT:',count)
        average = sum / count
        print( 'Column:',column,'Average:',average)
        doc[f'avg{column.title()}'] = average
    # sum
    for column in columns[5:]:
        print('Column:', column)
        sum = 0
        for record in season_records:
            item = record[column]
            if check_if_num(item):
                sum += float(item)
        print('SUM:', sum)
        doc[f'sum{column.title()}'] = sum
    print(doc)
    return doc

def compute_year(year_records):
    doc = dict()
    columns = ['price', 'open', 'high', 'low', 'close', 'volume', 'trades']
    # average
    for column in columns[:5]:
        print('Column:', column)
        sum = 0
        count = 0
        for record in year_records:
            item = record[column]
            if check_if_num(item): 
                sum += float(item)
                count += 1
        print('SUM:',sum)
        print('COUNT:',count)
        average = sum / count
        print( 'Column:',column,'Average:',average)
        doc[f'avg{column.title()}'] = average
    # sum
    for column in columns[5:]:
        print('Column:', column)
        sum = 0
        for record in year_records:
            item = record[column]
            if check_if_num(item):
                sum += float(item)
        print('SUM:', sum)
        doc[f'sum{column.title()}'] = sum
    print(doc)
    return doc

def save_record_to_postgres():
    return

    


if __name__ == '__main__':
    contents = get_month_record_mongo('stock1101', '2010', '01')
    # print(contents)
    compute_month(contents)
    print('='*10)
    contents_year = get_year_record_mongo('stock1101', '2010')
    compute_year(contents_year)
