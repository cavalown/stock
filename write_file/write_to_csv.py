<<<<<<< HEAD
import sys
sys.path.append(r'/home/cavalown/stock_project')
=======
# import sys
# sys.path.append(r'/home/cavalown/stock_project/stock')
>>>>>>> 92f675d74971e9546a619a610be4d01036a557ae
import datetime
import csv


def makeColumn(fileName, columns):
    with open('{}.csv'.format(fileName), 'w') as file:
        writeCsv = csv.writer(file)
        writeCsv.writerow(columns)
    return fileName


def writeToCsv(fileName, document_list):
    with open('{}.csv'.format(fileName), 'a', newline='') as file:
        writeCsv = csv.writer(file)
        writeCsv.writerow(document_list)


if __name__ == '__main__':
    writeToCsv('test', ['It works!!!: {}'.format(datetime.datetime.now())])
