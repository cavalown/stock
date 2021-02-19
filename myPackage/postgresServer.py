import psycopg2
from myPackage import read_yaml as ryaml

credential_path = 'credential/db.yaml'  

"""
steps:
1. make a connection
2. use the connection to build a cursor
3. read or update tables need cursor
4. in the end, close the connection
"""


def postgres_connection(machine, db_class):
    credential = ryaml.read_yaml(credential_path)
    db_info = credential[machine][db_class]
    host = db_info['host']
    port = db_info['port']
    db = db_info['db']
    user = db_info['user']
    password = db_info['pswd']
    connect = psycopg2.connect(database=db, user=user,
                               password=password, host=host, port=port)
    print("Connect to {} successfully!".format(db_name))
    return connect


def make_cursor(connect):
    cursor = connect.cursor()
    print("And get cursor.")
    return cursor


def close_connection(connection):
    connection.close()
    print('{} is closed!'.format(connection))


# Read tables
def readTable(query, cursor):
    # query = """SELECT {} from {};""".format(item, tableName))
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows


# Update tables
def updateTable(query, cursor, connect):
    # query = """Update book set {} where {}""".format()
    cursor.execute(query)
    connect.commit()
    count = cursor.rowcount
    print(count, "rows Updated successfully!")


# insert tables
def insertTable(query, cursor, connect):
    # "INSERT INTO a_table (c1, c2, c3) VALUES(%s, %s, %s)", (v1, v2, v3)
    cursor.execute(query)
    connect.commit()  # <- We MUST commit to reflect the inserted data
    cursor.close()
    connect.close()
    print(connect, "Insert successfully!")

if __name__ == '__main__':
    cn_cconnect = postgres_connection('postgresCN')
    cn_cursor = make_cursor(cn_cconnect)
    query = 'select public_path ,file_path from pat_attachment limit 10'
    content = readTable(query, cn_cursor)
    for i in content:
        print(i)
    close_connection(cn_cconnect)
