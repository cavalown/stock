import psycopg2

import read_yaml as ryaml

credential_path = 'credential/db.yaml'

"""
steps:
1. make a connection
2. use the connection to build a cursor
3. read or update tables need cursor
4. in the end, close the connection
"""

# connection
def postgres_connection(machine, db_class, database):
    credential = ryaml.read_yaml(credential_path)
    db_info = credential[machine][db_class]
    host = db_info['host']
    port = db_info['port']
    # db = db_info['db']
    user = db_info['user']
    password = db_info['pswd']
    connect = psycopg2.connect(database=database, user=user,
                               password=password, host=host, port=port)
    print(f"Connect to {database} successfully!")
    return connect

# cursor
def make_cursor(connection):
    cursor = connection.cursor()
    print("And get cursor.")
    return cursor

# close connection
def close_connection(connection):
    connection.close()
    print(f'{connection} is closed!')


# create table db.schema.table and default schema : public
def createTable(connection, query):
    cursor = make_cursor(connection)
    cursor.execute(query)
    connection.commit()
    close_connection(connection)
    print(f"Create table successfully!")
    

# Read tables
def readTable(query, cursor):
    # query = """SELECT {} from {};""".format(item, tableName))
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows


# Update tables
def updateTable(query, cursor, connection):
    # query = f"""Update book set {} where {}"""
    cursor.execute(query)
    connection.commit()
    count = cursor.rowcount
    close_connection(connection)
    print(count, "rows Updated successfully!")


# insert table
def insertTable(query, cursor, connect):
    # "INSERT INTO a_table (c1, c2, c3) VALUES(%s, %s, %s)", (v1, v2, v3)
    cursor.execute(query)
    connect.commit()  # <- We MUST commit to reflect the inserted data
    cursor.close()
    connect.close()
    print(connect, "Insert successfully!")


if __name__ == '__main__':
    connection = postgres_connection('linode1', 'postgres', 'stock')
    cursor = make_cursor(connection)
    print(cursor)
