import psycopg2

# from myPackage import read_yaml as ryaml
import read_yaml as ryaml

credential_path = '../../credential/db.yaml'

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
    connection = psycopg2.connect(database=database, user=user,
                                  password=password, host=host, port=port)
    print(f"Connect to {database} successfully!")
    return connection

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
def insertTable(connection, cursor, query):
    # "INSERT INTO a_table (c1, c2, c3) VALUES(%s, %s, %s)", (v1, v2, v3)
    cursor.execute(query)
    connection.commit()  # <- We MUST commit to reflect the inserted data
    cursor.close()
    connection.close()
    print(connection, "Insert successfully!")


# check if table exist
def check_table_exist(cursor, tableName):
    result = cursor.execute(f"""SELECT to_regclass('{tableName}')""")
    print(type(result), result)
    return result


if __name__ == '__main__':
    connection = postgres_connection('linode1', 'postgres', 'test')
    cursor = make_cursor(connection)
    check_table_exist(cursor, "persons")
    # query = """INSERT INTO persons ("personid", "lastname", "firstname", "address", "city") VALUES (124, 'chen', 'yishien', 'No. 155, second Road', 'chiayi');"""
    # insertTable(connection, cursor, query)
