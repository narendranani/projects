# python modules
import mysql.connector
import pyodbc
import psycopg2
# variables
from variables import datawarehouse_name
# from db_configurations import mysql, sqlserver, redshift
import datetime
import sql_queries


def etl_process(source_details={}, target_details={}):
    # establish source db connection
    source_tables = source_details["tables"]
    extract_queries = source_details["extract_queries"]
    target_tables = target_details["tables"]
    load_queries = target_details["load_queries"]
    for i in range(len(source_tables)):
        __etl_process(source_details, target_details, source_tables[i], extract_queries[i], target_tables[i],
                      load_queries[i])


def __etl_process(source_details, target_details, *args):
    source_table = args[0]
    extract_query = args[1]
    target_table = args[2]
    # load_query = args[3]
    source_cnx = get_connection(source_details["name"], source_details["credentials"])
    target_cnx = get_connection(target_details["name"], target_details["credentials"])
    source_cursor = source_cnx.cursor()
    extract_query = extract_query if extract_query.strip() else sql_queries.get_extract_query(source_cursor,
                                                                                              source_details["name"],
                                                                                              source_details["schema"],
                                                                                              source_table)
    try:
        source_cursor.execute(extract_query)
        identity_insert = 'SET IDENTITY_INSERT {0} ON'.format(target_table)
        data = source_cursor.fetchall()
        target_cursor = target_cnx.cursor()
        target_cursor.execute(identity_insert)
        # load data into warehouse db
        if data:
            load_query = sql_queries.get_load_query(data, target_cursor,
                                                    target_details["name"],
                                                    target_details["schema"],
                                                    target_table)
            target_cursor.execute(load_query)
            print('data loaded to warehouse db')
            source_cnx.commit()
            target_cnx.commit()
        else:
            print('data is empty')
    except Exception as error:
        print("Error: ", error)
        source_cnx.rollback()
        target_cnx.rollback()
        raise error
    finally:
        # close the source db connection
        target_cursor.close()
        source_cursor.close()
        source_cnx.close()
        target_cnx.close()


def to_csv(source_details, target_details):
    import csv
    file_name = target_details["file_name"] if target_details[
        "file_name"].strip() else datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S") + '.csv'
    source_cnx = get_connection(source_details["name"], source_details["credentials"])
    source_cursor = source_cnx.cursor()
    source_cursor.execute(source_details["extract_query"])
    data = source_cursor.fetchall()
    with open(file_name, "w") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(data)
    del data


def get_connection(db_platform, credentials):
    if db_platform == 'mysql':
        cnx = mysql.connector.connect(**credentials)
    elif db_platform == 'sqlserver':
        cnx = pyodbc.connect(**credentials)
    elif db_platform == 'redshift':
        cnx = psycopg2.connect(**credentials)
    else:
        return 'Error! unrecognised db platform'
    return cnx


def build_query(data, insert_query):
    print(datetime.datetime.now())
    values = []
    # data = [('na', 're'), ('nd', 'ra'), ('ya', 'mu')]
    value_list = []
    for row in data:
        for value in row:
            value_list.append("'" + str(value) + "'")
        values.append('(' + ','.join(value_list) + ')')
        value_list = []
    values = '\n,'.join(values)
    query = insert_query.replace('<values>', values)
    print("query: ", query)
    print(datetime.datetime.now())
    return query
