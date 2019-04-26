import base64
import json
import pyodbc
import requests


class ApiCallError(Exception):
    pass


def load_json(path):
    with open(path) as fil:
        return json.load(fil)


def _check_status(server_response, success_code):
    if server_response.status_code != success_code:
        error_message = server_response['error']['message'] + f". Detail: {server_response['error']['detail']}"
        raise ApiCallError(error_message)
    return


def get_token(username, password):
    usr_pwd = str.encode('{}:{}'.format(username, password))
    token = base64.b64encode(usr_pwd)
    # print(token)
    return token.decode("utf-8")


def get_params(fields):
    params = {}
    if fields:
        params.update({"sysparm_fields": ','.join(fields)})
    return params


def get_api_data(url, username, password, params={}):
    auth_token = get_token(username, password)
    header = {'Authorization': f'Basic {auth_token}'}
    server_response = requests.get(url, headers=header, params=params)
    _check_status(server_response, 200)
    return server_response.json()


def get_db_connection(auto_commit):
    config = load_json('config.json')
    config['db_credentials']['autocommit'] = auto_commit
    return pyodbc.connect(**config['db_credentials'])


def create_stg_table(cs, table_name, stg_table):
    query = f'SELECT * INTO {stg_table} FROM {table_name} WHERE 1=2'
    cs.execute(query)


def drop_stg_table(cs, table_name):
    query = f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    cs.execute(query)


def truncate_table(cs, table_name):
    query = f'TRUNCATE TABLE {table_name}'
    cs.execute(query)


def insert_into_db(cs, data, table, columns):
    placeholders = '?,' * len(columns)
    placeholders = placeholders.strip(',')
    columns = ','.join(columns)
    insert_query = f'INSERT INTO {table}( {columns}) VALUES( {placeholders}) '
    cs.executemany(insert_query, data)


def get_insert_query(table_name, columns, data):
    inert_query = f"insert into {table_name}({','.join(columns)}) values"
    rows = ['(' + ','.join(
        "'" + value.replace("'", "''") + "'" if isinstance(value, str) else 'NULL' if not value else str(value) for
        value in row) + ')' for row
            in data]
    return inert_query + ',\n'.join(rows)
