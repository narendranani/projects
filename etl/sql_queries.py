import datetime

mysql_get_columns_query = ""
sql_get_columns_query = "Select  Stuff(\n        (\n        Select ', ' + C.COLUMN_NAME\n        From INFORMATION_SCHEMA.COLUMNS As C\n        Where C.TABLE_SCHEMA = T.TABLE_SCHEMA\n            And C.TABLE_NAME = T.TABLE_NAME\n\t\t\tAND upper(C.TABLE_NAME) = upper('<table_name>')\n\t\t\tAND upper(C.TABLE_SCHEMA) = upper('<schema_name>')\n        Order By C.ORDINAL_POSITION\n        For Xml Path('')\n        ), 1, 2, '') As Columns\nFrom INFORMATION_SCHEMA.TABLES As T\nWHERE upper(TABLE_NAME) = upper('<table_name>')\nAND upper(TABLE_SCHEMA) = upper('<schema_name>')"
redshift_get_columns_query = ""


def get_extract_query(source_cursor, source_platform, schema, table):
    column_list = get_columns(source_cursor, source_platform, schema, table)
    query = 'SELECT {0} FROM {1}.{2}'.format(column_list, schema, table)
    return query


def get_load_query(data, target_cursor, target_db, schema, table):
    print(datetime.datetime.now())
    column_list = get_columns(target_cursor, target_db, schema, table)
    values = []
    value_list = []
    for row in data:
        for value in row:
            if isinstance(value, str):
                value_list.append("'" + str(value).replace("'", "''") + "'")
            elif isinstance(value, datetime.datetime):
                value_list.append("'" + str(value.strftime("%Y-%m-%d %H:%M:%S")) + "'")
            else:
                value_list.append(str(value))
        values.append('(' + ','.join(value_list) + ')')
        value_list = []
    values = '\n,'.join(values)
    values = values.replace('None', 'NULL')

    insert_query = 'INSERT INTO {0}.{1}({2}) VALUES {3}'.format(schema, table, column_list, values)
    print(datetime.datetime.now())
    return insert_query


def get_columns(db_cursor, db_platform, schema, table):
    if db_platform == 'mysql':
        get_columns_query = mysql_get_columns_query.replace('<schema_name>', schema).replace('<table_name>', table)
    elif db_platform == 'sqlserver':
        get_columns_query = sql_get_columns_query.replace('<schema_name>', schema).replace('<table_name>', table)
    elif db_platform == 'redshift':
        get_columns_query = redshift_get_columns_query.replace('<schema_name>', schema).replace('<table_name>', table)
    else:
        return 'Error! unrecognised db platform'
    db_cursor.execute(get_columns_query)
    column_list = db_cursor.fetchall()
    return column_list[0][0].strip().rstrip(',')
