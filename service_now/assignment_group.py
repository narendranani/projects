from utilities import get_api_data, get_db_connection, insert_into_db, load_json, get_params, create_stg_table, \
    truncate_table, drop_stg_table, get_insert_query

TABLE = 'SNOW_API_Assignment_Group'
STG_TABLE = 'STG_SNOW_API_Assignment_Group'
BATCH = 1000


def get_data(api_details, columns):
    params = get_params(columns)
    data = \
        get_api_data(api_details["url"]["assignment_group"], api_details["username"], api_details["password"],
                     params=params)[
            "result"].__iter__()
    while True:
        rows = (next(data, None) for x in range(BATCH))
        rows = [[row[column] for column in columns] for row in rows if row]
        yield rows
        if not rows:
            return


def insert_to_stage(api_details, columns):
    print(f'Inserting data into {STG_TABLE} table')
    try:
        cnx = get_db_connection(auto_commit=True)
        cs = cnx.cursor()
        drop_stg_table(cs, STG_TABLE)
        create_stg_table(cs, TABLE, STG_TABLE)
        for idx, data in enumerate(get_data(api_details, columns)):
            # print(f'data: {data}')
            if data:
                insert_query = get_insert_query(STG_TABLE,columns, data)
                # print(f'insert_query: {insert_query}')
                cs.execute(insert_query)
                # insert_into_db(cs, data, STG_TABLE, columns)
                print(f'Batch: {idx} - Loaded {len(data)} rows')
    finally:
        cs.close()
        cnx.close()


def load_to_main_table():
    try:
        cnx = get_db_connection(auto_commit=False)
        cs = cnx.cursor()
        truncate_table(cs, TABLE)
        query = f'INSERT INTO {TABLE} SELECT * FROM {STG_TABLE}'
        cs.execute(query)
        cs.commit()
    except Exception as e:
        cs.rollback()
        raise Exception
    finally:
        cnx.autocommit = True
        drop_stg_table(cs, STG_TABLE)
        cs.close()
        cnx.close()


if __name__ == '__main__':
    from datetime import datetime
    print(datetime.now())
    print('Started migration')
    config = load_json('config.json')
    schema = load_json('schema.json')
    columns = schema["schema"]["assignment_group"]
    insert_to_stage(config['api_details'], columns)
    print('Loaded to Stage table')
    load_to_main_table()
    print('Successfully Loaded to Main table')
    print(datetime.now())
