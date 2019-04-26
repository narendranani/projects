from utilities import get_api_data, get_db_connection, insert_into_db, load_json, get_params, create_stg_table, \
    truncate_table, drop_stg_table, get_insert_query

TABLE = 'SNOW_API_Service_Request'
STG_TABLE = 'STG_SNOW_API_Service_Request'
BATCH = 1000


def get_data(api_details, columns, column_names):
    params = get_params(column_names)
    data = \
        get_api_data(api_details["url"]["service"], api_details["username"], api_details["password"], params=params)[
            "result"].__iter__()
    while True:
        rows = (next(data, None) for x in range(BATCH))
        final_data = []
        for row in rows:
            row_data = []
            if row:
                for column in columns:
                    if column['value']:
                        row_data.append(
                            row[column['field']].get('value') if not isinstance(row[column['field']], str) else None)
                    else:
                        row_data.append(row[column['field']])
            if row_data:
                final_data.append(row_data)

        yield final_data
        if not final_data:
            return


def insert_to_stage(api_details, columns, column_names):
    print(f'Inserting data into {STG_TABLE} table')
    try:
        cnx = get_db_connection(auto_commit=True)
        cs = cnx.cursor()
        drop_stg_table(cs, STG_TABLE)
        create_stg_table(cs, TABLE, STG_TABLE)
        for idx, data in enumerate(get_data(api_details, columns, column_names)):
            # print(f'data: {data}')
            if data:
                insert_query = get_insert_query(STG_TABLE,column_names, data)
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
        query = f"""
        INSERT INTO {TABLE} (Number, Item, stage, Priority, short_description, Request, RequestedFor, due_date, assignment_group, opened_at, opened_by, closed_at, closed_by)
            SELECT stg.Number, stg.Item, stg.stage, SP.Priority, stg.short_description, SAR.Number  as Request, SAR.Requested_For, 
                stg.due_date, AG.name as assignment_group, stg.opened_at, OSU.name as opened_by, stg.closed_at, CSU.name as closed_by
            FROM {STG_TABLE} STG
            LEFT JOIN [dbo].[SNOW_API_Assignment_Group] AG
                ON STG.assignment_group = AG.sys_id  
            LEFT JOIN SNOW_API_Request SAR
                ON STG.Request = SAR.sys_id
            LEFT JOIN [dbo].[SNOW_API_System_User] OSU
                ON STG.opened_by = OSU.sys_id   
            LEFT JOIN [dbo].[SNOW_API_System_User] CSU
                ON STG.closed_by = CSU.sys_id   
            LEFT JOIN dbo.SNOW_Priority SP
                ON STG.Priority = SP.Id
        """
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
    columns = schema["schema"]["service"]
    column_names = [fields['field'] for fields in columns]
    insert_to_stage(config['api_details'], columns, column_names)
    print('Loaded to Stage table')
    load_to_main_table()
    print('Successfully Loaded to Main table')
    print(datetime.now())
