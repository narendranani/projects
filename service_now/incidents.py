import argparse
from datetime import datetime
from utilities import get_api_data, get_db_connection, insert_into_db, load_json, get_params, create_stg_table, \
    truncate_table, drop_stg_table, get_insert_query

TABLE = 'SNOW_API_Incident_Request'
STG_TABLE = 'STG_SNOW_API_Incident_Request'
BATCH = 1000


def get_data(api_details, columns, column_names):
    params = get_params(column_names)
    data = \
        get_api_data(api_details["url"]["incident"], api_details["username"], api_details["password"], params=params)[
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
        INSERT INTO {TABLE} (Number, opened_at, short_description, priority, state, category, assignment_group, assigned_to, sys_updated_on, sys_updated_by, closed_at)
        SELECT Number, stg.opened_at, stg.short_description, SP.priority, SIS.state, stg.category, 
            AG.name, SU.name , stg.sys_updated_on, stg.sys_updated_by, stg.closed_at 
        FROM {STG_TABLE} STG
        LEFT JOIN [dbo].[SNOW_API_Assignment_Group] AG
            ON STG.assignment_group = AG.sys_id  
        LEFT JOIN [dbo].[SNOW_API_System_User] SU
            ON STG.assigned_to = SU.sys_id   
        LEFT JOIN dbo.SNOW_Priority SP
            ON STG.Priority = SP.Id
        LEFT JOIN SNOW_Incident_State SIS
            ON STG.State = SIS.Id
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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config',
        help="Config File",
        required=True)
    parser.add_argument(
        '-s', '--schema',
        help="Schema File",
        required=True)
    args = parser.parse_args()
    print(datetime.now())
    print('Started migration')
    config = load_json(args.config)
    schema = load_json(args.schema)
    columns = schema["schema"]["incident"]
    column_names = [fields['field'] for fields in columns]
    insert_to_stage(config['api_details'], columns, column_names)
    print('Loaded to Stage table')
    load_to_main_table()
    print('Successfully Loaded to Main table')
