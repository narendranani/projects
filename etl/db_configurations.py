# sql-server (source db)
source_sqlserver = {
    "name": "sqlserver",
    "credentials": {
        'Trusted_Connection': 'no',
        'driver': '{SQL Server}',
        'server': 'NARENDRA/SQLExpress',
        'database': 'Test',
        'user': 'sa',
        'password': 'xxxxxxxxxxxxx',
        'autocommit': False,
    },
    "schema": "dbo",
    "tables": ["CLIENT"],
    "extract_queries": [""]
}

# sql-server (source db)
target_sqlserver = {
    "name": "sqlserver",
    "credentials": {
        'Trusted_Connection': 'no',
        'driver': '{SQL Server}',
        'server': 'NARENDRA/SQLExpress',
        'database': 'Test',
        'user': 'sa',
        'password': 'xxxxxxxxxxx',
        'autocommit': False,
    },
    "schema": "dbo",
    "tables": ["client_target"]
}

# mysql
mysql = {
    "name": "mysql",
    "credentials": {
        'user': 'your_user_1',
        'password': 'your_password_1',
        'host': 'db_connection_string_1',
        'database': 'db_1',
    },
    "schema": "",
    "tables": [],
    "extract_query": "",
    "load_query": ""
}

# redshift
redshift = {
    "name": "redshift",
    "credentials": {
        "host": "dj-analytics-stage.internal.demandjump.net",
        "port": "5439",
        "user": "Test",
        "password": "xxxxxxxxxxxx",
        "dbname": "analyticsdb"
    },
    "schema": "",
    "tables": [],
    "extract_query": "",
    "load_query": ""
}
from copy import deepcopy

source_details = deepcopy(source_sqlserver)
target_details = deepcopy(target_sqlserver)

# BULK INSERT dbo.customer_py
# FROM 'D:\\Python\\PythonSessionDemo\\sql_txt.txt' --'D:\\Python\\PythonSessionDemo\\sql_csv.csv'
# WITH
# (
#     --DATAFILETYPE = 'char',
# FIELDTERMINATOR = ',',
# ROWTERMINATOR = '\n'
# );
