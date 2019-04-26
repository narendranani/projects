# variables
from db_configurations import  source_details, target_details
from sql_queries import *
from variables import *
# methods
from etl import etl_process, to_csv
import pyodbc


def main(source_details={}, target_details={}):
    print('************************************Starting ETL************************************')
    try:
        print("Loading data from {0} to {1}".format(source_details["name"], target_details["name"]))
        if target_details["name"] == 'csv':
            to_csv(source_details, target_details)
        else:
            etl_process(source_details, target_details)
    except Exception as error:
        print("Error: ", error)
        raise error


if __name__ == "__main__":
    main(source_details, target_details)
