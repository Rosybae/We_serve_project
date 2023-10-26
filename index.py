import pandas as pd
from configparser import ConfigParser
from extraction import extract_jobs
from utils.helper import (create_s3_bucket, connect_to_warehouse, STAGING_DATASET, DWH_HOST,
                    RAW_DATASET, s3_lake_path, BUCKET_NAME)
from sql.create import raw_dataset, raw_tables
from sql.transform import transform_tables, staging_dataset
import os
from sql.insert import insert_into_transform
from utils.constant import tables






def extract_all_jobs():
    for job in extract_jobs:
        job()


# Raw schema and dev schema in data warehouse

def raw_dataset():
    conn = connect_to_warehouse()
    cursor = conn.cursor()
    print('Create raw schema')
    cursor.execute(raw_dataset.format(RAW_DATASET))
    conn.commit()
    cursor.close()




def staging_dataset():
    conn = connect_to_warehouse()
    cursor = conn.cursor()
    print(staging_dataset.__name__)
    cursor.execute(transform_tables.format(STAGING_DATASET))
    conn.commit()
    cursor.close()



def create_raw_tables():
    for query in raw_tables:
        conn = connect_to_warehouse()
        cursor = conn.cursor()
        print(f"{query[:35]}")
        cursor.execute(query.format(RAW_DATASET))
        conn.commit()
        print('All tables created')
    cursor.close()




def create_trans_tables():
    for query in transform_tables:
        conn = connect_to_warehouse()
        cursor = conn.cursor()
        print(f"{query[:45]}")
        cursor.execute(query.format(STAGING_DATASET))
        conn.commit()
        print('All transform tables created')
    cursor.close()



def copy_from_s3_dwh():
    try:
        dwh_conn = connect_to_warehouse()
        cursor = dwh_conn.cursor()
        for table in transform_tables:
            print(f"Copying {table} from s3 to DWH")
            table_copy_query = f"""
            copy {RAW_DATASET}.{table}
            from '{s3_lake_path.format(BUCKET_NAME, table)}'
            iam_role '{DWH_HOST}' 
            delimiter ','
            ignoreheader 1;
        """
            cursor.execute(table_copy_query)
            dwh_conn.commit()
        cursor.close()
        dwh_conn.close()
    except Exception as e:
        print(e)




def insert_into_trans_tables():
    for query in insert_into_transform:
        conn = connect_to_warehouse()
        cursor = conn.cursor()
        print(f"{query[:20]}")
        cursor.execute(query.format(STAGING_DATASET))
        conn.commit()
    print('All transform tables inserted')
    cursor.close()


def running_all_functions_jobs():
    create_s3_bucket()
    extract_all_jobs()
    raw_dataset()
    staging_dataset()
    create_raw_tables()
    create_trans_tables()
    copy_from_s3_dwh()
    insert_into_trans_tables()

