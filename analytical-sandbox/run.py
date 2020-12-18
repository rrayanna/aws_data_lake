# Python script.
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import sys
import gc
from utils import handle_partition
from datetime import datetime
from config import config
from spark.spark import start_spark
from jobs.job import etl_job
from aws.aws_functions import build_athena_catalog


def main():

    # Get Sys arguments
    csv_file = sys.argv[1]  # Input CSV File as a Parameter
    group_name = sys.argv[2]+'/'

    # Load the parameters
    params = config.Staging()
    s3_path = params.s3_path
    stage_path = params.stage_path+group_name
    catalog_path = params.catalog_path+group_name
    schema_path = params.schema_path+group_name
    csv_path = params.csv_path+group_name
    db_name = params.schema_name

    print('s3_path : ' + s3_path)
    print('stage_path : ' + stage_path)
    print('catalog_path : ' + catalog_path)
    print('schema_path : ' + schema_path)
    print('csv_path : ' + csv_path)
    print('db_name : ' + db_name)

    # Parse the file name and get table name and partition
    table_name, partition = handle_partition(csv_file)
    print('table_name : ' + table_name)
    print('partition : ' + str(partition))

    # Job Run
    spark = start_spark(app_name='csv_to_parquet', master='yarn')
    job = etl_job()

    # log that main ETL job is starting
    print('etl_job is up-and-running')

    schema_flag = job.schema_check(schema_path, table_name)
    df = job.extract_data(spark, schema_path, table_name, csv_path, csv_file, schema_flag)
    job.load_data(df, stage_path, table_name, partition)
    job.move_stage_files(s3_path, stage_path, table_name, partition)
    build_athena_catalog(df, catalog_path, table_name, db_name, s3_path)

    # log the success and terminate Spark application
    print('etl_job is finished')
    spark.stop()


if __name__ == '__main__':
    time = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    print('Start Time : '+time)
    main()
    gc.collect()
    time = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    print('End Time : '+time)
