"""
Python Program converts CSV to Parquet. Steps involved are:
    1. Reads CSV File to a Spark Dataframe
        a. Handle Partition file names
    2. Write to parquet file using schema
    3. Build a athena catalog
"""
import os
import re
import sys
import gc

from pyspark.sql import SparkSession
import awscli
from awscli.clidriver import create_clidriver
from datetime import datetime
import subprocess


def handle_partition_files(f):
    """
    Remove partitons added to file name while catlog build 

    Parameters
    ----------
    f : string (file name).

    Returns
    -------
    string (file name).

    """
    if re.search(r'\d{6}', f):
        return f[0:len(f)-7]
    else:
        return f
   
def get_partition(f):
    if re.search(r'\d{6}', f):
        return int(f[len(f)-6:])
    else:
        return f

def is_int(val):
    if type(val) == int:
        return True
    else:
        return False

def move_files(s, t):
# Move Parquet file from s3//stage to s3//file
	driver = create_clidriver()
	s3rm = 's3 rm '+t+' --recursive'
	s3copy = 's3 cp '+s+' '+t+' --recursive'
	driver.main(s3rm.split())
	driver.main(s3copy.split())
		
def build_athena_catalog(dataframe):
    """
    Build Catalog as a txt file for AWS Athena 

    Parameters
    ----------
    dataframe : Dataframe

    Returns
    -------
    None.

    """
   # Writes to a text file
    i = 1
    f = open(catalog_path+'%s.txt' % catalog_file, 'w')
    f.write('CREATE EXTERNAL TABLE IF NOT EXISTS ptledw_playarea.%s (' % catalog_file)
    for col, typ in dataframe.dtypes:
        sep = "," if i < len(dataframe.dtypes) else ""
        f.write(' %s %s%s' % (col, typ, sep))
        i += 1
    f.write(')')
    f.write(" ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://path%s'" % catalog_file)
    f.close()
    
print("Start Time =", datetime.now().strftime('%Y/%m/%d %H:%M:%S'))

# Input Parameters
csv_path = '/user/hadoop/input/'
csv_file = sys.argv[1] # Input CSV File as a Parameter
	
parquet_path = 's3:path'
parquet_folder = handle_partition_files(re.split("[.]", csv_file)[0])
parquet_partition = get_partition(re.split("[.]", csv_file)[0])
	
catalog_path = '/output/catalogs/'
catalog_file = handle_partition_files(re.split("[.]", csv_file)[0]) # Generate Output Catalog File

schema_path = '/input/schema/'
temp_path = '/input/temp/'

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("csv_to_parquet") \
    .master("yarn") \
    .config("dfs.client.read.shortcircuit.skip.checksum", "true") \
    .getOrCreate()


# Process CSV Files
proc = subprocess.Popen(['hadoop', 'fs', '-test', '-e', schema_path+catalog_file+'.csv'])
proc.communicate()
if (proc.returncode == 0) and is_int(parquet_partition):
    print('Step 1')
    dfs = spark.read.format('com.databricks.spark.csv').options(inferSchema = 'true', header = 'true', timestampFormat = 'yyyy/MM/dd hh:mm:ss').load(schema_path+catalog_file+'.csv')
    df = spark.read.format('com.databricks.spark.csv').options(header = 'true', timestampFormat = 'yyyy/MM/dd hh:mm:ss', quote='"',  multiLine = 'true', seperator = ',', escape = '\"').schema(dfs.schema).load(csv_path+csv_file)
    df.repartition(4).write.mode('overwrite').parquet(parquet_path+parquet_folder+'/'+str(parquet_partition))
    s = parquet_path+parquet_folder+'/'+str(parquet_partition)+'/'
    t = 's3://path/'+parquet_folder+'/'+str(parquet_partition)+'/'
    move_files(s, t)

elif (proc.returncode == 0) and not is_int(parquet_partition):
    print('Step 2')
    dfs = spark.read.format('com.databricks.spark.csv').options(inferSchema = 'true', header = 'true', timestampFormat = 'yyyy/MM/dd hh:mm:ss').load(schema_path+catalog_file+'.csv')
    df = spark.read.format('com.databricks.spark.csv').options(header = 'true', timestampFormat = 'yyyy/MM/dd hh:mm:ss', quote='"',  multiLine = 'true', seperator = ',', escape = '\"').schema(dfs.schema).load(csv_path+csv_file)
    df.repartition(4).write.mode('overwrite').parquet(parquet_path+parquet_folder)
    s = parquet_path+parquet_folder+'/'
    t = 's3://path/'+parquet_folder+'/'
    move_files(s, t)

elif (proc.returncode != 0) and is_int(parquet_partition):
    print('Step 3')
    df = spark.read.format('com.databricks.spark.csv').options(inferSchema = 'true', header = 'true', timestampFormat = 'yyyy/MM/dd hh:mm:ss', quote='"',  multiLine = 'true', seperator = ',', escape = '\"').load(csv_path+csv_file)
    df.repartition(4).write.mode('overwrite').parquet(parquet_path+parquet_folder+'/'+str(parquet_partition))
    s = parquet_path+parquet_folder+'/'+str(parquet_partition)+'/'
    t = 's3://path/'+parquet_folder+'/'+str(parquet_partition)+'/'
    move_files(s, t)
else:
    print('Step 4')
    df = spark.read.format('com.databricks.spark.csv').options(inferSchema = 'true', header = 'true', timestampFormat = 'yyyy/MM/dd hh:mm:ss', quote='"',  multiLine = 'true', seperator = ',', escape = '\"').load(csv_path+csv_file)
    df.repartition(4).write.mode('overwrite').parquet(parquet_path+parquet_folder)
    s = parquet_path+parquet_folder+'/'
    t = 's3://path/'+parquet_folder+'/'
    move_files(s, t)	

# Build Athena Catalog
build_athena_catalog(df)
spark.stop()
gc.collect()

print("End Time =", datetime.now().strftime('%Y/%m/%d %H:%M:%S'))