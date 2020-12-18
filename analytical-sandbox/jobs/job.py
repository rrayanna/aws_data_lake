import subprocess
from utils import is_int
from aws import aws_functions


class etl_job():


    def schema_check(self, schema_path, table_name):
        """Check if schema file exists or not.
        :param schema_path: file path, table: table name
        :return: Proc Return Code.
        """
        proc = subprocess.Popen(['hadoop', 'fs', '-test', '-e', schema_path + table_name + '.csv'])
        proc.communicate()
        return proc.returncode


    def extract_data(self, spark, schema_path, table_name, path, file, flag):
        """Load data from csv file format.
        :param spark: Spark session object.
        :param path: file path, file: file name
        :return: Spark DataFrame.
        """
        if flag == 0:
            dfs = spark.read.format('com.databricks.spark.csv') \
                .options(inferSchema='true', header='true', timestampFormat='yyyy/MM/dd hh:mm:ss') \
                .load(schema_path + table_name + '.csv')
            df = spark.read.format('com.databricks.spark.csv') \
                .options(header='true', timestampFormat='yyyy/MM/dd hh:mm:ss', quote='"',
                         multiLine='true', seperator=',', escape='\"') \
                .schema(dfs.schema) \
                .load(path + file)
        else:
            df = spark.read.format('com.databricks.spark.csv') \
                .options(inferSchema='true', header='true', timestampFormat='yyyy/MM/dd hh:mm:ss',
                         quote='"', multiLine='true', seperator=',', escape='\"') \
                .load(path + file)

        return df


    def load_data(self, df, stage_path, table_name, partition):
        """Collect data locally and write to parquet.
        :param df: DataFrame.
        :param stage_path: stage file path, table_name: table name
        :return: None
        """
        # write to Parquet file format
        if is_int(partition):
            df.repartition(4).write.parquet(stage_path + table_name + '/' + str(partition), mode='overwrite')
        else:
            df.repartition(4).write.parquet(stage_path + table_name, mode='overwrite')
        return None


    def move_stage_files(self, s3_path, stage_path, table, partition):
        if is_int(partition):
            source = stage_path + table + '/' + str(partition) + '/'
            target = s3_path + table + '/' + str(partition) + '/'
            aws_functions.move_files(source, target)
        else:
            source = stage_path + table + '/'
            target = s3_path + table + '/'
            aws_functions.move_files(source, target)
        return None
