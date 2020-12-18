from awscli.clidriver import create_clidriver

def move_files(source_path, target_path):
    """
    remove all files in target path
    move all files in source path to the target path
    input - source path, target path
    output - none
    """
    driver = create_clidriver()
    s3rm = 's3 rm '+target_path+' --recursive'
    s3copy = 's3 cp '+source_path+' '+target_path+' --recursive'
    driver.main(s3rm.split())
    driver.main(s3copy.split())
    print('S3 files move complete')
    return None

def build_athena_catalog(df, file_path, file_name, db_name, s3_path):
    """
    generate athena catalog for the
    input - dataframe, catalog path, catalog file name, s3 path, db name
    output - text file with catalog file name
    """
    i = 1
    f = open(file_path+'%s.txt' % file_name, 'w')
    f.write('CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (' % (db_name, file_name))
    for col, typ in df.dtypes:
        sep = "," if i < len(df.dtypes) else ""
        f.write(' %s %s%s' % (col, typ, sep))
        i += 1
    f.write(')')
    f.write(" ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT "
            "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT "
            "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION '%s%s'" % (s3_path, file_name))
    f.close()
    print('athena catalog build complete')
    return None