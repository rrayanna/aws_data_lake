import sys
import boto3
import pandas as pd
from datetime import datetime

from lake_formation import lake_formation as lf
from config import dl_mapping

pd.options.mode.chained_assignment = None  # default='warn'


def main():

    # session = boto3.Session(profile_name=conf['aws.env'])
    session = boto3.Session(profile_name=dl_mapping.profile)

    # Open the required athena and lake formation clients
    athena_client = session.client('athena', region_name=aws.region)
    lf_client = session.client('lakeformation', region_name=aws.region)

    if dl_mapping.action == 's':                 # re-map to sys parameters
        df = lf.get_results(athena_client, params)
        df1 = lf.list_access(lf_client, df)
        for role, account in dl_mapping.lf_mapping.items():
            print(role, account)
            df2 = df1[(df1.ROLE == account[0]) & (df1.TYPE == 'role')]
            df2.ROLE = role
            df2.TYPE = account[1]
            df2.LEVEL = account[2]
            if 'BDE' in role and 'BDEAdmin' in account[0]:
                df2.ACCESS = "['SELECT']"
            df2['ACCESS'] = df2['ACCESS'].astype('str')
            lf.grant_access(lf_client, df2, aws)

    if dl_mapping.action == 'l':                 # re-map to sys parameters
        df = pd.read_csv(dl_mapping.csv_file)    # re-map to sys parameters
        df1 = lf.list_access(lf_client, df)
        outputfile = 'LakeFormation_Access_' + datetime.now().strftime('%Y%m%d') + '.csv'
        df1.to_csv('/' + outputfile,
                   encoding='utf-8', index=False)

    if dl_mapping.action == 'g':                 # re-map to sys parameters
        df = pd.read_csv(dl_mapping.csv_file)    # re-map to sys parameters
        lf.grant_access(lf_client, df, aws)

    if dl_mapping.action == 'r':                 # re-map to sys parameters
        df = pd.read_csv(dl_mapping.csv_file)    # re-map to sys parameters
        lf.revoke_access(lf_client, df, aws)


if __name__ == '__main__':

    time = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    print('Start Time : ' + time)

    aws = dl_mapping.aws
    arn = aws.arn
    print(arn)

    params = {
        'region': aws.region,
        'database': aws.database,
        'bucket': aws.s3_bucket,
        'path': aws.queryResults,
        'query': aws.query,
        'WorkGroup': aws.workGroup
        }

    main()
    
    time = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    print('End Time : ' + time)
