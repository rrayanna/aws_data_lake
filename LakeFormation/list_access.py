import boto3
import pandas as pd
import os
from datetime import datetime
import athena_functions as af

print("Start Time =", datetime.now().strftime('%Y/%m/%d %H:%M:%S'))

# Intialize Variables
session = boto3.Session(profile_name='xxx')

params = {'region': 'XXXXX', 'database': 'XXXXX', 'bucket': 'XXXXX', 'path': 'XXXXX',
          'query': "XXXXX"}

srcFileName = af.athena_get_schema_tables(session, params)
print(srcFileName)

s3 = boto3.client('s3')
obj = s3.get_object(Bucket=params['bucket'], Key=params['path']+srcFileName)
df = pd.read_csv(obj['Body'])

y = []
for index, row in df.iterrows():
    client = boto3.client('lakeformation')
    response = client.list_permissions(ResourceType='TABLE', 
                            Resource={'Table': {'DatabaseName': row[0], 'Name': row[1]}})
    permissions = response['PrincipalResourcePermissions']
    for index in range(len(permissions)):
        x = []
        for key in permissions[index]:
            results = permissions[index][key]
            if isinstance(results, dict) and key == 'Principal':
                Principal = str(list(results.values())[0])
                Principal = Principal.rsplit('/', 1)[-1]            
            if isinstance(results, list) and len(results) > 0:
                x.append(results)
        Access = list(set([item for sublist in x for item in sublist]))
        dt = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
        y.append([row[0], row[1], Principal, Access, dt])

df1 = pd.DataFrame(y, columns = ['SCHEMA', 'TABLE_NAME', 'ROLE', 'ACCESS', 'AS_OF_DATE'])

os.chdir('/Output/')
outputfile = 'list_acess_'+datetime.now().strftime('%Y%m%d')+'.csv'
df1.to_csv(outputfile, encoding='utf-8', index=False)
print("End Time =", datetime.now().strftime('%Y/%m/%d %H:%M:%S'))