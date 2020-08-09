import boto3
import pandas as pd
import os
from datetime import datetime
import athena_functions as af
import time

print("Start Time =", datetime.now().strftime('%Y/%m/%d %H:%M:%S'))

# Intialize Variables
params = {'region': 'XXXXX', 'database': 'XXXXX', 'bucket': 'XXXXX', 'path': 'XXXXX',
          'query': "XXXXX"}

session = boto3.Session(profile_name='stg')
athena_client = session.client('athena', region_name=params["region"])

# Execute Initial Schema Query
exec_id = af.athena_execute(athena_client, session, params)
print(exec_id)
time.sleep(60)
status = af.athena_status(athena_client, session, params, exec_id)
print(status)

# Get all Schema Tables from response
resp = athena_client.get_query_results(QueryExecutionId=exec_id)
l = []
for i in resp['ResultSet']['Rows']:
    if i['Data'][0]['VarCharValue'] != 'TABLE_NAME':
        l.append(i['Data'][0]['VarCharValue'])
df = pd.DataFrame(l, columns = ['TABLE_NAME'])

# Start Lake Formation Client
client = boto3.client('lakeformation')
for index, row in df.iterrows():
    print(row[0])
    response = client.grant_permissions(
        Principal={'DataLakePrincipalIdentifier': 'arn:aws:iam::976432587531:'+'ROLEXXXX'},
        Resource={'Table': {'DatabaseName': params['database'], 'Name': row[0]}}, \
        Permissions=['SELECT'], \
        PermissionsWithGrantOption=['SELECT']
        )
# print(response)
print("End Time =", datetime.now().strftime('%Y/%m/%d %H:%M:%S'))