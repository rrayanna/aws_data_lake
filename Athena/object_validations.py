import boto3
import pandas as pd
import os
from datetime import datetime
import athena_functions as af
import time

print("Start Time =", datetime.now().strftime('%Y/%m/%d %H:%M:%S'))

# Intialize Variables
params = {'region': XXXXX, 
'database': XXXXX 
'bucket': XXXXX, 
'path': XXXXX,
'query': XXXXX}

session = boto3.Session(profile_name='xxx')
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
df['Query'] = df.apply(lambda x: f"SELECT * FROM {params['database']}.{x['TABLE_NAME']} LIMIT 10;", axis=1)

# Submit table selects for each table and get execution ids
x = []
for index, row in df.iterrows():
    params['query'] = row[1]
    execution_id = af.athena_execute(athena_client, session, params)
    dt = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    x.append([row[0], execution_id, dt])
    time.sleep(1)
df1 = pd.DataFrame(x, columns = ['TABLE_NAME', 'Exec_ID', 'Time'])

time.sleep(60)

# Get reponses of the queries for each execution ids
y = []
for index, row in df1.iterrows():
    execution_id = row[1]
    status = af.athena_status(athena_client, session, params, execution_id)
    dt = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    if status == 'FAILED' or status == 'None':
        y.append([row[0], status, dt])
df2 = pd.DataFrame(y, columns = ['TABLE_NAME', 'Status', 'Time']) 

# Save all to a csv file
os.chdir('/Output/Athena')
outputfilename = 'Athena_Status_'+datetime.now().strftime('%Y%m%d')+'.csv'
df2.to_csv(outputfilename, encoding='utf-8', index=False)

print("End Time =", datetime.now().strftime('%Y/%m/%d %H:%M:%S'))