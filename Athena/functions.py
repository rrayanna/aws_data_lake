import re
import time

def athena_query(client, params):    
    response = client.start_query_execution(
        QueryString=params["query"],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response

def athena_get_schema_tables(client, session, params, max_execution = 5):
    # client = session.client('athena', region_name=params["region"])
    execution = athena_query(client, params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'
    time.sleep(60)
    while (max_execution > 0 and state in ['RUNNING']):
        max_execution = max_execution - 1
        response = client.get_query_execution(QueryExecutionId = execution_id)
        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                return False
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                filename = re.findall('.*\/(.*)', s3_path)[0]
                return filename
        time.sleep(1)
    return False

def athena_execute(client, session, params):
    # client = session.client('athena', region_name=params["region"])
    execution = athena_query(client, params)
    execution_id = execution['QueryExecutionId']
    return execution_id
    
def athena_status(client, session, params, execution_id, max_execution = 5):
    # client = session.client('athena', region_name=params["region"])
    state = 'RUNNING'
    while (max_execution > 0 and state in ['RUNNING']):
        max_execution = max_execution - 1
        response = client.get_query_execution(QueryExecutionId = execution_id)
        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                return state
            elif state == 'SUCCEEDED':
                return state

# Deletes all files in your path so use carefully!
def cleanup(session, params):
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(params['bucket'])
    for item in my_bucket.objects.filter(Prefix=params['path']):
        item.delete()