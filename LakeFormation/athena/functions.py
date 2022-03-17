import time
import pandas as pd


def athena_query(client, params):
    response = client.start_query_execution(
        QueryString=params["query"],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        },
        WorkGroup=params['WorkGroup']
    )
    return response


def athena_execute(client, params):
    execution = athena_query(client, params)
    execution_id = execution['QueryExecutionId']
    return execution_id


def athena_status(client, execution_id, max_execution = 10):
    response = client.get_query_execution(QueryExecutionId = execution_id)
    state = 'RUNNING'
    while (max_execution > 0 and state in ['RUNNING']):
        print(max_execution)
        max_execution = max_execution - 1
        response = client.get_query_execution(QueryExecutionId = execution_id)
        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if (state == 'FAILED') or (state == 'CANCELLED'):
                return False
            elif state == 'SUCCEEDED':
                return state
        print(state)
        time.sleep(60)
    else:
        return state


def format_result(results):
    '''
    This function format the results toward append in the needed format.
    '''
    columns = [
        col['Label']
        for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']
    ]

    formatted_results = []

    for result in results['ResultSet']['Rows'][0:]:
        values = []
        for field in result['Data']:
            try:
                values.append(list(field.values())[0])
            except:
                values.append(list(''))

        formatted_results.append(
            dict(zip(columns, values))
        )
    return formatted_results


def paginate_results(client, exec_id):
    marker = None
    formatted_results = []
    i = 0
    start_time = time.time()

    while True:
        paginator = client.get_paginator('get_query_results')
        response_iterator = paginator.paginate(
            QueryExecutionId=exec_id,
            PaginationConfig={
                'MaxItems': 1000,
                'PageSize': 1000,
                'StartingToken': marker})
        for page in response_iterator:
            i = i + 1
            format_page = format_result(page)
            if i == 1:
                formatted_results = pd.DataFrame(format_page)
            elif i > 1:
                formatted_results = formatted_results.append(pd.DataFrame(format_page))
        try:
            marker = page['NextToken']
        except KeyError:
            break

    print("Pagination took", time.time() - start_time, "to run")

    return formatted_results
