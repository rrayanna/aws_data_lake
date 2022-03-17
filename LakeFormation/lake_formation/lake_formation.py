import ast
import argparse
import pandas as pd
from datetime import datetime
from athena import functions as af


def read_config(config_file):
    """Read configuration into variables"""
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
             description="Lake Formation Security Arguments")

    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    required.add_argument('-cf',
                          dest="config_file",
                          help="path to Lake Formation configuration file",
                          required=True)
    required.add_argument('-a',
                          dest="action",
                          help="possible values : g-grant, r-revoke, l-list access",
                          required=True)
    required.add_argument('-af',
                          dest="action_file",
                          help="path to the action file to grant, revoke or list access",
                          required=True)
    args = parser.parse_args()
    return args


def get_results(client, params):
    execution_id = af.athena_execute(client, params)
    status = af.athena_status(client, execution_id)
    print(status)
    df = af.paginate_results(client, execution_id)
    return df


def grant_access(client, df, conf):
    """Grant Access"""
    for idx, row in df.iterrows():
        # print('Schema : ' + row[0] + '; Table : ' + row[1])
        if row[6] != 'quicksight':
            client.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': f"arn:aws:iam::{row[2]}:" + row[3] + '/' + row[4]},
                Resource={'Table': {'DatabaseName': row[0], 'Name': row[1]}},
                Permissions=ast.literal_eval(row[5])
            )
        else:
            client.grant_permissions(
                Principal={'DataLakePrincipalIdentifier': f"arn:aws:{row[6]}:{conf.region}:{row[2]}:"
                                                          + row[3] + '/' + row[4]},
                Resource={'Table': {'DatabaseName': row[0], 'Name': row[1]}},
                Permissions=ast.literal_eval(row[5])
            )


def revoke_access(client, df, conf):
    """Revoke Access"""
    for idx, row in df.iterrows():
        # print('Schema : ' + row[0] + ' Table : ' + row[1])
        if row[6] != 'quicksight':
            client.revoke_permissions(
                Principal={'DataLakePrincipalIdentifier': f"arn:aws:iam::{row[2]}:"+row[3]+'/'+row[4]},
                Resource={'Table': {'DatabaseName': row[0], 'Name': row[1]}},
                Permissions=ast.literal_eval(row[5])
            )
        else:
            client.revoke_permissions(
                Principal={'DataLakePrincipalIdentifier': f"arn:aws:{row[6]}:{conf.region}:{row[2]}:"
                                                          + row[3] + '/' + row[4]},
                Resource={'Table': {'DatabaseName': row[0], 'Name': row[1]}},
                Permissions=ast.literal_eval(row[5])
            )


def list_access(client, df):
    """
    :param client: lake formation client
    :param df: schema and tables dataframe
    :return: dataframe with current lake formation access
    """
    y = []
    for index, row in df.iterrows():
        # print('Schema Name : ' + row[0] + ', Table Name : ' + row[1])
        response = client.list_permissions(ResourceType='TABLE',
                                           Resource={'Table': {'DatabaseName': row[0],
                                                               'Name': row[1]}})
        permissions = response['PrincipalResourcePermissions']
        for index in range(len(permissions)):
            x = []
            for key in permissions[index]:
                results = permissions[index][key]
                if isinstance(results, dict) and key == 'Principal' and \
                    str(list(results.values())[0]) != 'IAM_ALLOWED_PRINCIPALS':
                    Principal = str(list(results.values())[0])
                    ARN = Principal.rsplit('/', 1)[-2].rsplit(':', 1)[-2].rsplit(':', 1)[-1]
                    Type = Principal.rsplit('/', 1)[-2].rsplit(':', 1)[-1]
                    Role = Principal.rsplit('/', 1)[-1]
                    Level = 'iam' if Type in ['user', 'role'] else 'quicksight'
                if isinstance(results, list) and len(results) > 0:
                    x.append(results)
            Access = list(set([item for sublist in x for item in sublist]))
            dt = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            y.append([row[0], row[1], ARN, Type, Role, Access, Level, dt])
    df1 = pd.DataFrame(y, columns=['SCHEMA', 'TABLE_NAME', 'ARN', 'TYPE', 'ROLE', 'ACCESS', 'LEVEL', 'AS_OF_DATE'])
    return df1