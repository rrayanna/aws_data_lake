import re


def handle_partition(file_name):
    f = re.split("[.]", file_name)[0]
    if f[f.rindex('_')+1:].isdigit():
        table_name = f[0:f.rindex('_')]
        partition = f[f.rindex('_')+1:]
        return table_name, int(partition)
    else:
        return f, float('NaN')


def is_int(val):
    if type(val) == int:
        return True
    else:
        return False
