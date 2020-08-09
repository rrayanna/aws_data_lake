import os
import pandas as pd
import cx_Oracle
from datetime import datetime

print("Start Time =", datetime.now().strftime('%Y/%m/%d %H:%M:%S'))

# Oracle Prod Config
dsn_tns = cx_Oracle.makedsn('XXXXXX', '1521', service_name='XXXXX') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
conn = cx_Oracle.connect(user=r'XXXXX', password='XXXX', dsn=dsn_tns) # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'

os.chdir('XXXX')
df = pd.read_excel('Input/Oracle/Oracle_Queries.xlsx')

cur1 = conn.cursor()
cur2 = conn.cursor()
l = []

for index, row in df.iterrows():
    t_cur = cur1.execute(row['QRY_1'])
    d_cur = cur2.execute(row['QRY_2'])
    t_cnt = t_cur.fetchone()[0]
    d_cnt = d_cur.fetchone()[0]
    diff = int(t_cnt)-int(d_cnt)
    if t_cnt == d_cnt:
        valid = True 
    else:
        valid = False
    l.append([row['OWNER'], row['TABLE_NAME'], t_cnt, d_cnt, diff, valid])
    
conn.close()
df1 = pd.DataFrame(l, columns = ['OWNER', 'TABLE_NAME', 'TOTAL_CNT', 'DISTINCT_KEY_CNT', 'DIFF', 'VALID'])
df1.to_excel('Output/Oracle/Oracle_Validation_'+datetime.now().strftime('%Y%m%d')+'.xlsx', index = False)

print("End Time =", datetime.now().strftime('%Y/%m/%d %H:%M:%S'))
