
"""
Created on Fri Aug 10 11:18:16 2018

@author: INT625
"""


import yaml
import psycopg2
import csv
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import xlsxwriter
import json
#import jpype
#import aspose



hostname='10.66.53.49'
username='postgres'
dbname='rudra'
password='password'
port='5432'
engine = create_engine('postgresql://postgres:password@10.66.53.49:5432/rudra')


conn = psycopg2.connect(database=dbname,user=username,host=hostname,password=password,port=port)
yaml_prop=yaml.load(open (r'C:\Users\int625\Desktop/RudraInputs.yaml') )

df8  = pd.DataFrame(['Dataset Summary'])
df  = pd.DataFrame(['Column Summary'])
df1 = pd.read_sql_query("select data_type_orig,count(data_type_orig) from public.column where dataset_id = '19' group by data_type_orig ",con=engine)
df2 = pd.read_sql_query("select variable_type,count(variable_type) from public.column where dataset_id = '19' and variable_type in ('categorical','continuous','text','other') group by variable_type",con=engine)
df3 = pd.read_sql_query("select data_pattern,count(data_pattern) from public.column where dataset_id = '19' and data_pattern is not null group by data_pattern",con=engine)
df4 = pd.DataFrame(['Columns'])
df5 = pd.read_sql_query("select q2.column_name,q1.check_name from (select * from (select check_id,column_id,dataset_id from public.audit_check_exec where (status = 'failure' and dataset_id = '49' and column_id is not null)) as a inner join (select id as ch_id,name as check_name from public.check )as b on (a.check_id = b.ch_id) )as q1 inner join (select name as column_name,id as clm_id from public.column where dataset_id = '49')as q2 on q1.column_id = q2.clm_id group by q2.column_name , q1.check_name",con=engine)
df6 = pd.read_sql_query("select a.output from(select output,date, audit_id,dataset_id from public.audit_feature_exec where audit_id = '598' and feature_id = '2') as a",con=engine)
df6['row_count']=df6['output'].map(lambda x : x['row_count'])
df6['null_count']=df6['output'].map(lambda x : x['null_count'])
df6['duplicate_count']=df6['output'].map(lambda x : x['duplicate_count'])
del df6['output']
#df7 = pd.read_sql_query("select rudra_score from public.audit where id in (select max(id) from public.audit where status = 'error')",con=engine)



writer = pd.ExcelWriter('out.xlsx', engine='xlsxwriter')
df.to_excel(writer, sheet_name='Sheet1', startrow = 3,startcol =4,index = False,header=False) 
df1.to_excel(writer, sheet_name='Sheet1', startrow = 4,startcol = 1 ,index = False)  
df2.to_excel(writer, sheet_name='Sheet1', startrow = 4,startcol = 4, index = False)
df3.to_excel(writer, sheet_name='Sheet1', startrow = 4,startcol = 7, index = False)
df4.to_excel(writer, sheet_name='Sheet1', startrow = 14,startcol = 4, index = False,header =False)
df5.to_excel(writer, sheet_name='Sheet1', startrow = 16,startcol = 1, index = False)
df8.to_excel(writer, sheet_name='Sheet1', startcol =4,index = False,header=False)
df6.to_excel(writer, sheet_name='Sheet1', startrow = 1,startcol = 1, index = False)
writer.save()


#query = "select rudra_score, confidence_score,datas