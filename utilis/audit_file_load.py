from snowflake_connection import *
from datetime import date

def insert_file_load(source,table_name,layer,status,no_of_rows,in_file_name):
    connection = SnowFirst.connect_db()
    date_id = date.today()
    sql = """ MERGE INTO amazon.amazon_prod.file_load tar USING 
              (SELECT '{0}' source ,'{1}' date_id,'{2}' table_name,
              '{3}' layer,'{4}' status,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP, '{5}'no_of_rows,'{6}' in_file_name ) src
              ON tar.source = src.source
              and tar.table_name = src.table_name
              and tar.layer = src.layer
              and tar.date_id = src.date_id
              and trim(tar.file_name) = trim(in_file_name)
               when matched then
               update set tar.end_time = current_timestamp,
                   tar.status = src.status,
                   tar.processed_rows = src.no_of_rows
               when not matched then
               insert  (source ,date_id,table_name,layer,status,start_time,end_time,processed_rows,file_name)
               values( source ,date_id,table_name,layer,status,current_timestamp,current_timestamp,no_of_rows,in_file_name)
               
               
              """.format(source,date_id,table_name,layer,status,no_of_rows,in_file_name)
    #print(sql)
    cursor = connection.cursor()
    cursor.execute(sql)
