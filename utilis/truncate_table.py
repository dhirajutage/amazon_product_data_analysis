from snowflake_connection import *
from datetime import date


def trunc_table( in_table_name):
    connection = SnowFirst.connect_db()
    date_id = date.today()
    cursor = connection.cursor()

    cursor.execute('use database amazon;')
    cursor.execute('use schema amazon_prod;')

    sql = "truncate table amazon.amazon_prod.{0}  ;".format(in_table_name)
    # print(sql)

    cursor.execute(sql)
    connection.close()