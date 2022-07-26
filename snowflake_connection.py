#!/usr/bin/env python
import snowflake.connector
from sqlalchemy import *
from snowflake.sqlalchemy import URL

# to connect with snowflake
class SnowFirst:
    sfAccount = 'du19139.ap-southeast-1'
    sfUser = 'pldhiraj1812'
    sfPswd = 'Anita$2020'
    sf_database = "AMAZON"
    sf_schema = "AMAZON_PROD"

    @staticmethod
    def connect_db():
        # create connection
        try:
            conn = snowflake.connector.connect(
                user=SnowFirst.sfUser,
                password=SnowFirst.sfPswd,
                account=SnowFirst.sfAccount,
                 warehouse = 'COMPUTE_WH',
                 database = SnowFirst.sf_database,
                 schema = SnowFirst.sf_schema
            )
            return conn
        except:
            print("failed to connect")



    @staticmethod
    def test_db_connection():
        #conn_string = f"snowflake://{SnowFirst.sfUser}:{SnowFirst.sfPswd}@{SnowFirst.sfAccount}"
        #engine = create_engine(conn_string)
        #connection = engine.connect()
        engine = create_engine(URL(
            user=SnowFirst.sfUser,
            password=SnowFirst.sfPswd,
            account=SnowFirst.sfAccount,
            warehouse='COMPUTE_WH',
            database=SnowFirst.sf_database,
            schema=SnowFirst.sf_schema,
        ))
        return engine

    @staticmethod
    def test_db_connect():
        # create connection
        try:
            conn = SnowFirst.connect_db()
            curs = conn.cursor()
            # execute SQL statement
            curs.execute('select current_date;')
            # fetch result
            print('Connecton test is successful')
            curs.close()
        except:
            print('Connecton test is failed')

    @staticmethod
    def sf_connection():

        con = SnowFirst.connect_db()
        return con



