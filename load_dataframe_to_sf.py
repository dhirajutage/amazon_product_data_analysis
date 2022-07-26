import pandas as pd
import gzip
from snowflake_connection import *
from snowflake.connector.pandas_tools import *


class data_load:

    def __init__(self, file_name, table_name):
        self.file_name = file_name
        self.table_name = table_name

    def parse(self, file_name):
        try:
            g = gzip.open(file_name, 'rb')
            for l in g:
                yield eval(l)
        except EnvironmentError:
            print('Failed to open file')
        except IOError:
            print('File not found')

    # get data frame from dict
    def getDF(self):
        i = 0
        df = {}
        for d in self.parse(self.file_name):
            # if i < 2:
            df[i] = d
            i += 1
            # else:
            #    break
        return pd.DataFrame.from_dict(df, orient='index')

    # load dataframe to database table
    def df_load(self):
        try:
            df = self.getDF()
            df.columns = map(lambda x: str(x).upper(), df.columns)
            # engine = SnowFirst.test_db_connection()

            # df.to_sql(self.table_name, engine, if_exists='append', index=False,method=functools.partial(pd_writer,
            # quote_identifiers=False))

            cnx = SnowFirst.connect_db()
            success, nchunks, nrows, _ = write_pandas(cnx, df, self.table_name, parallel=9, quote_identifiers=False)
            return success, nrows
        except:
            print('failed to load ' + self.table_name)
