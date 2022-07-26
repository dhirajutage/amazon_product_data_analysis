import logging
logging.basicConfig()
#from load_landing_reviews import *
from datetime import date
import load_landing_metadata
import load_landing_reviews
import utilis.truncate_table
from load_landing_metadata import *
from apscheduler.schedulers.blocking import BlockingScheduler

#
date = date.today()

def run_daily_load():
    ####################################################################################
    # step 1
    #Split the large files into smaller chunks to process faster
    #I have used below unix command to do this task ...this can be done by python or unix
    #   1>  zcat metadata.json.gz | split -l 400000 --filter='gzip > $FILE.gz'
    #   2>  zcat items_dedup.json.gz | split -l 900000 --filter='gzip > $FILE.gz'
    #####################################################################################

    ####################################################################################
    # step 2
    # truncate and load  landing table  lnd_metadata
    ####################################################################################

    print('Data load started for ' + str(date))

    try:
        utilis.truncate_table.trunc_table('lnd_metadata')
        # load lnd_metadata
        load_landing_metadata.execute_load()
    except:
        print('Load landing table - lnd_metadata fail')



    ########################################################################################
    # step 3
    #  truncate  and load landing table lnd_metadata
    ########################################################################################

    try:
        utilis.truncate_table.trunc_table('lnd_reviews')

        #load lnd_metadata
        load_landing_reviews.execute_load()
    except:
        print('Load landing table - lnd_reviews fail')

    ########################################################################################
    # step 4
    #   load reviewer_dim table
    ########################################################################################

    # load stage table - reviewer_dim
    try:
        conn = SnowFirst.connect_db()
        cursor = conn.cursor()
        cursor.execute("call amazon.amazon_prod.proc_dim_reviewer_ld();")
        conn.close()
    except:
        print('Load stage table - reviewer_dim fail')

    ########################################################################################
    # step 5
    #   load product_category_dim table
    ########################################################################################

    # load  table- product_category_dim
    try:
        conn = SnowFirst.connect_db()
        cursor = conn.cursor()
        cursor.execute("call amazon.amazon_prod.proc_dim_reviewer_ld();")
        conn.close()
    except:
        print('Load stage table - product_category_dim fail')

    ########################################################################################
    # step 6
    #   load product_category_dim table
    ########################################################################################

    #  load  table- proc_dim_product_ld
    try:
        conn = SnowFirst.connect_db()
        cursor = conn.cursor()
        cursor.execute("call amazon.amazon_prod.proc_dim_product_ld();")
        conn.close()
    except:
        print('Load stage table - product_category_dim fail')
    ########################################################################################
    # step 6
    #   load product_category_dim table
    ########################################################################################
    # load table
    try:
        conn = SnowFirst.connect_db()
        cursor = conn.cursor()
        cursor.execute("call amazon.amazon_prod.proc_fact_reviews_ld();")
        conn.close()
    except:
        print('Load stage table - product_category_dim fail')

    print('Data load started for ' + str(date))

    return 0

if __name__ == "__main__":
    scheduler = BlockingScheduler()



    ########################################################################################
    # step
    #  # start the task daily at 5 pm
    ########################################################################################
    scheduler.add_job(run_daily_load, 'cron', hour=17)

    scheduler.start()









