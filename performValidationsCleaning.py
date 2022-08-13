

import pandas as pd
import logging
import datetime
import psycopg2
import time



# Read data from csv file
def read_data():
    try:
        data_url = "/Applications/PostgreSQL_12/data_load/concord.csv"
        publisher_df = pd.read_csv(data_url)
        return publisher_df
    except Exception as ex:
        logging.error('Exception in remove_spcl_characters' +ex.message)
        raise(ex.message)

# Remove special characters from data frame
def remove_spcl_characters(publisher_df):
    try:
        publisher_df['title'] = publisher_df['title'].str.replace(r'[^0-9a-zA-Z\s]+', '', regex=True).replace("'", '')
        publisher_df['country'] = publisher_df['country'].replace(r'[^0-9a-zA-Z\s]', '', regex=True).replace("'",'')
        publisher_df['institution'] = publisher_df['institution'].replace(r'[^0-9a-zA-Z\s]', '', regex=True).replace("'", '')
        return publisher_df
    except Exception as ex:
        logging.error(ex.message)

# Remove duplicates w.r.t all columns from data frame
def remove_duplicates(publisher_df):
    try:
        publisher_df.drop_duplicates(inplace=True)
        #publisher_df.drop_duplicates(subset='claim_uid',inplace=True)
        print(publisher_df.shape)
        return publisher_df
    except Exception as ex:
        logging.error('Exception in remove_duplicates')

# Remove rows if all column value is null
def remove_nullvalues(publisher_df):
    try:
        publisher_df=publisher_df.dropna(how = 'all')
        return publisher_df
    except Exception as ex:
        logging.error('Exception in remove_nullvalues')

# Create CSV file for dataframe
def create_csv(df, str):
    try:
        df.to_csv('/Applications/PostgreSQL_12/data_load/'+str+'.csv', index=False, header=False)
    except Exception as ex:
        logging.error('Exception in create_csv' + ex.message)

# Parameters of database
param_dic = {
    "host"      : "localhost",
    "database"  : "concord",
    "user"      : "postgres",
    "password"  : "Aspen123"
}

# Insert Claims_Data into postgresql
def insert_claims_data(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = '''copy claims_data FROM '/Applications/PostgreSQL_12/data_load/claims.csv' DELIMITER ',' CSV '''
    cursor.execute(postgreSQL_select_Query)
    conn.commit()
    print("insert successful into claims table")

# Insert Cord_Data into postgresql
def insert_cord_data(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = '''copy cord_data FROM '/Applications/PostgreSQL_12/data_load/cords.csv' DELIMITER ',' CSV '''
    cursor.execute(postgreSQL_select_Query)
    conn.commit()
    print("insert successful into cords table")

# connect to postgresql database
def connect(params_dic):
    conn = None
    try:
        conn = psycopg2.connect(**params_dic)
        print("connection successful")
        return conn
    except (Exception, psycopg2.DatabaseError) as ex:
        raise(ex)
    return conn

# Main Method to run ETL piepline
def execute_batch_job():
    try:
        # Start Job; Log the job start time
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug(datetime.datetime.now())
        # Step 1 read data
        publisher_df=read_data()
        publisher_df_shape = publisher_df.shape
        row_count = publisher_df_shape[0]
        column_count = publisher_df_shape[1]
        # Step 2 Data Cleansing
        publisher_df = remove_nullvalues(publisher_df)
        publisher_df = remove_duplicates(publisher_df)
        publisher_df = remove_spcl_characters(publisher_df)
        print(publisher_df.loc[publisher_df['cord_uid'] == "4wuv6ntb"]["title"])
        logging.debug(datetime.datetime.now())

        # Step 3 Create csv file for 2 tables-Claims_data
        claims_head=['claim_uid', 'cord_uid']
        claims_df=publisher_df[claims_head]
        claims_df = claims_df.assign(asofdt='')
        claims_df = claims_df.assign(create_dt=pd.to_datetime('now'))
        claims_df = claims_df.assign(create_user_id='kothap')
        create_csv(claims_df,'claims')

        # Step 3 Create csv file for 2 tables-Cord_data
        cord_head = ['cord_uid', 'title', 'doi', 'publish_time', 'journal', 'country', 'institution']
        cord_df = publisher_df[cord_head]
        cord_df=cord_df.assign(author_count=publisher_df['authors'].str.count(';')+1)
        cord_df=remove_duplicates(cord_df)
        print(cord_df.shape)
        create_csv(cord_df, 'cords')

        # Step 4 Connect to DB
        conn=connect(param_dic)
        # Step 5 insert cord data to DB
        insert_cord_data(conn)
        # Step 6 insert claims data to DB
        insert_claims_data(conn)

    except Exception as ex:
        logging.error('Exception in execute_batch_job'+ex.message)

#ETL Pipeline
start = time.time()
execute_batch_job()

# End job
end = time.time()
print(format((end-start)*1000))
logging.info("time elapsed to run job{} milli seconds".format((end-start)*1000))