import cleaning
import db
import M1
import run_producer as pr
import consumer as con

LOOKUP_TABLENAME = 'lookup_fintech_data_MET_P02_52_20136'
DATA_TABLENAME = 'fintech_data_MET_P02_52_20136_clean'
KAFKA_URL = 'kafka:9092'
TOPIC = 'fintech-topic'
DATA_DIR = './data/'

def get_and_save_data():
    import os, pandas as pd
    if os.path.exists(f"{DATA_DIR}{DATA_TABLENAME}.parquet") and os.path.exists(f"{DATA_DIR}lookup_table.csv"):
        print("Reading data from parquet file. Clean Data already exists.")
        df = M1.read_parquet_file(f"{DATA_DIR}{DATA_TABLENAME}.parquet")
        lookup = pd.read_csv(f"{DATA_DIR}lookup_table.csv")
    else:
        print("Running the cleaning pipeline. Because ONE OR BOTH of the clean data or lookup data does not exist.")
        df = cleaning.get_cleaned_dataset()
        M1.save_cleaned_dataset_to_parquet(df, DATA_TABLENAME, DATA_DIR)
        M1.save_lookup_table(M1.lookup_table, DATA_DIR)
        lookup = M1.lookup_table
    db.save_to_db(df, False, DATA_TABLENAME)
    db.save_to_db(lookup, False, LOOKUP_TABLENAME)
    return df





if __name__ == '__main__':
    print("Hello World\nHi")

    df = get_and_save_data()

    id = pr.start_producer('52_20136', KAFKA_URL, TOPIC)
    print(f'Producer started: {id}')
    streamed_df = con.run_consumer(KAFKA_URL, TOPIC)
    print('Consumer stopped.')
    pr.stop_container(id)
    print('Producer stopped.')

    # db.save_to_db(streamed_df, append=True, tablename=DATA_TABLENAME, subset=db.FULL_SCHEMA)

