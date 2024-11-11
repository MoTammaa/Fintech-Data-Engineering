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
    df = cleaning.get_cleaned_dataset()
    db.save_to_db(df, False, DATA_TABLENAME)
    db.save_to_db(M1.lookup_table, False, LOOKUP_TABLENAME)





if __name__ == '__main__':
    print("Hello World\nHi")

    df = get_and_save_data()

    id = pr.start_producer('52_20136', KAFKA_URL, TOPIC)
    print(f'Producer started: {id}')
    con.run_consumer(KAFKA_URL, TOPIC)
    print('Consumer stopped.')
    pr.stop_container(id)
    print('Producer stopped.')


