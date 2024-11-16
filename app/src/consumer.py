import pandas as pd
from kafka import KafkaConsumer
import json
import time
from datetime import datetime
import main
import cleaning as cl
import db

READ_COLS = ['timestamp','Loan Id','Customer Id','Emp Title','Emp Length','Home Ownership','Annual Inc','Annual Inc Joint','Verification Status','Zip Code','Addr State','Avg Cur Bal','Tot Cur Bal','Loan Status','Loan Amount','State','Funded Amount','Term','Int Rate','Grade','Issue Date','Pymnt Plan','Type','Purpose','Description']

def run_consumer(kafka_url:str=None, topic:str=None) -> pd.DataFrame:
    if not kafka_url:
        kafka_url = main.KAFKA_URL
    if not topic:
        topic = main.TOPIC
    df = pd.DataFrame(columns=READ_COLS)
    cleaned_data = None

    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_url,
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Listening for messages in '{topic}'...")


    print("Listening for messages...")

    while True:
        message = consumer.poll(timeout_ms=2000)  
        
        if message:
            foundEOF = False
            for tp, messages in message.items():
                for msg in messages:
                    print(f"Received: {msg.value}")

                    # if the message value is EOF, then stop listening
                    if msg.value == 'EOF':
                        print("Received EOF, stopping consumer...")
                        foundEOF = True
                        break
                    
                    msg.value['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    new_row = pd.DataFrame([msg.value], columns=READ_COLS)
                    df = pd.concat([df, new_row], ignore_index=True)
                    # ------------------------------------------------------------test
                    # print("\n"*3, "raw data", new_row, "\n"*3, '-'*50)
                    # # new_row['Emp Title'] = None  ## to test the cleaning function

                    # print("\n"*3, "cleaned data", cl.clean_row(new_row), "\n"*3, '-'*50)
                    # ------------------------------------------------------------
                    new_row_cleaned = cl.clean_row(new_row)
                    if cleaned_data is None:
                        cleaned_data = new_row_cleaned
                    else:
                        cleaned_data = pd.concat([cleaned_data, new_row_cleaned], ignore_index=True)

                    db.save_to_db(new_row_cleaned, append=True, tablename=main.DATA_TABLENAME, subset=db.FULL_SCHEMA)


                if foundEOF:
                    break
            if foundEOF:
                break
        else:
            print("No messages received, polling again...")

    consumer.close()

    print("Consumer stopped.")
    print(f"Received {len(df)} messages. raw data:")
    print(df.head())

    # df.to_csv(f'{main.DATA_DIR}/{int(time.time())}-output.csv', index=False)

    return df,cleaned_data