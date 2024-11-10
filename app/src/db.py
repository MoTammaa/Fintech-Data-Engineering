from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import pandas as pd

engine : Engine = create_engine('postgresql://root:root@pgdatabase:5432/fintechdb')

def save_to_db(cleaned : pd.DataFrame, append : bool):
    if(engine.connect()):
        print('Connected to Database')
        try:
            print('Writing cleaned dataset to database')
            cleaned.to_sql('cleaned_db', con=engine, if_exists=('fail' if not append else 'append'))
            print('Done writing to database')
        except ValueError as vx:
            print('Cleaned Table already exists.')
        except Exception as ex:
            print(ex)
    else:
        print('Failed to connect to Database')

