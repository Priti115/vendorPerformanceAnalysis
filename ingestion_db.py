import pandas as pd
import os
import time
import logging
from sqlalchemy import create_engine

# Setup logging
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)


engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con=engine, index=False, if_exists='replace')

def load_raw_data():
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv') and not file.startswith('._'):
            try:
                df = pd.read_csv(os.path.join('data', file), encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(os.path.join('data', file), encoding='latin1')
            logging.info('Ingesting {file} in ib')
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start) / 60
    loggig.info('-----------------Inestion Complete-----------------')
    loggig.info('Total Time Taken : {total_time} minutes')

if __name__ == '__main__':
    load_raw_data()