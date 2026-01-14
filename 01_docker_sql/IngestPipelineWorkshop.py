#!/usr/bin/env python
# coding: utf-8

#get_ipython().system('uv add pandas')
#get_ipython().system('uv add sqlalchemy')
#get_ipython().system('uv add psycopg2-binary')
#get_ipython().system('uv add tqdm')
#get_ipython().system('uv add pyarrow')

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import time
import click

#Urls:
prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
url = f'{prefix}/green_tripdata_2025-11.parquet'

prefix_2 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc'
url_2 = f'{prefix_2}/taxi_zone_lookup.csv'

#Parameters main(): 
user = 'postgres'
pwd = 'postgres'
host = 'localhost'
port = 5433
db = 'ny_taxi'
t1_name = 'ny_green_taxi_trips'
t2_name = 'ny_trip_zones'

#CLI Commands/parameters
@click.command()
@click.option('--user', default='postgres', show_default=True, help='Postgres user')
@click.option('--pwd', default='postgres', show_default=True, help='Postgres password')
@click.option('--host', default='localhost', show_default=True, help='Postgres host')
@click.option('--port', default=5433, type=int, show_default=True, help='Postgres port')
@click.option('--db', default='ny_taxi', show_default=True, help='Database name')
@click.option('--t1-name', 't1_name', default='ny_green_taxi_trips', show_default=True, help='Target table name')
@click.option('--t2-name', 't2_name', default='ny_trip_zones', show_default=True, help='Target table name')

def main(user, pwd, host, port, db, t1_name, t2_name):

    #Pg connection
    pg_engine = create_engine(f'postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}')
    
    # Ingest green taxi trips .parquet
    try: 
        df_green_taxi = pd.read_parquet(
            url, 
            engine= 'pyarrow'
        )   
        
        table_green_taxi_pa = pa.Table.from_pandas(df_green_taxi)

        total_rows_t1 = 0

        for batch_no, batch in tqdm(enumerate(table_green_taxi_pa.to_batches(max_chunksize=10000), start=1)):
            df_batch = batch.to_pandas()
            df_batch.to_sql(name= t1_name,
                            con= pg_engine, 
                            if_exists= 'append'
                           )
            total_rows_t1 = total_rows_t1 + len(df_batch)
            print(f'{len(df_batch)} Rows were added to the {t1_name} table; Batch No. {batch_no}')
    
    except: 
        print(f'An error ocurred ingesting {t1_name}')

    finally: 
        print(f'Total {total_rows_t1} rows added')
        
    # Ingest taxi zone .csv 
    try:
        df_trip_zones = pd.read_csv(
            url_2, 
            iterator= True, 
            chunksize= 50
        )

        total_rows_t2 = 0

        for chunk_no, df_no in tqdm(enumerate(df_trip_zones, start= 1)):
            df_no.to_sql(name= t2_name,
                         con= pg_engine,
                         if_exists= 'append'
                        )
            total_rows_t2 = total_rows_t2 + len(df_no)
            print(f'{len(df_no)} Rows were added to the {t2_name} table; Dataframe No. {chunk_no}')

    except: 
        print(f'An error ocurred ingesting {t2_name}')

    finally: 
        print(f'Total {total_rows_t2} rows added')
        

if __name__ == '__main__':
    main()



