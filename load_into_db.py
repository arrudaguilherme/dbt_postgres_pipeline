import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")


base = os.getcwd()
path_tables = os.path.join(base,'tables')
tables = os.listdir(path_tables)

con = create_engine(f"postgresql+psycopg2://postgres:{DB_PASSWORD}@localhost:5432/{DB_NAME}")

for table in tables:
    path_table = os.path.join(path_tables,table)
    table = table.replace(".csv","")
    df = pd.read_csv(path_table)
    df.to_sql(
        table, con, "transactional",
        if_exists="replace", index = False
    )
    print(f"Table {table} upload! ")
