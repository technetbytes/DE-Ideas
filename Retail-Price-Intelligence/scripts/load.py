from dotenv import load_dotenv
import os
import pandas as pd
import logging

from sqlalchemy import create_engine
from datetime import datetime

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Pipeline started")


DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(DATABASE_URL)

df = pd.read_csv("data/clean_products.csv")

df["scraped_at"] = datetime.now()

df.to_sql(
    "fact_prices",
    engine,
    if_exists="append",
    index=False
)

logging.info("Data loaded successfully")