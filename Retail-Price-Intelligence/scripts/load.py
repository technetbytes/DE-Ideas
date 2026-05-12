import pandas as pd
import logging

from sqlalchemy import create_engine
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Pipeline started")

DATABASE_URL = (
    "postgresql://retail:retail123@localhost:5432/retail_dw"
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