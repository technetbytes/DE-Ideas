import pandas as pd

df = pd.read_csv("data/raw_products.csv")

df["brand"] = df["brand"].str.lower()

df["product_name"] = (
    df["product_name"]
    .str.lower()
    .str.strip()
)

df["store"] = df["store"].str.lower()

df["category"] = df["category"].str.lower()

df.drop_duplicates(inplace=True)

df.dropna(inplace=True)

df.to_csv("data/clean_products.csv", index=False)

print("Data transformed successfully")
print(df)