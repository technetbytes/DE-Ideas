import pandas as pd
import os

data = [
    ["Imtiaz", "Olpers Milk 1L", "Olpers", "Dairy", 320],
    ["Naheed", "Olpers Milk 1L", "Olpers", "Dairy", 335],
    ["Chase Up", "Surf Excel 1kg", "Surf Excel", "Laundry", 890],
    ["Carrefour", "Tapal Danedar 900g", "Tapal", "Tea", 1450],
    ["Imtiaz", "Pepsi 1.5L", "Pepsi", "Beverages", 210]
]

columns = [
    "store",
    "product_name",
    "brand",
    "category",
    "price"
]

df = pd.DataFrame(data, columns=columns)

os.makedirs("data", exist_ok=True)

df.to_csv("data/raw_products.csv", index=False)

print("Raw data extracted successfully")
print(df)