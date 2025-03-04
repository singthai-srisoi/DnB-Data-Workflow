import polars as pl
import numpy as np


with open("data/excel data/Maintain Customer.xlsx", "rb") as f:
    df = pl.read_excel(f)
    print(df)