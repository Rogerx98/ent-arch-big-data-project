import pandas as pd
import re as re
import numpy as np
from transformers import pipeline

df = pd.read_csv("data copy.csv")
#df = df.sample(frac = 0.002) #for tests with small sample (4 rows)
print(df.info())


df["location.display_name"] = df["location.display_name"].str.split(",").str[0]
df_unique = df["location.display_name"].unique()

df_u = pd.DataFrame(df_unique)

df_u.insert(0, 'ID', range(1, 1+len(df_u)))

print(df_u)

df_u.to_csv("City_uniques_with_ID.csv")
#substring = split_string[0]
#print(df["city"])