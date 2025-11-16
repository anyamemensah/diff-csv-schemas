import os
import pandas as pd
import polars as pl
import duckdb as db

### find csv files
all_files = os.listdir("./data/")
csv_files = [os.path.join("data", c) for c in all_files if c.endswith(".csv")]

### Pandas
## pass
dfs = [
    pd.read_csv(c)
    for c in csv_files
]
df = pd.concat(dfs, ignore_index=True)

# examine the dataset
df.head()
df.columns



### Polars
## fail
dfs = pl.scan_csv("./data/*.csv", has_header=True).collect()

## pass (medium to large-ish data)
dfs = [
    pl.scan_csv(c, has_header=True)
    for c in csv_files
]
df = pl.concat(dfs, how="diagonal_relaxed").collect()

## pass (small to medium data)
dfs = [
    pl.read_csv(c, has_header=True)
    for c in csv_files
]
df = pl.concat(dfs, how="diagonal_relaxed")

# examine the dataset
df.head()
df.columns


### DuckDB
con = db.connect(':memory:')

## fail
con.sql(
    """
    SELECT * FROM read_csv('data/*.csv');
    """
).show()

## pass
con.sql(
    """    
    SELECT * FROM read_csv('data/*.csv', delim = ',', header = true, quote='"', union_by_name = true, strict_mode = false);
    """
).show()

