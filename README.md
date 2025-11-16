# Combining CSVs With Slightly Different Schemas

<img style="margin-top:0px;margin-left:0px;width:50vw" src="https://www.dropbox.com/scl/fi/9ff47a76dgt88483dskxm/combining_csvs_post_image.png?rlkey=q6twco5fnr7ly4sw9dtgpvlsc&raw=1" alt="Image showing three CSV files being merged into a single file.">

I was asked a question the other day, one that I hear often from clients:

> How do you merge hundreds of data files when some of them have mismatched schemas?

The person was dealing with the classic "extra column" issue: sometimes those columns were empty, sometimes not, and wondered whether the best solution was to review each file manually, identify the common columns, keep only those, and then merge everything into one large dataset before proceeding with analysis.

Sure, you could do that. But why?

Even though their data was spread across multiple files, merging didn't have to be a headache because they knew three things:

1.	The columns they cared about followed consistent naming conventions.
2.	There was overlap in column names across the files.
3.	The overlapping columns contained data of the same type.

Knowing this, the solution was simple: merge the schemas by column name while loading the files, or right after. Plus, I pointed out that by removing non-shared columns, they potentially could be reducing the scope of their analysis, since those columns might hold useful information.

In this post, I'll show you several ways to merge multiple CSV files that don't share the exact same schema, thanks to an extra column sprinkled here and there, using three popular Python (3.13.6) libraries:

- pandas (2.3.3)
- polars (1.35.2)
- duckdb (1.4.2)

Let's get started.

### Setting up your environment

The first thing we need to do is set up our environment. The data and Python code used in this post can be downloaded from my [repository](https://github.com/anyamemensah/diff-csv-schemas). The data used in this post comes from the [School District of Philadelphia](https://www.philasd.org/research/#opendata) and was lightly cleaned as part of another project. 

Let's load a few libraries.

```python
import os
import pandas as pd
import polars as pl
import duckdb as db
```

Once we have loaded all the libraries we want to use, let's locate the CSV files and store the file paths to these files in a list called `csv_files`:

```python
all_files = os.listdir("./data/")
csv_files = [os.path.join("data", c) for c in all_files if c.endswith(".csv")]
csv_files
```

```
['data/sdp_info_2019-2020.csv',
 'data/sdp_info_2018-2019.csv',
 'data/sdp_info_2021-2022.csv',
 'data/sdp_info_2020-2021.csv',
 'data/sdp_info_2024-2025.csv',
 'data/sdp_info_2022-2023.csv',
 'data/sdp_info_2023-2024.csv']
```

Now let's dive into the good stuff: three different ways to merge the data with Python.

### pandas

Pandas gets a lot of flak these days, especially with the shiny new kid on the block (looking at you, polars). But if you're working with small-to-medium-sized tabular data and Python is your go-to language, pandas is still a fantastic choice.

Our goal is to merge the files into a single pandas DataFrame, even though they may have different schemas. The easiest approach? Read each file with [`.read_csv()`](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html) and then combine them with [`.concat()`](https://pandas.pydata.org/docs/reference/api/pandas.concat.html).

```python
dfs = [
    pd.read_csv(c)
    for c in csv_files
]
df = pd.concat(dfs, ignore_index=True)
```
The code ran smoothly without any errors, thanks to the [`.concat()`](https://pandas.pydata.org/docs/reference/api/pandas.concat.html) method's defaults, which do most of the heavy lifting behind the scenes. It automatically stacks the data by rows and adds any columns that appear in one file but not in others, placing those extra columns at the end of the combined DataFrame in alphabetical order.

```python
df.columns
```

```
Index(['school_id', 'school_year', 'standardized_school_name',
       'original_school_name', 'year_school_opened', 'is_elementary_school',
       'is_middle_school', 'is_high_school', 'is_other_school',
       'admission_type', 'governance', 'serves_grade_k', 'serves_grade_1',
       'serves_grade_2', 'serves_grade_3', 'serves_grade_4', 'serves_grade_5',
       'serves_grade_6', 'serves_grade_7', 'serves_grade_8', 'serves_grade_9',
       'serves_grade_10', 'serves_grade_11', 'serves_grade_12', 'new_col',
       'that_col', 'this_col'],
      dtype='object')
```

```python
df.head()
```

```
   school_id school_year                     standardized_school_name  \
0       1010   2019-2020                     John Bartram High School   
1       1020   2019-2020                West Philadelphia High School   
2       1030   2019-2020                    High School of the Future   
3       1050   2019-2020  Paul Robeson High School for Human Services   
4       1100   2019-2020                 William L. Sayre High School   

                          original_school_name  year_school_opened  \
0                     John Bartram High School              1939.0   
1                West Philadelphia High School              1911.0   
2                    High School of the Future              2006.0   
3  Paul Robeson High School for Human Services              2003.0   
4                 William L. Sayre High School              1950.0   

   is_elementary_school  is_middle_school  is_high_school  is_other_school  \
0                     0                 0               1                0   
1                     0                 0               1                0   
2                     0                 0               1                0   
3                     0                 0               1                0   
4                     0                 0               1                0   

  admission_type  ... serves_grade_6  serves_grade_7  serves_grade_8  \
0   Neighborhood  ...            0.0             0.0             0.0   
1   Neighborhood  ...            0.0             0.0             0.0   
2       Citywide  ...            0.0             0.0             0.0   
...
2       NaN       NaN  
3       NaN       NaN  
4       NaN       NaN  

[5 rows x 27 columns]
```

### polars

Polars is a lightweight, efficient alternative to pandas for complex data preprocessing and cleaning workflows. It can crunch through medium to large-ish data without too much pain. 

Polars has a [`.scan_csv()`](https://docs.pola.rs/api/python/dev/reference/api/polars.scan_csv.html) method that is handy for processing data from large CSV files. Let's try using its defaults to import and merge the CSV files into a single polars DataFrame:

```python
dfs = pl.scan_csv("./data/*.csv", has_header=True).collect()
```
```
---------------------------------------------------------------------------
ComputeError                              Traceback (most recent call last)
Cell In[21], line 1
----> 1 dfs = pl.scan_csv("./data/*.csv", has_header=True).collect()

File ~/Library/CloudStorage/Dropbox/diff-csv-schemas/.venv/lib/python3.13/site-packages/polars/_utils/deprecation.py:97, in deprecate_streaming_parameter.<locals>.decorate.<locals>.wrapper(*args, **kwargs)
     93         kwargs["engine"] = "in-memory"
     95     del kwargs["streaming"]
---> 97 return function(*args, **kwargs)

File ~/Library/CloudStorage/Dropbox/diff-csv-schemas/.venv/lib/python3.13/site-packages/polars/lazyframe/opt_flags.py:328, in forward_old_opt_flags.<locals>.decorate.<locals>.wrapper(*args, **kwargs)
    325         optflags = cb(optflags, kwargs.pop(key))  # type: ignore[no-untyped-call,unused-ignore]
    327 kwargs["optimizations"] = optflags
--> 328 return function(*args, **kwargs)

File ~/Library/CloudStorage/Dropbox/diff-csv-schemas/.venv/lib/python3.13/site-packages/polars/lazyframe/frame.py:2422, in LazyFrame.collect(self, type_coercion, predicate_pushdown, projection_pushdown, simplify_expression, slice_pushdown, comm_subplan_elim, comm_subexpr_elim, cluster_with_columns, collapse_joins, no_optimization, engine, background, optimizations, **_kwargs)
   2420 # Only for testing purposes
   2421 callback = _kwargs.get("post_opt_callback", callback)
-> 2422 return wrap_df(ldf.collect(engine, callback))

ComputeError: schema lengths differ

This error occurred with the following context stack:
	[1] 'csv scan'
	[2] 'sink'
```

We got an error message calling out the very challenge we're tackling: mismatched schema lengths.

One way to solve this issue is by using the [`.concat()`](https://docs.pola.rs/api/python/dev/reference/api/polars.concat.html) method. It works much like pandas' version, but with an added advantage: you can dictate how the data are merged using the `how` parameter.

For this example, we'll set `how` to `"diagonal_relaxed"`. This option creates a union of the column schemas, by name, fills missing values with null, and converts mismatched columns to a common supertype.

```python
dfs = [
    pl.scan_csv(c, has_header=True)
    for c in csv_files
]
df = pl.concat(dfs, how="diagonal_relaxed").collect() 
```

If your data is more on the small to medium size, swap out [`pl.scan_csv()`](https://docs.pola.rs/api/python/dev/reference/api/polars.scan_csv.html) for [`pl.read_csv()`](https://docs.pola.rs/api/python/dev/reference/api/polars.read_csv.html) and remove [`.collect()`](https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect.html).

```python
dfs = [
    pl.read_csv(c, has_header=True)
    for c in csv_files
]
df = pl.concat(dfs, how="diagonal_relaxed")
```

```python
df.columns
```

```
['school_id',
 'school_year',
 'standardized_school_name',
 'original_school_name',
 'year_school_opened',
 'is_elementary_school',
 'is_middle_school',
 'is_high_school',
 'is_other_school',
 'admission_type',
 'governance',
 'serves_grade_k',
 'serves_grade_1',
 'serves_grade_2',
 'serves_grade_3',
 'serves_grade_4',
 'serves_grade_5',
 'serves_grade_6',
 'serves_grade_7',
 'serves_grade_8',
 'serves_grade_9',
 'serves_grade_10',
 'serves_grade_11',
 'serves_grade_12',
 'new_col',
 'that_col',
 'this_col']
```

```python
df.head()
```

```
shape: (5, 27)
┌───────────┬────────────┬────────────┬────────────┬───┬───────────┬─────────┬──────────┬──────────┐
│ school_id ┆ school_yea ┆ standardiz ┆ original_s ┆ … ┆ serves_gr ┆ new_col ┆ that_col ┆ this_col │
│ ---       ┆ r          ┆ ed_school_ ┆ chool_name ┆   ┆ ade_12    ┆ ---     ┆ ---      ┆ ---      │
│ i64       ┆ ---        ┆ name       ┆ ---        ┆   ┆ ---       ┆ str     ┆ str      ┆ str      │
│           ┆ str        ┆ ---        ┆ str        ┆   ┆ i64       ┆         ┆          ┆          │
│           ┆            ┆ str        ┆            ┆   ┆           ┆         ┆          ┆          │
╞═══════════╪════════════╪════════════╪════════════╪═══╪═══════════╪═════════╪══════════╪══════════╡
│ 1010      ┆ 2019-2020  ┆ John       ┆ John       ┆ … ┆ 1         ┆ null    ┆ null     ┆ null     │
│           ┆            ┆ Bartram    ┆ Bartram    ┆   ┆           ┆         ┆          ┆          │
│           ┆            ┆ High       ┆ High       ┆   ┆           ┆         ┆          ┆          │
│           ┆            ┆ School     ┆ School     ┆   ┆           ┆         ┆          ┆          │
│ 1020      ┆ 2019-2020  ┆ West Phila ┆ West Phila ┆ … ┆ 1         ┆ null    ┆ null     ┆ null     │
│           ┆            ┆ delphia    ┆ delphia    ┆   ┆           ┆         ┆          ┆          │
│           ┆            ┆ High       ┆ High       ┆   ┆           ┆         ┆          ┆          │
│           ┆            ┆ School     ┆ School     ┆   ┆           ┆         ┆          ┆          │
│ 1030      ┆ 2019-2020  ┆ High       ┆ High       ┆ … ┆ 1         ┆ null    ┆ null     ┆ null     │
│           ┆            ┆ School of  ┆ School of  ┆   ┆           ┆         ┆          ┆          │
│           ┆            ┆ the Future ┆ the Future ┆   ┆           ┆         ┆          ┆          │
│ 1050      ┆ 2019-2020  ┆ Paul       ┆ Paul       ┆ … ┆ 1         ┆ null    ┆ null     ┆ null     │
│           ┆            ┆ Robeson    ┆ Robeson    ┆   ┆           ┆         ┆          ┆          │
│           ┆            ┆ High       ┆ High       ┆   ┆           ┆         ┆          ┆          │
│           ┆            ┆ School for ┆ School for ┆   ┆           ┆         ┆          ┆          │
│           ┆            ┆ H…         ┆ H…         ┆   ┆           ┆         ┆          ┆          │
│ 1100      ┆ 2019-2020  ┆ William L. ┆ William L. ┆ … ┆ 1         ┆ null    ┆ null     ┆ null     │
│           ┆            ┆ Sayre High ┆ Sayre High ┆   ┆           ┆         ┆          ┆          │
│           ┆            ┆ School     ┆ School     ┆   ┆           ┆         ┆          ┆          │
└───────────┴────────────┴────────────┴────────────┴───┴───────────┴─────────┴──────────┴──────────┘
```

Great, no error messages. By using [`pl.concat()`](https://docs.pola.rs/api/python/dev/reference/api/polars.concat.html), we were able to replicate what we did using the pandas library: our datasets were automatically stacked by rows and any columns that appear in one dataset but not in others were added to the combined DataFrame in alphabetical order.

### DuckDB

Last, but certainly not least, we have DuckDB. DuckDB is a powerhouse! I use it for local development, proof-of-concept projects, data warehousing, data wrangling, and more. DuckDB really shines at analyzing datasets larger than memory. DuckDB has a [`read_csv()`](https://duckdb.org/docs/stable/data/csv/overview) function that can be used to read multiple CSV files. Let's use that function's defaults to import and merge the CSV files into a single table:

First, we'll create a connection to a persistent database. Then use [`read_csv()`](https://duckdb.org/docs/stable/data/csv/overview) to read all CSV files from the data folder:

```python
con = db.connect(':memory:')

con.sql(
    """
    SELECT * FROM read_csv('data/*.csv');
    """
).show()
```

```
Traceback (most recent call last):
  File "<python-input-27>", line 5, in <module>
    ).show()
      ~~~~^^
_duckdb.InvalidInputException: Invalid Input Error: Schema mismatch between globbed files.
Main file schema: data/sdp_info_2018-2019.csv
Current file: data/sdp_info_2019-2020.csv
Column with name: "new_col" is missing
Potential Fixes 
* Consider setting union_by_name=true.
* Consider setting files_to_sniff to a higher value (e.g., files_to_sniff = -1)
```

We encountered an error indicating a schema mismatch between the files we were trying to read. Not good. 

But DuckDB is pretty helpful when errors occur. Not only does DuckDB identify where the mismatch occurs but also specifies the column causing the issue and suggests potential fixes.

```
Main file schema: data/sdp_info_2018-2019.csv
Current file: data/sdp_info_2019-2020.csv
Column with name: "new_col" is missing
Potential Fixes 
* Consider setting union_by_name=true.
* Consider setting files_to_sniff to a higher value (e.g., files_to_sniff = -1)
```

To get DuckDB to merge datasets with different schemas by column name, we can specify a few parameters in the [`read_csv()`](https://duckdb.org/docs/stable/data/csv/overview) function:

-	` union_by_name = true`
-	` strict_mode = false`

We'll also include a few additional parameters to make sure DuckDB correctly interprets the files as comma-separated values with column headers in the first row, and explicitly set the quote character to a double quotation mark.

```python
con.sql(
    """    
    SELECT * FROM read_csv('data/*.csv', delim = ',', header = true, quote='"', union_by_name = true, strict_mode = false);
    """
    ).show()
```

```
┌─────────┬───────────┬─────────────┬─────────────────────────────────────────────┬─────────────────────────────────────────────┐
│ new_col │ school_id │ school_year │          standardized_school_name           │            original_school_name             │
│ varchar │   int64   │   varchar   │                   varchar                   │                   varchar                   │
├─────────┼───────────┼─────────────┼─────────────────────────────────────────────┼─────────────────────────────────────────────┤
│ NULL    │      1010 │ 2018-2019   │ John Bartram High School                    │ John Bartram High School                    │
│ NULL    │      1020 │ 2018-2019   │ West Philadelphia High School               │ West Philadelphia High School               │
│ NULL    │      1030 │ 2018-2019   │ High School of the Future                   │ High School of the Future                   │
│ NULL    │      1050 │ 2018-2019   │ Paul Robeson High School for Human Services │ Paul Robeson High School for Human Services │
│ NULL    │      1100 │ 2018-2019   │ William L. Sayre High School                │ William L. Sayre High School                │
│ NULL    │      1130 │ 2018-2019   │ William T. Tilden School                    │ William T. Tilden School                    │
│ NULL    │      1190 │ 2018-2019   │ Motivation High School                      │ Motivation High School                      │
│ NULL    │      1200 │ 2018-2019   │ John Barry School                           │ John Barry School                           │
│ NULL    │      1230 │ 2018-2019   │ William C. Bryant School                    │ William C. Bryant School                    │
│ NULL    │      1250 │ 2018-2019   │ Joseph W. Catharine School                  │ Joseph W. Catharine School                  │
├─────────┴───────────┴─────────────┴─────────────────────────────────────────────┴─────────────────────────────────────────────┘
```

Success!

--

I hope this post has provided some practical strategies for importing and combining CSV files with slightly different schemas. Sure, you could manually inspect each file, identify the shared columns, and limit your analysis to those, but let's be honest, that's tedious. More importantly, what if your data changes over time and those "extra" columns you removed actually contain data you'd want to explore? By restricting yourself to common columns, you risk losing valuable information. That's why it's worth considering approaches that merge schemas while preserving as much data as possible.

Which approach would you use in your day-to-day data work? Do you have another method you prefer? Let me know by emailing me at ama@anyamemensah.com.
