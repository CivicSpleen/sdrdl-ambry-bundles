{about_summary}


## Caveats

There are three columns that have code columns, but probably shouldn't. Code columns are extra columns to hold values that are not of the right datatype, such as letter code in an integer column. The columns in the `calls` table with codes are: 

* hshld_pct_fpl
* zip
* hshld_size

The code columns have the same name as the source column, appended with `_code`. Note that this means that the `zip_code` column does not hold a zip_code, it holds the non-numeric values from the `zip` column. 

Analysts should example the unique values for the `hshld_pct_fpl_codes` and `hshld_size_codes` columns before using the source columns. 




