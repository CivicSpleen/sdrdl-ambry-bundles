
# FFIEC CRA Disclosures

This conversion of multiple years of CRA disclosures is taken from the flat files [published on the FFIEC website](http://www.ffiec.gov/cra/craflatfiles.htm). 

The source data is complicated. Each flat file has multiple tables, each table expressed as a fixed-width rows, with the positions of the colums documented in a PDF. We've scrape* D the PDFs into CSV files, which are stored in the __meta/disclosure_datadicts__ directory. The __datadict-disclosures diffs.xls__ file notes the differences between the years.

Each file has multiple tables, differentiated by the first column of the row. The tables are:
* D 1-1 Small Business Loans by County - Originations 
* D 1-2 Small Business Loans by County - Purchases 
* D 2-1 Small Farm Loans by County - Originations 
* D 2-2 Small Farm Loans by County - Purchases 
* D 3 Assessment Area/Non Assessment Area Activity - Small Business 
* D 4 Assessment Area/Non Assessment Area Activity - Small Farm 
* D 5 Community Development/Consortium-Third Party Activity 
* D 6 Assessment Area(s) by Tract 


## Errors In Source Data

In many of the years, in the D5-0 table, the "Action Type" field is described as having values 'Y' or 'N'. It 
actually has 'P', 'O', or 'T', for 'purchased', 'originated' or 'total'.  


In Table D6-0, year 1996, the MSA column is incorrectly listed as 5 characters wide. It should be 4, as it is in other tables and other years. 

In Table D5-0, year 1996, many of the records are missing the last two fields ( 14 char combined ) so the rows have a length of 34 chars, rather than 48.

Table D5-0, year 1999, has lines of length 114 chars, while the file spec indicates they are 115 chars. The specification carried for the filler size of 65 chars from the previous years, which should have been reduced to 64 chars because of the addition of the "Action Type" field. 

Table D6-0, year 2005, has a similar problem with the filler size. 


## Notes

The schema.csv has width and description values for the tables, but these values may be different from year to year. The best description of the column positions and widths are in the __meta/table_regexes.yaml__ file. 

