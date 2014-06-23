
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
* D 5 Community Development/Consortium-Thir* D Party Activity 
* D 6 Assessment Area(s) by Tract 

