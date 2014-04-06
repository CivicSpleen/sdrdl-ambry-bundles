
# Caveats

The ACS_46 column for the 2009 through 2012 files seems to be overlaid with the SCI column.  So, for a record  where  in the DBF format ACS_46 is 'N/A' and SCI is '155.9600864', in the TXT format, ACS_46 is 'N155.' In the text file, the 
whole  SCI value will appear, not just the first few characters, so I 
suspect there is more widespread file corruption. 

# Versions

* 0.1.1 Initial import
* 0.1.2  Currected field names that are wrong in CDE documentation. In 2001 and 2001, The 'valid-num' cokumn is actually named 'valid'