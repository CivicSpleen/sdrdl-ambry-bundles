
# {about_title}

{about_summary}


The data in this dataset was submitted byt the San Diego Foo System Alliance to support the 2015 SDSU Data Analysis Contest. The datasets are extracts of administrative systems, there they have all of the inconsistencies and quality issues that are typical of administrative data. 

The three partitions in this dataset are: 

* `sandiegohungercoalition.org-agencies-agency_list`  is a subset of the organizations references in the [SDFSA Master Agency Matrix spreadsheet](http://ds.civicknowledge.org/sandiegohungercoalition.org/Agency%20Matrix.xlsx). It enumerates the agencies in the list and assigns `site_id` values to the different locations. 

* `sandiegohungercoalition.org-agencies-sdfb_partners`  has a direct import of  a list of agencies that are partners of the San Diego Food Bank. This list may be different than in `agency_list`

* `sandiegohungercoalition.org-agencies-locations` geocodes all of the addresses in the `agency_list` and `sdfb_partners` files

