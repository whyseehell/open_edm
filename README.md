# open_edm
pattern to extract securities of interest from financial data vendor files and to report
the requested securities with only the fields of interest.

A demonstrator of a simple ETL for financial data that can be expanded to cater for 
multiple vendors, asset types and data types.


vendor adapter: an iterator class that return each row featured in a  data vendor file as 
python data dictionary. The example provided is bbg_standard_file_reader.py  that returns 
the data from a Bloomberg back office file. Try the code with the equity_"region".out files.

The process start with the indexing of the file data. Each row in the data files is stored 
as a whoosh index (http://whoosh.readthedocs.io/en/latest/index.html) document 
with a number of fields extracted as index searchable values.

create_index.py defines the index schema and creates the whoosh index. It is called by 
index_file.py. The schema provided is for a Bloomberg equity instrument

index_file.py first clears the current contents of the index by calling create_index.py 
and then add each row returned by the vendor adapter as an index document.

parameters needed for indexing:
 - for the index location
 	- index_base_path = location of the directory that hosts the indices
 	- vendor_code = location of the sub-directory holding the indices for each vendor
	- index_type = location of the directory for an index (schema) 
 - for the data to read and index
 	- data_path: the directory holding the data files
 	- file_list: the list of files to read provided as a python list


Now that the data is indexed we can search and extract data for the "securities of 
interest" (SOI).

The SOI file (soi_aapl_demo.csv in the example) allows to define the securities of interest.
It includes:
 - identifiers: isin,cusip,sedol,corp_ticker. 
				   Any number of identifiers can be provided (see search mode)
				   It could include vendor identifiers such as FIGI or RIC
				
- optional search parameters: country_issue_iso,exch_code,currency. 
								These are optional parameters. Any number can be provided
								
The search has 2 modes:
 - 'and_all_terms': uses a logical AND with all the search term provided. 
					  The search will return a hit only if all terms are found
 - 'hierarchical':	if the search fails to return a hit for "and_all_terms", it will 
						try each identifier and with each optional parameters until it has a hit.
						The identifiers and optional search parameters have an order of preference
	search_ids = ['sedol','isin', 'cusip','corp_ticker'], 
	search_options = ['country_issue_iso','currency','exch_code']

If the search returns more then one hit the search includes a process to select a security 
based on an hierarchy of rules (select_securities.py). The search will return all hits left
 at the end of the selection process.
 
The output file includes 3 sections:
 - soi input: echo the soi 
 - a search report with:
 	- search_status: "not found",
					 "selection success" (one hit left),
					 "selection incomplete" (more than one hit left)
	- selection_rule: the last rule exercised that reduced the data to 1 hit
	- found terms: the search term(s) that returned a result from the index search
	- class_code: a security classifier returned by classifier_by_DT.py
 - the list of fields for which data was extracted from the vendor files
	
To run search.py the following parameters are required:
 - for the index location:
	- index_base_path = location of the directory that hosts the indices
	- vendor_code = location of the sub-directory holding the indices for each vendor
	- index_type = location of the directory for an index (schema)  
 - for the soi:
	- soi_path = location of the directory holding soi file
	- soi_file = the name of the soi file (as a csv file)
	- soi_result = the name of the result file
 - for the search mode:
	- search_mode = as either 'and_all_terms' or 'hierarchical'
	- fields_oi = fields of interest. The list of fields for which data will be 
				 extracted, provided as a python list  
							
To run:
 - run index_file.py once or as required by the file update cycle
 - run search.py as needed