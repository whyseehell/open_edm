from datetime import datetime
import os
import csv
from collections import namedtuple
from whoosh import index
from whoosh.fields import *
from whoosh.qparser import QueryParser
import search_remove_duplicate as dedupe
import classifier_by_DT as dt
import utilities as ut


def write_out(sois,outfile,fields_oi):

	soi_input_fields = [x for x in sois[0].keys() if x != 'hits']
	search_details = ['search_status','dedupe_rule','class_code']
	output_headers = soi_input_fields + search_details + fields_oi

	with open(outfile,'wb') as fou:
		dw = csv.DictWriter(fou, delimiter=',', fieldnames=output_headers)
		dw.writeheader()
		for soi in sois:
			if soi['hits'] == None:
				o_row = {key: '' for key in output_headers}
				o_row['search_status'] = 'Not found'
				for soi_input_field in soi_input_fields:
					o_row[soi_input_field] = soi[soi_input_field]
				dw.writerow(o_row)
			elif len(soi['hits']) == 1:
				o_row = {key: '' for key in output_headers}
				for soi_input_field in soi_input_fields:
					o_row[soi_input_field] = soi[soi_input_field]
				hit = soi['hits'][0]
				for field_oi in fields_oi:
					o_row[field_oi] = hit[field_oi]
				o_row['search_status'] = hit['search_status']
				o_row['dedupe_rule'] = hit['dedupe_rule']
				o_row['class_code'] = hit['class_code']
				dw.writerow(o_row)
			else:
				for hit in soi['hits']:
					o_row = {key: '' for key in output_headers}
					for soi_input_field in soi_input_fields:
						o_row[soi_input_field] = soi[soi_input_field]
					for field_oi in fields_oi:
						o_row[field_oi] = hit[field_oi]
					o_row['search_status'] = hit['search_status']
					o_row['dedupe_rule'] = hit['dedupe_rule']
					o_row['class_code'] = hit['class_code']
					dw.writerow(o_row)

	return outfile

@ut.profile
def whoosh_search(ix,soi,dt_instrument_class,country_exch_order):
	parse_string = ' '.join('{}:{}'.format(k, v.strip("\'")) for k, v in soi.iteritems() if v)
	with ix.searcher() as searcher:
		query = QueryParser("isin", ix.schema).parse(parse_string)
		results = searcher.search(query, limit=None)
		soi['hits'] = dedupe.remove_duplicates([result['raw_data'] for result in results],dt_instrument_class,country_exch_order)

def load_soi(soi_file_in):
	with open(soi_file_in, 'rb') as file:
		reader = csv.DictReader(file, delimiter=',')
		return list(reader)

def main(index_base_path,vendor_code,index_type,
		 soi_path,soi_file,soi_result,fields_oi,
		 ref_data_path,DT_instrument_classifier,country_exch_order_filename):

	if index.exists_in(os.path.join(index_base_path,vendor_code,index_type)):
		ix = index.open_dir(os.path.join(index_base_path,vendor_code,index_type))
	else:
		print "failed to open index at -->: ", os.path.join(index_base_path, vendor_code, index_type)
		quit()

	# load reference data
	dt_instrument_class = dt.read_lu(os.path.join(ref_data_path,DT_instrument_classifier))
	country_exch_order = ut.load_country_exch_order(os.path.join(ref_data_path, country_exch_order_filename))


	sois = load_soi(os.path.join(soi_path,soi_file))
	counter = 0
	for soi in sois:
		whoosh_search(ix, soi,dt_instrument_class,country_exch_order)
		counter += 1
		if counter % 1000 == 0:
			print counter, datetime.datetime.now() - start_time

	print counter
	output_file = write_out(sois,os.path.join(soi_path,soi_result),fields_oi)

	print output_file



if __name__ == "__main__":
	start_time = datetime.datetime.now()
	print start_time

	vendor_code = 'ven_bbg'
	index_base_path = '/Users/yvescoupez/PycharmProjects/data/Indices'
	index_type = 'Instrument'
	soi_path = '/Users/yvescoupez/PycharmProjects/data'
	soi_file = 'soi_aapl_demo.csv'
	soi_result = 'soi_aapl_demo_results.csv'
	fields_oi = ['TICKER','EXCH_CODE','NAME','CRNCY','ID_SEDOL1','ID_ISIN','ID_CUSIP','CNTRY_ISSUE_ISO',
				 'SEDOL1_COUNTRY_ISO','MARKET_STATUS','REL_INDEX','LONG_COMP_NAME','CNTRY_OF_DOMICILE','EQY_SH_OUT',
				 'EQY_PRIM_EXCH','EQY_PRIM_SECURITY_CRNCY','EQY_PRIM_SECURITY_TICKER']

	ref_data_path = '/Users/yvescoupez/PycharmProjects/data/reference'
	DT_instrument_classifier = 'DT_instrument_classifier.csv'
	country_exch_order_filename = 'country_exch_order.csv'

	main(index_base_path,vendor_code,index_type,soi_path,soi_file,soi_result,fields_oi,ref_data_path,DT_instrument_classifier,country_exch_order_filename)

	end_time = datetime.datetime.now()

	print end_time - start_time
	ut.print_prof_data()