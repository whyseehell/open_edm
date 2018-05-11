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
import copy


def write_out(sois,outfile,fields_oi):

	soi_input_fields = [x for x in sois[0].keys() if x != 'hits']
	search_details = ['search_status','dedupe_rule','found terms','class_code']
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
			else:
				for hit in soi['hits']:
					o_row = {key: '' for key in output_headers}
					for soi_input_field in soi_input_fields:
						o_row[soi_input_field] = soi[soi_input_field]
					for field_oi in fields_oi:
						o_row[field_oi] = hit[field_oi]
					for search_detail in search_details:
						o_row[search_detail] = hit[search_detail]
					dw.writerow(o_row)

	return outfile


@ut.profile
def whoosh_search(ix,soi,mode,dt_instrument_class,country_exch_order):
	search_ids = ['sedol','isin', 'cusip','corp_ticker']
	search_options = ['country_issue_iso','exch_code','currency']
	active_search_terms	= list()
	active_search_terms.append(search_ids + search_options)

	if mode == 'hierarchical':

		for search_id in search_ids:
			for n in range(len(search_options) + 1):
				opt = search_options[:len(search_options) -  n]
				active_search_terms.append([str(search_id)] + opt)

		exit = False
		while not exit:
			for active_search_term in active_search_terms:
				parse_string = ' '.join('{}:{}'.format(k, v.strip("\'"))
										for k, v in soi.iteritems() if k in active_search_term and v.strip())
				soi['hits'] = execute_search(ix, parse_string, soi, dt_instrument_class, country_exch_order)
				if not soi['hits'] == None:
					for hit in soi['hits']:
						hit['found terms'] = parse_string
					break
			exit = True
	elif mode == 'and_terms':
		parse_string = ' '.join('{}:{}'.format(k, v.strip("\'"))
								for k, v in soi.iteritems() if k in active_search_terms[0] and v.strip())
		soi['hits'] = execute_search(ix, parse_string, soi, dt_instrument_class, country_exch_order)
		if not soi['hits'] == None:
			for hit in soi['hits']:
				hit['found terms'] = parse_string


	# parse_string = ' '.join('{}:{}'.format(k, v.strip("\'")) for k, v in soi.iteritems() if k in active_search_terms and v)
	# soi['hits'] = execute_search(ix,parse_string,soi,dt_instrument_class, country_exch_order)


@ut.profile
def execute_search(ix,parse_string,soi,dt_instrument_class, country_exch_order):
	with ix.searcher() as searcher:
		query = QueryParser("isin", ix.schema).parse(parse_string)
		# isin is the default search field if the parse_string does not provide a search field
		results = searcher.search(query, limit=None)
		return dedupe.remove_duplicates([result['raw_data'] for result in results],dt_instrument_class, country_exch_order)


def load_soi(soi_file_in):
	with open(soi_file_in, 'rb') as file:
		reader = csv.DictReader(file, delimiter=',')
		return list(reader)

def main(index_base_path,vendor_code,index_type,mode,
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
		whoosh_search(ix, soi,mode,dt_instrument_class,country_exch_order)
		counter += 1
		if counter % 10 == 0:
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
	search_mode = 'and_terms'# 'and_terms' # 'hierarchical'
	soi_result = 'soi_aapl_demo_results.csv'
	fields_oi = ['TICKER','EXCH_CODE','NAME','CRNCY','ID_SEDOL1','ID_ISIN','ID_CUSIP','CNTRY_ISSUE_ISO',
				 'SEDOL1_COUNTRY_ISO','MARKET_STATUS','REL_INDEX','LONG_COMP_NAME','CNTRY_OF_DOMICILE','EQY_SH_OUT',
				 'EQY_PRIM_EXCH','EQY_PRIM_SECURITY_CRNCY','EQY_PRIM_SECURITY_TICKER']

	ref_data_path = '/Users/yvescoupez/PycharmProjects/data/reference'
	DT_instrument_classifier = 'DT_instrument_classifier.csv'
	country_exch_order_filename = 'country_exch_order.csv'

	main(index_base_path,vendor_code,index_type,search_mode,soi_path,soi_file,soi_result,fields_oi,ref_data_path,DT_instrument_classifier,country_exch_order_filename)

	end_time = datetime.datetime.now()

	print end_time - start_time
	ut.print_prof_data()