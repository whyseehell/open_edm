'''
Created on 09 may 2018

@author: yves.coupez
'''

from datetime import datetime
import os
import csv
from whoosh import index
from whoosh.fields import *
from whoosh.qparser import QueryParser
import select_securities as dedupe
import classifier_by_DT as dt
import utilities as ut
from cerberus import Validator
from dateutil.parser import *


@ut.profile
def validate(sois,validate_schema):
	is_bool = lambda v: v.lower() in ('true', '1', 'y', 'false', '0', 'n')

	v = Validator(validate_schema)
	v.allow_unknown = True

	for soi in sois:
		if soi['hits'] is not None:
			for hit in soi['hits']:
				if not v.validate(hit):
					hit['errors'] = v.errors

def write_out(sois,outfile,fields_oi):

	soi_input_fields = [x for x in sois[0].keys() if x != 'hits']
	search_details = ['search_status','selection_rule','found terms','class_code']
	output_headers = soi_input_fields + search_details + fields_oi + ['validate_errors']

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
						o_row[field_oi] = hit[field_oi] if field_oi in hit.keys() else ''
					for search_detail in search_details:
						o_row[search_detail] = hit[search_detail]
					if hit.get('errors', None) :
						o_row['validate_errors'] = hit['errors']
					dw.writerow(o_row)

	return outfile


@ut.profile
def whoosh_search(ix,soi,mode,dt_instrument_class,country_exch_order):
	search_ids = ['sedol','isin', 'cusip','corp_ticker']
	search_options = ['country_issue_iso','currency','exch_code']
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

	elif mode == 'and_all_terms':
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
		return dedupe.select_security([result['raw_data'] for result in results], dt_instrument_class, country_exch_order)


def load_soi(soi_file_in):
	with open(soi_file_in, 'rb') as file:
		reader = csv.DictReader(file, delimiter=',')
		return list(reader)


def main(index_base_path, vendor_code, index_type, mode,
		 soi_path, soi_file, soi_result, fields_oi,
		 ref_data_path, DT_instrument_classifier, country_exch_order_filename,
		 validate_schema):

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
			print 'soi rows processed so far: {} in {}'.format(counter, datetime.datetime.now() - start_time)

	print 'soi rows processed: {} in {}'.format(counter, datetime.datetime.now() - start_time)

	if validate_schema:
		validate(sois, validate_schema)
	output_file = write_out(sois, os.path.join(soi_path, soi_result), fields_oi)

	print output_file



if __name__ == "__main__":
	start_time = datetime.datetime.now()
	print start_time

	vendor_code = 'ven_bbg'
	index_base_path = '/Users/yvescoupez/PycharmProjects/data/Indices'
	index_type = 'Instrument'
	soi_path = './data'
	soi_file = 'soi_aapl_demo.csv'
	search_mode = 'hierarchical'# 'and_all_terms' # 'hierarchical'
	soi_result = 'soi_aapl_demo_results.csv'
	fields_oi = ['TICKER','EXCH_CODE','NAME','CRNCY','ID_SEDOL1','ID_ISIN','ID_CUSIP','CNTRY_ISSUE_ISO',
				 'SEDOL1_COUNTRY_ISO','MARKET_STATUS','REL_INDEX','LONG_COMP_NAME','CNTRY_OF_DOMICILE','EQY_SH_OUT',
				 'EQY_SH_OUT_DT', 'EQY_PRIM_EXCH','EQY_PRIM_SECURITY_CRNCY','EQY_PRIM_SECURITY_TICKER','144A_FLAG']

	ref_data_path = './data/reference'
	DT_instrument_classifier = 'DT_instrument_classifier.csv'
	country_exch_order_filename = 'country_exch_order.csv'

	is_bool = lambda v: v.lower() in ('true', '1', 'y', 'false', '0', 'n')

	validate_schema = {'ID_ISIN': {'regex': '([A-Z]{2}[A-Z\d*#]{9}\d)', 'minlength': 12, 'type': 'string', 'maxlength': 12},
	                   'ID_CUSIP': {'regex': '([A-Z\d\s#*@/]{4,9})', 'minlength': 4, 'type': 'string', 'maxlength': 9},
	                   'CNTRY_ISSUE_ISO': {'minlength': 2, 'type': 'string', 'maxlength': 5},
	                   'NAME': {'minlength': 1, 'type': 'string', 'maxlength': 30},
	                   'CRNCY': {'minlength': 2, 'type': 'string', 'maxlength': 5},
	                   'EXCH_CODE': {'minlength': 2, 'type': 'string', 'maxlength': 20},
	                   'SEDOL1_COUNTRY_ISO': {'minlength': 2, 'type': 'string', 'maxlength': 2},
	                   'MARKET_STATUS': {'minlength': 1, 'type': 'string', 'maxlength': 4},
	                   'REL_INDEX': {'minlength': 2, 'type': 'string', 'maxlength': 8},
	                   '144A_FLAG': {'coerce': (str, is_bool), 'allowed': [True]},
	                   'EQY_SH_OUT': {'coerce': float, 'type': 'float'},
	                   'CNTRY_OF_DOMICILE': {'minlength': 2, 'type': 'string', 'maxlength': 2},
	                   'EQY_PRIM_SECURITY_CRNCY': {'minlength': 3, 'type': 'string', 'maxlength': 5},
	                   'LONG_COMP_NAME': {'minlength': 1, 'type': 'string', 'maxlength': 100},
	                   'EQY_PRIM_SECURITY_TICKER': {'minlength': 1, 'type': 'string', 'maxlength': 10},
	                   'TICKER': {'minlength': 1, 'type': 'string', 'maxlength': 22},
	                   'PX_ROUND_LOT_SIZE': {'coerce': int, 'type': 'integer'},
	                   'ID_SEDOL1': {'regex': '([A-Z\d]{6}\d)', 'minlength': 7, 'type': 'string', 'maxlength': 7},
	                   'EQY_SH_OUT_DT': {'type': 'date','nullable': True,
	                                     'coerce': (str, lambda v: parse(str(v)) if v != 'N.A.' else None)}
	                   }

	main(index_base_path,vendor_code,index_type,search_mode,soi_path,soi_file,soi_result,fields_oi,
		 ref_data_path,DT_instrument_classifier,country_exch_order_filename, validate_schema)

	print datetime.datetime.now() - start_time
	ut.print_prof_data()