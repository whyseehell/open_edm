"""
Created on 09 may 2018

@author: yves.coupez
"""

import classifier_by_DT as dt
import utilities as ut


def select_asset_not_derivative(results,country_exch_order):
	deleted = []
	left = []

	asset_type_order = {'ESX':0,'EMX':1,'ERX':2,'ELX':3,'ESV':4,'ESA':5}
	default_order = 12345678

	pfd_asset_type = min([asset_type_order.get(hit.get('class_code'), default_order) for hit in results])

	pfd_asset_type_hits = [hit for hit in results if
						   asset_type_order.get(hit.get('class_code'),
												default_order) == pfd_asset_type]

	if pfd_asset_type_hits:
		left = pfd_asset_type_hits
	else:
		left = results

	return deleted, left

def select_primary(results,country_exch_order):
	deleted = []
	left = []

	primary_hits = [hit for hit in results if hit.get('ID_BB_PRIM_SECURITY_FLAG','N') == 'Y']

	if primary_hits:
		left.extend(primary_hits)
	else:
		left = results

	return deleted, left

def select_local_line(results,country_exch_order):
	deleted = []
	left = []

	local_line_hits = [hit for hit in results if hit.get('SEDOL1_COUNTRY_ISO') == hit.get('CNTRY_ISSUE_ISO')]

	if local_line_hits:
		left.extend(local_line_hits)
	else:
		left = results

	return deleted, left

def select_exch_country(results,country_exch_order):
	deleted = []
	left = []

	countries = set([result.get('CNTRY_ISSUE_ISO') for result in results])
	country = countries.pop()

	if country in country_exch_order.keys():
		exch_order = country_exch_order[country]
		ordered_exchange_hits = [min(results, key=lambda x: exch_order.get(x.get('EXCH_CODE')))]

		if ordered_exchange_hits:
			left.extend(ordered_exchange_hits)
		else:
			left = results
	else:
		left = results

	return deleted, left


def select_by_pfd_country_issue(results,country_exch_order):
	deleted = []
	left = []

	country_issue_order = {'GB':1,'DE':2,'CH':3,'AT':4,'IT':5,'US':0}
	default_rank = 123456789

	pfd_country_issue_rank = min([country_issue_order.get(hit.get('CNTRY_ISSUE_ISO'),default_rank) for hit in results ])
	pfd_country_issue_hits = [hit for hit in results if
									country_issue_order.get(hit.get('CNTRY_ISSUE_ISO'),default_rank) == pfd_country_issue_rank]

	if pfd_country_issue_hits:
		left = pfd_country_issue_hits
	else:
		left = results

	return deleted, left


def apply_removal_rules(rules_to_apply,results,country_exch_order):
	deleted = None
	left = results

	for r in rules_to_apply:
		deleted, left = r(left,country_exch_order)

		if len(left) == 1:
			left[0]['selection_rule'] = r.__name__
			left[0]['search_status'] = 'selection success'
			return left

		else:
			for l in left:
				l['selection_rule'] = r.__name__
				l['search_status'] = 'selection incomplete'


	return left

@ut.profile
def classify(results,dt_instrument_class):
	for result in results:
		luaction = dt.classify(dt_instrument_class,result)
		result['class_code'] = ''.join(luaction[k] for k in luaction.keys())

@ut.profile
def select_security(results, dt_instrument_class, country_exch_order):

	rules_to_apply = [select_asset_not_derivative,select_primary,select_local_line,select_by_pfd_country_issue,select_exch_country]#,]
	if results:
		classify(results,dt_instrument_class)
		return apply_removal_rules(rules_to_apply, results,country_exch_order)
	else:
		return None


if __name__ == "__main__":
	results = list()
	select_security(results)

	ut.print_prof_data()