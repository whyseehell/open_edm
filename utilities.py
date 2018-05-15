"""
Created on 09 may 2018

@author: yves.coupez
"""

import os
import csv
from collections import defaultdict
import collections
import time
from functools import wraps


def load_file_in_list(ffname):
	with open(os.path.join('/Users/yvescoupez/Documents/GitHub/open_edm/data','fields_to_extract.txt'),'rb') as fi:
		answer = list(fi.read().split('\n'))
	return answer
	
# ===============================================================================


def load_country_exch_order(ffname):
	'''

	:param ffname: fully qualified filename of the file holding the ordered hierachy of exchnagce by country
	:return: a dictionay keyed on country and exchange with each entry holdign the rank of the excahnge int he country
	'''
	country_exch_order = defaultdict(dict)

	with open(ffname, 'rb') as file_data:
		reader = csv.DictReader(file_data)
		for row in reader:
			country_exch_order[row['CNTRY_ISSUE_ISO']][row['EXCH_CODE']] = row['order']

	return country_exch_order


# ===============================================================================


PROF_DATA = collections.OrderedDict()


def profile(fn):
	@wraps(fn)
	def with_profiling(*args, **kwargs):
		start_time = time.time()

		ret = fn(*args, **kwargs)

		elapsed_time = time.time() - start_time

		name = fn.func_code.co_filename + ' > ' + fn.__name__

		if name not in PROF_DATA:
			PROF_DATA[name] = [0, []]
		PROF_DATA[name][0] += 1
		PROF_DATA[name][1].append(elapsed_time)

		return ret

	return with_profiling


def print_prof_data():
	for fname, data in PROF_DATA.items():
		max_time = max(data[1])
		avg_time = sum(data[1]) / len(data[1])
		print "Function %s called %d times. " % (fname, data[0]),
		print 'Execution time max: %.3f, average: %.3f' % (max_time, avg_time)


def clear_prof_data():
	global PROF_DATA
	PROF_DATA = {}


# ===============================================================================




def main():
	path = '/Users/yvescoupez/PycharmProjects/data/reference'
	filename = 'country_exch_order.csv'
	country_exch_order = load_country_exch_order(os.path.join(path,filename))

	print country_exch_order['DE']

# ===============================================================================

if __name__ == "__main__":
	# config, logConfig = pllib.pl_utils.initialize()
	main()