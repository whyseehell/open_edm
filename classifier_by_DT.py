
'''

Created on 09 may 2018

@author: yves.coupez

'''

import csv
from collections import namedtuple
from collections import OrderedDict


def read_lu(ffname):
	'''

	:param ffname: a fully qualified file name. The file is a CSV with condtion columns separated from action
					columns by an ampty column
	:return:
	'''

	category = namedtuple('category', 'luconditions luactions')
	answer = list()
	with open(ffname, 'rb') as file_data:
		reader = csv.DictReader(file_data)
		headers = reader.fieldnames
		separator_pos = headers.index('')
		conditions = headers[:separator_pos]
		actions = headers[separator_pos + 1:]
		for row in reader:
			lu_conditions = dict()
			for condition in conditions:
				val = row[condition].strip()
				if val and val != '*':
					if val.startswith(tuple(["<", ">", "==","<=",">="])):
						lu_conditions[condition] = val
					elif str(val).isdigit():
						lu_conditions[condition] = '== {}'.format(val)
					else:
						lu_conditions[condition] = '== \"{}\"'.format(val)
			if lu_conditions:
				lu_actions = OrderedDict()
				for action in actions:
					lu_actions[action] = row[action].strip()
				answer.append(category(lu_conditions,lu_actions))

		return answer


def string_val(val):
	if not str(val).isdigit():
		return '\"{}\"'.format(val)
	else:
		return val


def classify(lu_reference, row_facts):
	for entry in lu_reference:
		conditions = entry.luconditions
		term = ' and '.join('{} {}'.format(string_val(row_facts[k]), str(conditions[k])) for k in conditions.keys())
		if eval(term):
			return entry.luactions
	return {k: 'U' for k in entry.luactions.keys()}


def main():
	dt = read_lu('test_dt_fact.csv')

	facts = {'colA': 'qax', 'colB': 123, 'colC': 6}
	answer = classify(dt, facts)
	print facts, answer['output']

	facts = {'colA': 'qax', 'colB': 123, 'colC': 4}
	answer = classify(dt, facts)
	print facts, answer['output']

	facts = {'colA': 'row3', 'colB': 567, 'colC': 5}
	answer = classify(dt, facts)
	print facts, answer['output']


if __name__ == "__main__":
	main()
