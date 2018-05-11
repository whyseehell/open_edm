'''
Created on 09 may 2018

@author: yves.coupez
'''

import os
from collections import Iterator


class BoReader(Iterator):
	'''
	BoReader is an iterator sub-class that reads a Bloomberg back office file and return each row as Python dictionary
	'''


	def	__init__(self, path,filename):

		self.start_headers = "START-OF-FIELDS"
		self.end_headers = "END-OF-FIELDS"
		self.start_fields = "START-OF-DATA"
		self.end_fields = "END-OF-DATA"

		self.read_bo_standard_file(path, filename)


	def read_bo_standard_file(self,path,filename):
		input_file = os.path.join(path,filename)
		with open(input_file, "r") as file:
			data = file.read()
			headers = data[data.find(self.start_headers) + len(self.start_headers):data.find(self.end_headers)]
			fields = data[data.find(self.start_fields) + len(self.start_fields):data.find(self.end_fields)]
			headers = [x for x in headers.split("\n") if "#" not in x and x != ""]
			self.headers = ['col_zero', 'error_code', 'nbr_fields'] + headers
			self.values = [x.split("|") for x in fields.split("\n") if x]


	def next(self):
		if not self.values:
			raise StopIteration
		return dict(zip(self.headers, self.values.pop()))
