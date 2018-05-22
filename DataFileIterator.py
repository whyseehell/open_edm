'''
Created on 09 may 2018

@author: yves.coupez
'''

import os
from collections import Iterator
import Reader_BOStandard as rbggs
import Reader_TextwithHeader as txt

class DataFileIterator(Iterator):
	'''
	'''


	def	__init__(self, file_type,path,filename):

		self.path = path
		self.filemame = filename
		self.headers = list()
		self.values = list()
		self.dispatch_map = {'BOStandard': self.BOStandard_loader,
		                     'TextHeaderComma': self.TextHeaderComma_loader,
		                     'TextHeaderTab': self.TextHeaderTab_loader}
		self.dispatch(file_type)

	def next(self):
		if not self.values:
			raise StopIteration
		return dict(zip(self.headers, self.values.pop()))

	def BOStandard_loader(self):
		reader = rbggs.Reader_BOStandard(self.path, self.filemame)

		self.headers = reader.headers
		self.values = reader.values

	def TextHeaderComma_loader(self):
		reader = txt.ReaderTextwithHeader(self.path, self.filemame,'comma')

		self.headers = reader.headers
		self.values = reader.values

	def TextHeaderTab_loader(self):
		reader = txt.ReaderTextwithHeader(self.path, self.filemame,'tab')

		self.headers = reader.headers
		self.values = reader.values

	def dispatch(self, arg):
		bound = self.dispatch_map[arg].__get__(self, type(self))
		bound()

