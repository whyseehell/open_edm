import csv
import os


class ReaderTextwithHeader(object):
	'''

	'''

	def	__init__(self, path,filename,delimiter):
		self.delimiter_map = {'comma': ',', 'tab': '\t'}
		self.delimiter = self.delimiter_map[delimiter]
		self.headers = list()
		self.values = list()

		self.read_text_file_with_header(path, filename)


	def read_text_file_with_header(self, path, filename):

		input_file = os.path.join(path, filename)

		with open(input_file, "rb") as f:
			reader = csv.reader(f, delimiter=self.delimiter)
			self.headers = reader.next()
			self.values = [row for row in reader]