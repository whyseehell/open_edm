'''
Created on 14 may 2018

@author: yves.coupez
'''

import os
import unittest
import classifier_by_DT as dt

class TestClassifier(unittest.TestCase):

	def setUp(self):
		self.dt_ref_dt = dt.read_lu(os.path.join('./data/reference', 'DT_test_ref_data.csv'))

	def test_case_1(self):
		facts = {'cond_1': 'abc', 'cond_2': 'def', 'cond_3': 'hijk'}
		self.assertEqual(dt.classify(self.dt_ref_dt, facts), {'action_1': 'all_text 3 condtions', 'action_2': 'row1'})

	def test_case_2(self):
		facts = {'cond_1': 'def', 'cond_2': 'abc', 'cond_3': 'hijk'}
		self.assertEqual(dt.classify(self.dt_ref_dt, facts), {'action_1': 'all_text 2 condtions', 'action_2': 'row2'})

	def test_case_3(self):
		facts = {'cond_1': 'def', 'cond_2': 2, 'cond_3': 3}
		self.assertEqual(dt.classify(self.dt_ref_dt, facts), {'action_1': 'numeric equal', 'action_2': 'row3'})

	def test_case_4(self):
		facts = {'cond_1': 2, 'cond_2': 3, 'cond_3': 4}
		self.assertEqual(dt.classify(self.dt_ref_dt, facts), {'action_1': 'all numbers eq', 'action_2': 'row4'})

	def test_case_5(self):
		facts = {'cond_1': 'abc', 'cond_2': 12, 'cond_3': 8}
		self.assertEqual(dt.classify(self.dt_ref_dt, facts), {'action_1': 'numbers compare 1', 'action_2': 'row5'})

	def test_case_6(self):
		facts = {'cond_1': 'abc', 'cond_2': 12, 'cond_3': 18}
		self.assertEqual(dt.classify(self.dt_ref_dt, facts), {'action_1': 'numbers compare 2', 'action_2': 'row6'})

	def test_case_7(self):
		facts = {'cond_1': 'qaz', 'cond_2': 12, 'cond_3': 'wer'}
		self.assertEqual(dt.classify(self.dt_ref_dt, facts), {'action_1': 'numbers compare 3', 'action_2': 'row7'})

	def test_case_8(self):
		facts = {'cond_1': 'qaz', 'cond_2': 2, 'cond_3': 'wer'}
		self.assertEqual(dt.classify(self.dt_ref_dt, facts), {'action_1': 'numbers compare 4', 'action_2': 'row8'})

	def test_case_9(self):
		facts = {'cond_1': 'qaz', 'cond_2': 99, 'cond_3': 5}
		self.assertEqual(dt.classify(self.dt_ref_dt, facts), {'action_1': 'numbers compare 5', 'action_2': 'row9'})


	def test_case_99(self):
		facts = {'cond_1': 'pass trhu', 'cond_2': 'abc', 'cond_3': 'hijk'}
		self.assertEqual(dt.classify(self.dt_ref_dt, facts), {'action_1': 'U', 'action_2': 'U'})

if __name__ == '__main__':
	unittest.main()
