import bbg_standard_file_reader as bbgr
from datetime import datetime
from whoosh import index
import os

import create_index as cidx




def main(index_base_path,vendor_code,index_type,data_file_path,data_file_list):
	if cidx.create_whoosh_idx(os.path.join(index_base_path,vendor_code),index_type):
		print "sucess index creation at -->: ",os.path.join(index_base_path,vendor_code,index_type)
	else:
		print "failed index creation at -->: ", os.path.join(index_base_path, vendor_code, index_type)
		quit()


	if index.exists_in(os.path.join(index_base_path,vendor_code,index_type)):
		ix = index.open_dir(os.path.join(index_base_path,vendor_code,index_type))

		for file in file_list:
			print "indexing file : ", file
			idx_writer = ix.writer()
			data_reader = bbgr.BoReader(data_file_path, file)
			for iRecord in data_reader:
				idx_writer.add_document(isin=unicode(iRecord.get('ID_ISIN', None), "utf-8"),
										sedol=unicode(iRecord.get('ID_SEDOL1', None), "utf-8"),
										cusip=unicode(iRecord.get('ID_CUSIP', None), "utf-8"),
										country_issue_iso=unicode(iRecord.get('CNTRY_ISSUE_ISO', None), "utf-8"),
										corp_ticker=unicode(iRecord.get('EQY_PRIM_SECURITY_TICKER', None), "utf-8"),
										exch_code=unicode(iRecord.get('EXCH_CODE', None), "utf-8"),
										currency=unicode(iRecord.get('CRNCY', None), "utf-8"),
										raw_data=iRecord)
			idx_writer.commit()

	else:
		print "failed to open index at -->: ", os.path.join(index_base_path, vendor_code, index_type)
		quit()
	quit()


if __name__ == "__main__":
	index_base_path = '/Users/yvescoupez/PycharmProjects/data/Indices'
	vendor_code = 'ven_bbg'
	index_type = 'Instrument'

	data_path = '/Users/yvescoupez/PycharmProjects/data/bo_files'
	file_list = ['equity_namr.out.gz.enc.20170824', 'equity_euro.out.gz.enc.20170824',
				 'equity_asia1.out.gz.enc.20170825', 'equity_asia2.out.gz.enc.20170824',
				 'equity_lamr.out.gz.enc.20170824']
	main(index_base_path,vendor_code,index_type,data_path,file_list)

