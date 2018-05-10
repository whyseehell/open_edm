from whoosh.index import create_in
from whoosh.fields import *
import os

def create_whoosh_idx(path,index_type):

	schema = Schema(isin=ID(stored=True),
					cusip=ID(stored=True),
					sedol=ID(stored=True),
					country_issue_iso=ID(stored=True),
					corp_ticker=ID(stored=True),
					exch_code=ID(stored=True),
					currency=ID(stored=True),
					raw_data=STORED)

	try:
		create_in(os.path.join(path, index_type), schema)
		success = True
	except:
		success = False

	return success