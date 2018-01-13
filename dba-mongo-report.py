from mongo import Mongodb
import argparse

parser = argparse.ArgumentParser(description='dba-mongodb-reporting')
parser.add_argument('-db', help='db name', action='store', dest='db_name', default=None, )
parser.add_argument('-coll', action='store', dest='coll_name', help='collection name', default=None)
arg_results = parser.parse_args()


mon_conn = Mongodb()

mongo_db_names = mon_conn.get_db_names(arg_results, list_all='yes')
#print(mongo_db_names)
mongo_coll_names = mon_conn.get_collection_names(arg_results, mongo_db_names)
#print(mongo_coll_names)

mongo_coll_stat = mon_conn.run_collstats(mongo_db_names, mongo_coll_names)
#print(mongo_coll_stat)

#mongo_ss = mon_conn.run_dbServerStatus()
#print(mongo_ss)

report = mon_conn.run_report(mongo_db_names, mongo_coll_names)

