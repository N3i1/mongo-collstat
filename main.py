from commands import Commands
from mongo import Mongodb


mon_conn = Mongodb()
conn = mon_conn.est_mongodb_connection()

mongo_db_names = mon_conn.get_db_names(conn, list_all='yes')

mongo_coll_names = mon_conn.get_collection_names(conn, mongo_db_names)

query = Commands(conn)

report = query.run_report(mongo_db_names, mongo_coll_names)






