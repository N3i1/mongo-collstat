from pymongo import MongoClient
from pymongo import ReadPreference

pri_conn = MongoClient()

discover = pri_conn["admin"].command("replSetGetStatus", "admin")

members = []

for value in discover['members']:
    if "SECONDARY" in value['stateStr']:
        members.append(value['name'])

sec_conns = []

for m in members:
    h, p = m.split(":")
    sec_conns.append(MongoClient(host=h, port=int(p), read_preference=ReadPreference.SECONDARY))

db_names = pri_conn.list_database_names()

if "admin" in db_names:
    db_names.remove("admin")
if "config" in db_names:
    db_names.remove("config")
if "local" in db_names:
    db_names.remove("local")

coll_names_dict = {}

coll = []

for i in db_names:
    coll = pri_conn[i].collection_names()
    coll_names_dict[i] = coll

coll_stats_results = {}

print('{:30} {:6} {:6} {:6} '.format("CollName", "BtyesUsed", "ReadIn", "WrittenIn"))

for db_name in db_names:

    print(pri_conn.address)

    for collection_name in coll_names_dict[db_name]:

        coll_stats = pri_conn[db_name].command("collstats", collection_name)
        coll_stats_results[collection_name] = coll_stats

        print('{:30}'.format("  " + coll_stats_results[collection_name]['ns']),
              '{:6}'.format(coll_stats_results[collection_name]["wiredTiger"]["cache"]["bytes currently in the cache"]),
              '{:6}'.format(coll_stats_results[collection_name]["wiredTiger"]["cache"]["bytes read into cache"]),
              '{:6}'.format(coll_stats_results[collection_name]["wiredTiger"]["cache"]["bytes written from cache"]))
        coll_index = {}
        coll_index[collection_name] = coll_stats_results[collection_name]["indexDetails"]

        for key, value in coll_index[collection_name].items():
            indx_n = key
            print('{:30}'.format("  " + key),
                  '{:6}'.format(coll_index[collection_name][indx_n]["cache"]["bytes currently in the cache"]),
                  '{:6}'.format(coll_index[collection_name][indx_n]["cache"]["bytes read into cache"]),
                  '{:6}'.format(coll_index[collection_name][indx_n]["cache"]["bytes written from cache"]))

# Secondarys
for i in range(len(sec_conns)):

    for db_name in db_names:

        print(sec_conns[i].address)

        for collection_name in coll_names_dict[db_name]:

            coll_stats = sec_conns[i][db_name].command("collstats", collection_name)
            coll_stats_results[collection_name] = coll_stats
            print('{:30}'.format(coll_stats_results[collection_name]['ns']),
                  '{:6}'.format(
                      coll_stats_results[collection_name]["wiredTiger"]["cache"]["bytes currently in the cache"]),
                  '{:6}'.format(coll_stats_results[collection_name]["wiredTiger"]["cache"]["bytes read into cache"]),
                  '{:6}'.format(coll_stats_results[collection_name]["wiredTiger"]["cache"]["bytes written from cache"]))
            coll_index = {}
            coll_index[collection_name] = coll_stats_results[collection_name]["indexDetails"]

            for key, value in coll_index[collection_name].items():
                indx_n = key
                print('{:30}'.format(key),
                      '{:6}'.format(coll_index[collection_name][indx_n]["cache"]["bytes currently in the cache"]),
                      '{:6}'.format(coll_index[collection_name][indx_n]["cache"]["bytes read into cache"]),
                      '{:6}'.format(coll_index[collection_name][indx_n]["cache"]["bytes written from cache"]))
