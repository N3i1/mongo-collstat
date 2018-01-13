from pymongo import MongoClient
import pymongo
"""
 
"""


class Mongodb(object):
    HOST = "localhost"
    PORT = int(27017)

    def __init__(self, args_results=None):

        if args_results.host is None:
            self.host = self.HOST
        else:
            self.host = args_results.host
        if args_results.port is None:
            self.port = self.PORT
        else:
            self.port = int(args_results.port)
        if not isinstance(self.port, int):
            raise TypeError("port must be an instance of int")

        try:
            self.mongo_conn = Mongodb.est_mongodb_connection(self)

        except ConnectionError as e:
            print(e)

    def get_db_names(self, arg_results, list_all="no"):
        """

        :param mongodb_conn:
        :return: list of database names
        """
        try:
            db_names = list(self.mongo_conn.database_names())
        except EnvironmentError:
            print("no db names")

        if arg_results.db_name is not None:
            try:
                if arg_results.db_name in db_names:
                    db_names.clear()
                    db_names.append(arg_results.db_name)
            except ValueError:
                print("db not exist")

        if list_all is "no":
            if "admin" in db_names:
                db_names.remove("admin")
            if "config" in db_names:
                db_names.remove("config")
            if "local" in db_names:
                db_names.remove("local")
        return db_names

    def get_index_names(self, parent_stat):
        """

        :param parent_stat:
        :return: list of index names
        """
        # TODO splice first entry which is the index name for the given collection
        return list(parent_stat)

    def get_collection_names(self, arg_results, mongodb_db_names):

        """

        :param mongodb_conn:
        :param mongodb_db_names:
        :return:
        """
        coll_names_dict = {}
        coll = []
        for i in mongodb_db_names:
            coll = self.mongo_conn[i].collection_names()
            coll_names_dict[i] = coll

        # When we get to here we have a dict(list[])
        if arg_results.coll_name is not None:
            for key in coll_names_dict.keys():
                if arg_results.coll_name not in coll_names_dict[key]:
                    print("not found")
                    exit()
                else:
                    if arg_results.coll_name in coll_names_dict[key]:
                        # the collecton is in this key -
                        coll_names_dict = {}
                        coll.clear()
                        coll.append(arg_results.coll_name)
                        coll_names_dict[key] = coll
                    #return coll_names_dict
        return coll_names_dict

    def est_mongodb_connection(self):
        mongo_conn = MongoClient(host=self.host, port=self.port)
        return mongo_conn

    def print_indexstats_results(self, index_stat_results):
        pass

    def print_collstat_results(self, mongodb_collstats_metrics):
        # TODO Add in a flag to speficy DB name
        Child = ["LSM", "block-manager", "btree", "cache", "compression", "cursor", "reconciliation", "session",
                 "transaction"]
        for db_name in mongodb_collstats_metrics:
            print(db_name)
            for collection_name in mongodb_collstats_metrics[db_name]:
                print("\t", collection_name)
                coll_stats_parent_doc = data.parse_collstat_parent_doc(
                    mongodb_collstats_metrics[db_name][collection_name],
                    "wiredTiger")
                for v in Child:
                    print("\t##", v, ":")
                    coll_stats_child_doc = data.parse_collstat_sub_docs(coll_stats_parent_doc, v)
                    for key, value in coll_stats_child_doc.items():
                        if value != 0:
                            print("\t\t", key, ":", value)

    def run_indexstats(self, db_name, collection_name):
        """

        :param mongodb_conn:
        :param db_name:
        :param collection_name:
        :return: {index_name : index_stats_doc }
        """
        # TODO Either specify a single db and/or collection, or by default it will call for each collection in a db
        pipeline = [{"$indexStats": {}}]
        index_stat_results = {}
        query = pymongo.collection.Collection(self.mongo_conn[db_name], name=collection_name)
        cursor = query.aggregate(pipeline)
        res = list(cursor)
        for v in res:
            name = v['name']
            index_stat_results[name] = v
        return index_stat_results

    def run_collstats(self, mongo_db_names, mongodb_coll_names):
        """
        Return a dict() =  { collection_name: collection_stats{} }
        """
        coll_stats_results = {}
        for db_name in mongo_db_names:
            for collection_name in mongodb_coll_names[db_name]:
                coll_stats = self.mongo_conn[db_name].command("collstats", collection_name)
                coll_stats_results[collection_name] = coll_stats
        return coll_stats_results

    def run_dbServerStatus(self):
        stat = self.mongo_conn["admin"].command("serverStatus")
        return stat

    def run_report(self, mongo_db_names, mongo_coll_names):
        # TODO Add options so you can pick which statst to run
        # TODO Add flags so you can specify just one db and/or collection
        """Child = ["LSM", "block-manager", "btree", "cache", "compression", "cursor", "reconciliation", "session",
                 "transaction"]"""
        coll_stats_results = Mongodb.run_collstats(self, mongo_db_names, mongo_coll_names)
        for db_name in mongo_coll_names:

            print("!-------------------- MONGODB Collection STATS --------------------!")
            print("\n\t\tDATABASE NAME:", db_name)
            for collection_name in mongo_coll_names[db_name]:
                if collection_name in coll_stats_results:
                    print("\n\tCollection Name:", collection_name)
                    print("\tTotal size in memory:", coll_stats_results[collection_name]['size'])
                    print("\tTotal amount of storage allocated:", coll_stats_results[collection_name]['storageSize'])
                    print("\tTotal number of documents:", coll_stats_results[collection_name]['count'])
                    # print("\tAvg size of object in collection:", coll_stats['avgObjSize'])
                    print("\tTotal amount of storageallocated for documents",
                          coll_stats_results[collection_name]['storageSize'])
                    print("\tTotal amount of storage used by Indexes:",
                          coll_stats_results[collection_name]['totalIndexSize'])

                    print("\n\t\t\tWired Tiger Storage Stat:\n")
                    coll_stats_parent_doc = Mongodb.parse_collstat_parent_doc(self, coll_stats_results[collection_name],
                                                                              "wiredTiger")
                    Child = ["btree", "cache", "cursor", "session"]
                    for v in Child:
                        print("\t", v, ":")
                        coll_stats_child_doc = Mongodb.parse_collstat_sub_docs(self, coll_stats_parent_doc, v)
                        for key, value in coll_stats_child_doc.items():
                            if value != 0:
                                print("\t\t", key, ":", value)

                    print("\t\t\t\n\nIndex statistics for collection:", collection_name)

                    coll_stats_parent_doc = Mongodb.parse_collstat_parent_doc(self, coll_stats_results[collection_name],
                                                                              "indexDetails")

                    for key, value in coll_stats_parent_doc.items():
                        indx_name = key
                        print("\t\t\nINDEX NAME:", key, "\n")

                        coll_stats_index_doc = coll_stats_parent_doc[key]
                        Child = ["btree", "cache"]
                        for v in Child:
                            print("\t", v, ":")
                            coll_stats_child_doc = Mongodb.parse_collstat_sub_docs(self, coll_stats_index_doc, v)
                            for key, value in coll_stats_child_doc.items():
                                if value != 0:
                                    print("\t\t\t", key, ":", value)
                        print("\t\t\nIndex usage:", "\n")
                        # Call indexStats on a per db basis
                        # TODO move this call out of this module
                        index_stat_results = Mongodb.run_indexstats(self, db_name, collection_name)
                        for key, value in index_stat_results.items():
                            if key == indx_name:
                                print("\tNo of Ops:", value['accesses']['ops'])

    def parse_collstat_parent_doc(self, collstat_stats, parent_stat_name):
        return dict(collstat_stats[parent_stat_name])

    def parse_collstat_sub_docs(self, stat_name, child_stat_name):
        return dict(stat_name[child_stat_name])

    def parse_index_doc(self, parent_stat, index_name):
        # TODO Handle a list of indexes
        return dict(parent_stat[index_name])

# if __name__ == "__main__":
