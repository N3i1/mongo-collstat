import pymongo

class Commands(object):

    def __init__(self, conn=None):
        if conn == None:
            #TODO Put correct error
            raise TypeError("Commands needs a db connection")
            exit()
        else:
            self.conn = conn


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
                coll_stats_parent_doc = data.parse_collstat_parent_doc(mongodb_collstats_metrics[db_name][collection_name],
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
        query = pymongo.collection.Collection(self.conn[db_name], name=collection_name)
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
                coll_stats = self.conn[db_name].command("collstats", collection_name)
                coll_stats_results[collection_name] = coll_stats
        return coll_stats_results


    def run_report(self, mongo_db_names, mongo_coll_names):
        # TODO Add options so you can pick which statst to run
        # TODO Add flags so you can specify just one db and/or collection
        Child = ["LSM", "block-manager", "btree", "cache", "compression", "cursor", "reconciliation", "session",
                 "transaction"]
        coll_stats_results = Commands.run_collstats(self, mongo_db_names, mongo_coll_names)
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
                    coll_stats_parent_doc = Commands.parse_collstat_parent_doc(self, coll_stats_results[collection_name],
                                                                      "wiredTiger")

                    for v in Child:
                        print("\t", v, ":")
                        coll_stats_child_doc = Commands.parse_collstat_sub_docs(self, coll_stats_parent_doc, v)
                        for key, value in coll_stats_child_doc.items():
                            if value != 0:
                                print("\t\t", key, ":", value)

                    print("\t\t\t\n\nIndex statistics for collection:", collection_name)

                    coll_stats_parent_doc = Commands.parse_collstat_parent_doc(self, coll_stats_results[collection_name],
                                                                      "indexDetails")

                    for key, value in coll_stats_parent_doc.items():
                        indx_name = key
                        print("\t\t\nINDEX NAME:", key, "\n")

                        coll_stats_index_doc = coll_stats_parent_doc[key]
                        for v in Child:
                            print("\t", v, ":")
                            coll_stats_child_doc = Commands.parse_collstat_sub_docs(self, coll_stats_index_doc, v)
                            for key, value in coll_stats_child_doc.items():
                                if value != 0:
                                    print("\t\t\t", key, ":", value)
                        print("\t\t\nIndex usage:", "\n")
                        # Call indexStats on a per db basis
                        # TODO move this call out of this module
                        index_stat_results = Commands.run_indexstats(self, db_name, collection_name)
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