from pymongo import MongoClient
from pymongo import ReadPreference
import pymongo
import argparse


class Collstats(object):
    HOST = 'localhost'
    PORT = 27017
    client = []
    members = []
    coll_stats_results = {}
    index_stat_results = {}
    i = 0

    def __init__(self, args_results=None):

        if args_results.host is None:
            self.host = self.HOST
        else:
            self.host = args_results.host

        if args_results.port is None:
            self.port = self.PORT
        else:
            self.port = int(args_results.port)

        self.db_name = args_results.database_name
        self.coll_name = args_results.collection_name

        if args_results.discover is not None:
            self.client.append(MongoClient(self.host, self.port))
            discover = self.client[0]["admin"].command("replSetGetStatus", "admin")
            for value in discover['members']:
                if "SECONDARY" in value['stateStr']:
                    self.members.append(value['name'])
            for m in self.members:
                h, p = m.split(":")
                h = "192.168.56.101"
                self.client.append(MongoClient(host=h, port=int(p), read_preference=ReadPreference.SECONDARY))
        else:
            self.client.append(MongoClient(self.host, self.port))

        ''' 
        Check to ensure db and collection exists
        '''
        db_names = self.client[0].database_names()

        if self.db_name not in db_names:
            print("Error no db")
            exit(1)

        collection_names = self.client[0][self.db_name].collection_names()

        if self.coll_name not in collection_names:
            print("No colleciton name")
            exit(1)

    '''
    Funtions
    '''

    def run_collStats(self):

        for self.i in range(len(self.client)):
            coll_stats = self.client[self.i][self.db_name].command("collstats", self.coll_name)
            self.coll_stats_results[self.client[self.i].address] = coll_stats

    def print_collStat(self):
        for self.i, v in self.coll_stats_results.items():
            print(self.i, v)

    def print_wiredTiger_info(self):

        print('{:30}'.format(" " + self.coll_stats_results[self.client[self.i].address]['ns']),
              '{:10}'.format(self.coll_stats_results[self.client[self.i].address]["wiredTiger"]["cache"][
                                 "bytes currently in the cache"]),
              '{:10}'.format(self.coll_stats_results[self.client[self.i].address]["wiredTiger"]["cache"][
                                 "tracked dirty bytes in the cache"]),
              '{:10}'.format(
                  self.coll_stats_results[self.client[self.i].address]["wiredTiger"]["cache"]["pages read into cache"]),
              '{:10}'.format(self.coll_stats_results[self.client[self.i].address]["wiredTiger"]["cache"][
                                 "pages written from cache"]),
              '{:10}'.format(self.coll_stats_results[self.client[self.i].address]["wiredTiger"]["cache"][
                                 "pages requested from the cache"]))

    def print_index_info(self):

        self.index_stat_results[self.client[self.i].address] = self.coll_stats_results[self.client[self.i].address][
            "indexDetails"]

        for key in self.index_stat_results[self.client[self.i].address]:
            indx_n = key
            pipeline = [{"$indexStats": {}}]
            query = pymongo.collection.Collection(self.client[self.i][self.db_name], name=self.coll_name)
            cursor = query.aggregate(pipeline)
            res = list(cursor)
        for v in res:
            name = v['name']
            self.index_stat_results[name] = v

        print('{:30}'.format("  " + key),
              '{:10}'.format(self.index_stat_results[self.client[self.i].address][indx_n]["cache"][
                                 "bytes currently in the cache"]),
              '{:10}'.format(self.index_stat_results[self.client[self.i].address][indx_n]["cache"][
                                 "tracked dirty bytes in the cache"]),
              '{:10}'.format(self.index_stat_results[self.client[self.i].address][indx_n]["cache"][
                                 "pages read into cache"]),
              '{:10}'.format(self.index_stat_results[self.client[self.i].address][indx_n]["cache"][
                                 "pages written from cache"]),
              '{:10}'.format(self.index_stat_results[self.client[self.i].address][indx_n]["cache"][
                                 "pages requested from the cache"]),
              '{:10}'.format(self.index_stat_results[indx_n]['accesses']['ops']))

    def print_all_results(self):

        for self.i in range(len(self.client)):
            print(self.client[self.i].address)
            print('{:30} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}'.format("name", "used", "dirty", "pri", "pwf", "prq",
                                                                           "ops"))
            self.print_wiredTiger_info()
            self.print_index_info()


def main():
    parser = argparse.ArgumentParser(description='dba-mongodb-reporting')
    parser.add_argument('--host', help='hostname', action='store', dest='host', default=None)
    parser.add_argument('--port', help='port number', action='store', dest='port', default=None, )
    parser.add_argument('--db', help='database_name', action='store', dest='database_name', required=True)
    parser.add_argument('--coll', action='store', dest='collection_name', help='collection name', required=True)
    parser.add_argument('--discover', action='store_true', dest='discover', help='Discover repset members',
                        default=None)
    arg_results = parser.parse_args()

    results = Collstats(arg_results)
    results.run_collStats()
    results.print_all_results()


if __name__ == '__main__':
    main()
