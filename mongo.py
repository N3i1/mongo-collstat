from pymongo import MongoClient

class Mongodb(object):

    HOST = "localhost"
    PORT = int(27017)

    def __init__(self, host=None, port=None, username=None, password = None):

        if host is None:
            self.host = self.HOST
        else:
            self.host = host
        if port is None:
            self.port = self.PORT
        else:
            self.port = port
        if not isinstance(self.port, int):
            raise TypeError("port must be an instance of int")
        if username is None:
            self.username = None
        if password is None:
            self.password = None


    def get_db_names(self, mongodb_conn, list_all="no"):
        """

        :param mongodb_conn:
        :return: list of database names
        """
        if list_all is "no":
            db_names = list(mongodb_conn.database_names())
            if "admin" in db_names:
                db_names.remove("admin")
            if "config" in db_names:
                db_names.remove("config")
            if "local" in db_names:
                db_names.remove("local")
        else:
            db_names = list(mongodb_conn.database_names())
        return list(db_names)


    def get_index_names(self, parent_stat):
        """

        :param parent_stat:
        :return: list of index names
        """
        # TODO splice first entry which is the index name for the given collection
        return list(parent_stat)


    def get_collection_names(self, mongodb_conn, mongodb_db_names):
        """

        :param mongodb_conn:
        :param mongodb_db_names:
        :return:
        """
        coll_names_dict = {}
        for i in mongodb_db_names:
            coll = []
            for c in mongodb_conn[i].collection_names():
                coll.append(c)
            coll_names_dict[i] = coll
        return coll_names_dict


    def est_mongodb_connection(self):
        if self.username and self.password == None:
            mongo_conn = MongoClient(host=self.host, port=self.port)
        else:
            mongo_conn = MongoClient(host=self.host, port=self.port, username=self.username, password=self.password)
        return mongo_conn
