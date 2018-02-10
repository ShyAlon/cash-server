import pymongo
import datetime
import pprint


class Database:
    def __init__(self):
        self.data = []
        self.connection_string = "mongodb://cash:qwerty123456cash@cluster0-shard-00-00-fsl8x.mongodb.net:27017" + \
                        ",cluster0-shard-00-01-fsl8x.mongodb.net:27017" + \
                        ",cluster0-shard-00-02-fsl8x.mongodb.net:27017" + \
                        "/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin"
        self.client = pymongo.MongoClient(self.connection_string)
        self.db = self.client.test
        # self.posts = self.db.posts
        # for index in range(0, 3):
        #     post = {"author": "Shy",
        #             "text": "My first blog post!",
        #             "tags": ["mongodb", "python", "pymongo"],
        #             "date": datetime.datetime.utcnow()}
        #     post_id = posts.insert_one(post).inserted_id
        #     print(post_id)
        # self.collections = db.collection_names(include_system_collections=False)
        # print self.collections
        # # pprint.pprint(posts.find_one({"author": "Mush"}))
        # for post in self.posts.find({"author": "Shy"}):
        #     pprint.pprint(post)

    def insert_results(self, results):
        result = self.db.results.insert_many(results)
        print (result.inserted_ids)

    def read_data(self, index):
        # self.collections = self.db.collection_names(include_system_collections=False)
        target = 0
        if self.data == []:
            for result in self.db.results.find({"data_type": "joined_stock_data"}).sort([
                ("date_and_time", pymongo.DESCENDING)
            ]):
                if target >= index:
                    self.data = result
                    print("Getting result #{}".format(target))
                    return result
                else:
                    target += 1


    def insert_result(self, result):
        retval = self.db.results.insert_one(result)
        #print (retval.inserted_id)

    def pull_last_result(self):
        sort = {'_id': pymongo.DESCENDING}
        return self.db.results.find({"Data Type":"Raw Stock Data"}, limit=1).sort([("Time Stamp",pymongo.DESCENDING)])[0]
