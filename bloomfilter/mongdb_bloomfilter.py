import asyncio
import json
import sys
import time

import _pickle as cPickle
import motor.motor_asyncio
import pymongo
from bloom_filter import BloomFilter


# from pybloomfilter import BloomFilter

class mongodb_synic():

    def __init__(self, host, username, password, port, authSource, db_name, collection_name):
        """
        :param host: ip
        :param username:
        :param password:
        :param port:
        :param authSource:'admin'
        :param db_name:
        :param collection_name:
        """
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.authSource = authSource
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = pymongo.MongoClient(self.host, username=self.username, password=self.password, port=self.port,
                                          authSource=self.authSource)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    def do_drop(self):
        """
        # empty a collections of db
        :return:
        """
        self.collection.drop()

    def do_add(self, string_lists):
        """
        :param string_lists: eg. {lib1_fn: string_list, lib1_1g: string_list, lib1_2g: string_list, ..., }
        :return:
        """
        try:
            for key in string_lists:
                document = {}
                bf = BloomFilter(max_elements=2 ** 16, error_rate=0.01)
                [bf.add(element) for element in string_lists.get(key)]
                bf_pickle = cPickle.dumps(bf)
                document[key] = bf_pickle
                self.collection.save(document)
                size = sys.getsizeof(document[key])
                print("save bloom tree into mongoDB: %s \t size is %f M" % (key, size / 1024 / 1024))
        except Exception as e:
            print(e)

    def do_query(self, string_features):
        """
        :param string_features: [str1, str2, str3, str4, ..., ]
        :return: {lib1_fn: count1, lib1_1g: count2, lib1_2g: count3, ..., }
        """
        result = {}
        cursor = self.collection.find({})
        for document in cursor:
            index = 0
            for key in document:
                if index == 1:
                    bf_pickle = document.get(key)
                    bf = cPickle.loads(bf_pickle)
                    count = 0
                    for feature in string_features:
                        if feature in bf:
                            count += 1
                    result[key] = count
                index += 1
        return result

    def do_delete_many(self, documents):
        """
        # delete documents in collections of mongoDB
        :param documents:
        :return:
        """
        self.collection.delete_many(documents)

    def do_count(self):
        """
        # count the number of documents in mongoDB
        :return:
        """
        return self.collection.count_documents({})

    def do_replace(self, id, document):
        """
        # replace the content of OBjectID in mongoDB
        :param id:
        :param document:
        :return:
        """
        old_document = self.collection.find()
        _id = old_document['_id']
        self.collection.replace_one({'_id': _id}, document)


class mongodb_asynic():

    def __init__(self, host, username, password, port, authSource, db_name, collection_name):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.authSource = authSource
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            'mongodb://%s:%s@%s:%s' % (self.username, self.password, self.host, self.port))
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    async def do_drop(self):
        await  self.collection.drop()

    async def do_add(self, string_lists):
        try:
            for key in string_lists:
                document = {}
                bf = BloomFilter(max_elements=2 ** 16, error_rate=0.01)
                [bf.add(element) for element in string_lists.get(key)]
                bf_pickle = cPickle.dumps(bf)
                document[key] = bf_pickle
                size = sys.getsizeof(document[key])
                await self.collection.insert_one(document=document)
                print("asynic save bloom tree into mongDB: %s \t size is %f" % (key, size / 1024 / 1024))
        except Exception as e:
            print(e)

    async def do_count(self):
        return await self.collection.count_documents({})

    async def do_query(self, string_features, document_count):
        result = {}
        cursor = self.collection.find({})
        for document in await cursor.to_list(document_count):
            index = 0
            for key in document:
                if index == 1:
                    bf_pickle = document.get(key)
                    bf = cPickle.loads(bf_pickle)
                    count = 0
                    for feature in string_features:
                        if feature in bf:
                            count += 1
                    result[key] = count
                index += 1
        return result


if __name__ == '__main__':
    # load json_file to CPU Memory
    file_path = './bt_dict.json'
    file_read = open(file_path, 'r')
    file_json = json.load(file_read)
    test_path = './test.txt'
    test_strings = []
    with open(test_path, 'r') as test_read:
        for line in test_read.readlines():
            test_strings.append(line.split(',')[0].split('\"')[1])
    print(test_strings)

    mongodb_synic = mongodb_synic(host='localhost', username='root', password='example', port=27018, authSource='admin',
                                  db_name='synic_bloom_filter', collection_name='filters')
    mongodb_synic.do_drop()
    start_time = time.time()
    mongodb_synic.do_add(file_json)
    print("write files, size 0.15M / file, time is %f" % (time.time() - start_time))
    start_time = time.time()
    result = mongodb_synic.do_query(test_strings)
    print("find_result \n %s" % result)
    print("query time is :%f" % (time.time() - start_time))

    mongodb_asynic = mongodb_asynic(host='localhost', username='root', password='example', port=27018,
                                    authSource='admin',
                                    db_name='asynic_bloom_filter', collection_name='filters')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mongodb_asynic.do_drop())
    start_time = time.time()
    loop.run_until_complete(mongodb_asynic.do_add(file_json))
    print("write files, size 0.15M / file, time is %f" % (time.time() - start_time))
    start_time = time.time()
    document_count = loop.run_until_complete(mongodb_asynic.do_count())
    result = loop.run_until_complete(mongodb_asynic.do_query(test_strings, document_count))
    print("find_result \n %s" % result)
    print("query time is :%f" % (time.time() - start_time))
