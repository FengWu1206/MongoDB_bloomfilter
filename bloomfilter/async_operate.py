import asyncio
import json
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
import time

class CombineShop:
    def __init__(self, db: str, collection_read: str, collection_write: str, json_strings:list, batch: int = 200):
        """
        :param db:
        :param collection:
        :param collection_read:
        :param collection_write:
        :param batch:
        """

        self.batch = batch
        self.db = db
        self.collection_read = collection_read
        self.collection_write = collection_write
        self.queue = asyncio.Queue()  # 共享队列
        self.json_strings = json_strings
        self.finished = False  # 完成标志

    async def run(self):
        """入口"""
        #await asyncio.gather(self.read(), self.write(json_strings=json_string))
        await self.write()

    def connect(self):
        """连接"""
        client = AsyncIOMotorClient('localhost', username='root',
                                password='example', port=27018, authSource='admin')
        return client[self.db]

    async def read(self):
        """读，生产数据"""
        db = self.connect()
        async for shop in db[self.collection_read].find({}):
            shop['shop_id'] = str(shop['shop_id'])
            await self.queue.put({'filter': {'shop_id': shop['shop_id']}, 'update': {'$set': shop}})

        self.finished = True

    async def write(self):
        """写，消费数据"""

        db = self.connect()
        db[self.collection_write].create_index("library_id", unique=True)

        while not self.finished or not self.queue.empty():
            if len(self.json_strings) == self.batch:
                await db[self.collection_write].insert(self.json_strings)
                self.json_strings.clear()

        if self.json_strings:
            await db[self.collection_write].bulk_write(self.json_strings)


if __name__ == '__main__':
    file_path = '/home/wufeng/Downloads/atomix.json'
    file_read = open(file_path, 'r')
    file_json = json.load(file_read)
    file_jsons = []
    for index in range(3000):
        file_jsons.append(file_json.copy())
    start_time = time.time()
    c = CombineShop(db='bloom_tree', collection_read='collection_read', collection_write='collection_write', json_strings=file_jsons)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(c.run())
    print("write time: %f" %(time.time() - start_time))
