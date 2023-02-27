import random
import time 
from datetime import datetime
import pymongo

def insert_benchmark(mycol):
    total_time = 0
    num_records = 100000
    for i in range(num_records):
        camera_id = random.randint(1,20)
        event_id = random.randint(1,7)
        duration = random.randint(1,500)
        str_time = str(datetime.fromtimestamp(time.time()+i))
        mydict = { "camera_id": camera_id, "event_id": event_id, "ts": str_time, "duration": duration, "path":  'temp/abc.m3u8'}
        start = time.time()
        x = mycol.insert_one(mydict)
        total_time += time.time() - start
        print("affected_rows", x) 
        if i %1000==0:
            print('done inserted', i)
    print('Speed insert time/record =', total_time/(num_records))

def query_benchmark(mycol):
    # camera_id = random.randint(1,20)
    # event_id = random.randint(1,7)
    # start = time.time()
    # mydoc = mycol.find(myquery)
    # total_time += time.time() - start
    # for x in mydoc:
    #     print(x)

    total_time = 0
    num_sql = 1000
    for i in range(num_sql):
        camera_id = random.randint(1,20)
        event_id = random.randint(1,7)
        myquery = { "camera_id": camera_id, "event_id":event_id, "ts":{"$gt":'2023-02-27 11:23:55.821876'}}
        myquery = { "camera_id": camera_id, "event_id":event_id}
        start = time.time()
        mydoc = mycol.count_documents(myquery)
        total_time += time.time() - start
        print(mydoc)
    print('Speed sql time/query =', total_time/num_sql)

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
# create database
mydb = myclient["mydatabase"]
# create collection - like table
mycol = mydb["eventcamera"]
# insert benchmark
# insert_benchmark(mycol)
query_benchmark(mycol)

myclient.close()
