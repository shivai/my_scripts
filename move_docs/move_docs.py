from pymongo import MongoClient
import pymongo
import time

src_mongo_host = "10.104.70.11:27019"
src_collection = "connection_log"
src_db = "IBSng"

dst_mongo_host = "127.0.0.1:27019"
dst_collection = "connection_log"
dst_db = "IBSng"

from datetime import datetime
start_logout_time = datetime(2019, 6, 14, 0, 0, 0)
end_logout_time = datetime(2019, 6, 18, 0, 0, 0)

try:
    src_conn = MongoClient(src_mongo_host)
    print("Source : Connected successfully!!!")
except:
    print("Source : Could not connect to MongoDB")


try:
    dst_conn = MongoClient(dst_mongo_host)
    print("Destination : Connected successfully!!!")
except:
    print("Destination : Could not connect to MongoDB")


src_coll = src_conn[src_db][src_collection]
src_query = {"logout_time": {"$gte": start_logout_time, "$lt": end_logout_time}}
print(src_query)
dst_coll = dst_conn[dst_db][dst_collection]

bulk = []
for doc in src_coll.find(src_query):
    connection_id = int(doc["_id"])
    user_id = int(doc["user_id"])
    imsi = doc["username"]
    dst_query = {"_id": connection_id, "user_id": user_id}

    dst_result =  dst_coll.find(dst_query).count()
    if dst_result > 0:
        continue

    print("====================Adding to bulk=====================")
    print(connection_id, user_id, imsi)
    print(" ======================================================")
    bulk.append(doc)
    if len(bulk) == 1000:
        try:
            dst_coll.insert_many(bulk)
            print("===================== inserting bulk =========================")
            bulk = []

            time.sleep(0.1)
        except Exception as Error:
            print("##################Error in insert action#################", str(Error))
if bulk:
    print("================ Inserting final bulk ================")
    dst_coll.insert_many(bulk)
    print("=================== End ===================")

