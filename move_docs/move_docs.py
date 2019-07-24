from pymongo import MongoClient
import configparser
import pymongo
import time


cfg = configparser.ConfigParser()
cfg.read('config_file.cfg')

src_mongo_host = cfg.get('DB', 'src_host')
src_collection = cfg.get('Collection', 'src_col')
src_db = cfg.get('DB', 'src_db')

dst_mongo_host = cfg.get('DB', 'dst_host')
dst_collection = cfg.get('Collection', 'dst_col')
dst_db = cfg.get('DB', 'dst_db')

from datetime import datetime
st_logout = cfg.get('Logout', 'start').split(',')
end_logout = cfg.get('Logout', 'end').split(',')
start_logout_time = datetime(int(st_logout[0]), int(st_logout[1]), int(st_logout[2]), int(st_logout[3]), int(st_logout[4]), int(st_logout[5]))
end_logout_time = datetime(int(end_logout[0]), int(end_logout[1]), int(end_logout[2]), int(end_logout[3]), int(end_logout[4]), int(end_logout[5]))

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
    username = doc["username"]
    dst_query = {"_id": connection_id, "user_id": user_id}

    dst_result =  dst_coll.find(dst_query).count()
    if dst_result > 0:
        continue

    print("====================Adding to bulk=====================")
    print(connection_id, user_id, username)
    bulk.append(doc)
    if len(bulk) == 1000:
        try:
            print("===================== inserting bulk =========================")
            dst_coll.insert_many(bulk)
            bulk = []

            time.sleep(0.3)
        except Exception as Error:
            print("##################Error in insert action#################", str(Error))
if bulk:
    print("================ Inserting final bulk ================")
    dst_coll.insert_many(bulk)
    print("=================== Fnish ===================")

