from pymongo import MongoClient
import pymongo
import time
import jdatetime

src_mongo_host = "127.0.0.1:27019"
src_collection = "connection_log"
src_db = "IBSng"

from datetime import datetime
start_logout_time = datetime(2019, 3, 28, 19, 30, 0)
end_logout_time = datetime(2018, 3, 30, 19, 30, 0)
remote_ip = "89.44.132.81"

try:
    src_conn = MongoClient(src_mongo_host, readPreference='secondaryPreferred')
except Exception as Error:
    print(str(Error))



src_coll = src_conn[src_db][src_collection]
src_query = {"type_details.remote_ip": remote_ip, "login_time": {"$gte": start_logout_time, "$lt": end_logout_time}}
print(src_query)
print("user_id,username,login_time,logout_time,remote_ip,connection_log_id")
for doc in src_coll.find(src_query).hint("login_time_-1_type_details.remote_ip_1"):

    user_id = int(doc["user_id"])
    username = doc["username"]
    login = doc["login_time"]
    ltime = jdatetime.date.fromgregorian(day=login.day,month=login.month,year=login.year)
    login_time = "%d-%d-%d %d:%d:%d" % (ltime.year, ltime.month, ltime.day, login.hour, login.minute, login.second)

    logout = doc["logout_time"]
    time = jdatetime.date.fromgregorian(day=logout.day,month=logout.month,year=logout.year)
    logout_time = "%d-%d-%d %d:%d:%d" % (time.year, time.month, time.day, logout.hour, logout.minute, logout.second)

    connection_id = int(doc["_id"])
    remote_ip = doc["type_details"]["remote_ip"]
    print("%s,%s,%s,%s,%s,%s" % (user_id, username, login_time, logout_time, remote_ip, connection_id))
