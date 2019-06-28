# About script
This script moves docs from source db to destination db. First of all you should fill the source and destination server information like IP, port, and collection in script:
```
src_mongo_host = "192.168.1.1:27019"
src_collection = "old_collection"
src_db = "old_db"

dst_mongo_host = "127.0.0.1:27019"
dst_collection = "new_connection"
dst_db = "new_db"
```
in this example we want to find the docs which are in a special date, now we should fill date filed in script:
```
start_logout_time = datetime(2019, 6, 14, 0, 0, 0)
end_logout_time = datetime(2019, 6, 18, 0, 0, 0)
```

