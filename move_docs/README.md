# About script
This script moves docs from source db to destination db based on logout time. First of all you should fill the source and destination server information like IP, port, and collection in config file:
```
src_host = "192.168.1.1:27019"
dst_host = "127.0.0.1:27019"
src_db = "old_db"
dst_db = "new_db"

src_collection = "old_collection"
dst_collection = "new_connection"

```
Also you should set logout time start and end:

```
start = 2019, 6, 14, 0, 0, 0
end = 2019, 6, 18, 0, 0, 0
```

