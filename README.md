
## Go to out->artifacts->freshworkassignment->freshworkassignment.jar

### How to Use? 

###### 1 - Initialized the object of class FileBasedKeyValueDB.
######  1-a) FileBasedKeyValueDB db = FileBasedKeyValueDB() for default path.
######  1-b) FileBasedKeyValueDB db = FileBasedKeyValueDB("your_path") for custom path.
###### 2 - create(String key, JSONObject jsonObject) is used to add data without timetolive functionality.
###### 3 - create(String key, JSONObject jsonObject, int timeToLive) is used is used to add data with timetolive functionality.
###### 4 - readJSONObject(String key) is used to read the data.
###### 5 - delete(String key) is used to delete the data.

## Note: a) Max key length is *32 char* & max JSON object size is *16 kb *
##       b) Max database file size is *1 GB*
