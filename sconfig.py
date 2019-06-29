import os

env = os.environ.get

MYSQL_HOST = env("MYSQL_HOST", "139.159.176.118")

MYSQL_PORT = env("MYSQL_PORT", 3306)

MYSQL_USER = env("MYSQL_USER", "dcr")

MYSQL_PASSWORD = env("MYSQL_PASSWORD", "acBWtXqmj2cNrHzrWTAciuxLJEreb*4EgK4")

MYSQL_DB = env("MYSQL_DB", "datacenter")

MONGO_URL = env("MONGO_URL", "mongodb://127.0.0.1:27017")

MONGO_URL_JQData = env("MONGO_URL_JQData", "mongodb://127.0.0.1:27017/JQdata?connect=false")

MONGO_URL_STOCK = env("MONGO_URL_STOCK", "mongodb://127.0.0.1:27017/stock?connect=false")

# mongo 数据存在两个数据库中 一个是"JQdata" 一个是 "stock"
MONGO_DB1 = env("MONGO_DB", "JQdata")

MONGO_DB2 = env("MONGO_DB", "stock")

MONGO_COLL_CALENDARS = env("MONGO_COLL", "calendars")

MONGO_COLL_INDEX = env("MONGO_COLL_INDEX", "generate_indexcomponentsweight")

