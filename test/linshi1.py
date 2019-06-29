import pickle
from collections import defaultdict
import pymongo


cli = pymongo.MongoClient("127.0.0.1:27017")
cld = cli.stock.calendars


codes = sorted(cld.find().distinct("code"))
f = open("localcodes.pickle", 'wb')
pickle.dump(codes, f)
f.close()

ret = list(cld.find({"code": {"$in": codes}}, {"code": 1, "date_int": 1, "_id": 0}))
data = defaultdict(list)
for r in ret:
    data[r.get("code")].append(r.get("date_int"))
f = open("localinfo.pickle", 'wb')
pickle.dump(data, f)
f.close()
