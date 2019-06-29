import pickle
import sys
import time

f = open("localcodes.pickle", 'rb')
localcodes = pickle.load(f)
# print(localcodes)

f = open("remotecodes.pickle", 'rb')
remotecodes = pickle.load(f)
# print(remotecodes)

f = open("localinfo.pickle", 'rb')
localinfo = pickle.load(f)
# print(localinfo)

f = open("remoteinfo.pickle", 'rb')
remoteinfo = pickle.load(f)
# print(remoteinfo)


nocodes = set(remotecodes) - set(localcodes)
print(nocodes)
# nocodes = set(localcodes) - set(remotecodes)
# print(nocodes)
print("vvvvvv")

errors = list()

for code in localinfo:
    if code in nocodes:
        continue
    codelocal = localinfo.get(code)
    coderemote = remoteinfo.get(code)
    if not codelocal:
        codelocal = []
    if not coderemote:
        coderemote = []
    if sorted(coderemote) == sorted(codelocal):
        pass
    else:
        errors.append(code)

# for code in remoteinfo:
#     if code in nocodes:
#         continue
#     if sorted(remoteinfo.get(code)) == sorted(localinfo.get(code)):
#         pass
#     else:
#         errors.append(code)


print(sorted(list(set(errors))))
print(len(errors))   # 133
print("hahahhaha ")

#
# for code in errors:
#     r1 = sorted(remoteinfo.get(code))
#     r2 = sorted(localinfo.get(code))
#     print(code)
#     print(set(r1) - set(r2))   # 测试有 而正式没有的
#     print(set(r2) - set(r1))   #
#     print()
#     print()
#     time.sleep(1)




