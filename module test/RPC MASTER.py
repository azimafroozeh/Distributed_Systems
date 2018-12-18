from xmlrpc.client import ServerProxy
from xmlrpc.client import MultiCall
import pickle
#import word_count
import marshal
import time
from dill.source import getsource


# proxy = ServerProxy("http://localhost:8000/")
# multicall = MultiCall(proxy)
# class A(object):
#     def __init__(self):
#         self.name=0
#     def Add(self,a,b):
#         return a+b
# b=A()
# b=A(b)
# f=open('functions/add','wb')
# pickle.dump(b,f,1)
# f.close()
def ADD(a,b):
    return a+b
code_string=marshal.dumps(ADD.__code__)
# with open('functions/add','wb') as f:
#     f.write(code_string)
print(getsource(ADD))
#multicall.add(1,2)
#status=proxy.heartbeat()

# result=multicall()
# for i in result:
#     print(i)
#print(status)
# print(tuple(result))
# add_result=proxy.UDF(str(code_string),{'a':1,'b':2})
# code_string=marshal.dumps(word_count.WC.__code__)
# map_result=proxy.UDF(str(code_string),{'path':'text_text'})
#
# #result=multicall()
# print(map_result)
# time.sleep(10)
# #print(status)