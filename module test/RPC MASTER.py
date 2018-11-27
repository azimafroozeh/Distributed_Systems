from xmlrpc.client import ServerProxy
from xmlrpc.client import MultiCall
import pickle
import marshal


proxy = ServerProxy("http://localhost:8000/")
multicall = MultiCall(proxy)
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
print(code_string)
#multicall.add(1,2)
multicall.UDF(str(code_string),{'a':1,'b':2})
result=multicall()
# for i in result:
#     print(i)
print(tuple(result))