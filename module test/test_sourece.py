import rpyc
def ADD(a,b):
    return a+b
def MIN(a,b):
    return min(a,b)
def conbain(a,b):
    return ADD(a,b)+1
from dill.source import getsource
conn=rpyc.classic.connect("localhost", port=8000)
func_list=[conbain,ADD,MIN,conbain]
for f in func_list:
    conn.execute(getsource(f))
cn=conn.namespace['conbain']
print(cn(1,2))