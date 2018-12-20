import rpyc
def ADD(a,b):
    return a+b
def MIN(a,b):
    return min(a,b)
def conbain(a,b):
    return ADD(a,b)+1
def return_list():
    return [[12,3],[],[1,4]]
from dill.source import getsource
def read_csv():
    import csv
    reader = csv.reader(open("../key_values_split_8.txt"), delimiter='\t')
    return [row for row in reader]
conn=rpyc.classic.connect("localhost", port=18812)
func_list=[conbain,ADD,MIN,conbain,return_list,read_csv]
for f in func_list:
    conn.execute(getsource(f))
cn=conn.namespace['read_csv']
# print(type(cn()))
# # print(cn())
result=cn()
result=list(result)
for i in result:
    print(i)
conn=rpyc.classic.connect()
conn.module.os.makedir()