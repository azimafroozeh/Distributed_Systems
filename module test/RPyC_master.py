import rpyc
from rpyc.utils.classic import teleport_function
def add(a,b):
    return a+b
conn = rpyc.connect('localhost',12233)

#result = conn.root.get_time()
#print(result)
print(conn.root.word_count('text_text',1))
print(conn.root.udf(add,"{'a':1,'b':2}"))

print()
conn.close()