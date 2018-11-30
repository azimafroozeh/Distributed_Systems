import rpyc
from rpyc.utils.classic import teleport_function

conn = rpyc.connect('localhost',12233)

result = conn.root.get_time()
print(result)
print(conn.root.word_count('text_text'))

conn.close()