import rpyc

# class MasterNode():
#     def __init__(self,conf_path=".conf"):
#         with open(conf_path,'r') as f:
#             conf=eval(f.read()
#         self.conf=
def word_count(path, jobid):
    from collections import Counter
    with open(path, 'r') as f:
        text = f.read()
    for char in '-.,\n':
        Text = text.replace(char, ' ')
    Text = Text.lower()
    # split returns a list of words delimited by sequences of whitespace (including tabs, newlines, etc, like re's \s)
    word_list = Text.split()
    # word_list=map(lambda x:x+'1',word_list)
    return Counter(word_list).most_common()
def add(a,b):
    return a+b
conn = rpyc.connect('localhost',18812)

#result = conn.root.get_time()
#print(result)
print(conn.root.udf(word_count,'''{"path":'text_text',"jobid":1}'''))
print(conn.root.udf(add,"{'a':1,'b':2}"))

print()
conn.close()


