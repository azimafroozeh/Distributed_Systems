import rpyc

class MasterNode():
    def __init__(self,conf_path=".conf"):
        with open(conf_path,'r') as f:
            try:
                conf=eval(f.read())
                self.conf = conf
            except:
                print('conf fail')
    def excute(self,INPUT,OUTPUT,MAP_FUNCTION,REDUCE_FUNCTION):
        self.connect()
        self.init_map_task_queue(INPUT)
        while(1):
            self.update_node_status()
            if self.check_map_queue():
                break
            self.flatmap(MAP_FUNCTION)
        self.init_reduce_task_queue(OUTPUT)
        while(1):
            self.update_node_status()
            if self.check_reduce_queue():
                break
            self.flatreduce(REDUCE_FUNCTION)
    def update_node_status(self):
        for node in self.node_list:
            conn=self.connctions[node]
            try:
                conn.root.heartbeat()
            except:
                print(node+' failed')
                self.failure_process(node)
                self.node_list.remove(node)
    def connect(self):
        self.node_list=self.conf['Worker_List']
        self.connctions={}
        self.working_table={}
        for Node in self.node_list:
            try:
                conn=rpyc.classic.connect(self.conf[Node]['ip'],self.conf[Node]['port'])
                self.connctions[Node]=conn
                self.working_table[Node]=0
            except:
                print(Node+' missing')
                self.node_list.remove(Node)
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
    import time
    print('time start')
    time.sleep(10)
    return a+b
conn = rpyc.connect('localhost',18812)

#result = conn.root.get_time()
#print(result)
print(conn.root.udf(word_count,'''{"path":'text_text',"jobid":1}'''))
print(conn.root.udf(add,"{'a':1,'b':2}"))

print()
conn.close()


