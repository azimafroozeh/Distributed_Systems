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



from collections import Counter
import os
from math import ceil
import time
import hashlib
def WC(text):
    from collections import Counter
    #
    # with open(path,'r') as f:
    #     text=f.read()
    for char in '-.,\n':
        Text = text.replace(char, ' ')
    Text = Text.lower()
    # split returns a list of words delimited by sequences of whitespace (including tabs, newlines, etc, like re's \s)
    word_list = Text.split()
    #word_list=map(lambda x:x+'1',word_list)
    return [(i,1) for i in word_list]
def reduce(pair_list):
    print(pair_list)
    out_dic={}
    for pair in pair_list:
        if pair[0] in out_dic:
            out_dic[pair[0]]+=pair[1]
        else:
            out_dic[pair[0]]= pair[1]
    print(out_dic)
    return [(i,out_dic[i]) for i in out_dic]
def chunks(l,n):
    for i in range(0,len(l),n):
        yield l[i:i+n]
def flatmap(input,function,task_split=10,inter_split=5):
    file_list=os.listdir(input)
    file_list=[input+'/'+f for f in file_list]
    #print(file_list)
    inter_path = 'intermediate_data/' + str(int(time.time()))
    if not os.path.exists(inter_path):
        os.mkdir(inter_path)
    for input_list in chunks(file_list,ceil(len(file_list)/task_split)):
        conn.root.map_task(input_list,function,inter_path,inter_split)# here we can assign it to
        '''
            here you can do:
            rpyc.root.udf(map_task,parameters)
        '''
    return inter_path

def flatReduce(output,function,inter_path,task_split=2):
    file_list = os.listdir(inter_path)
    file_list = [inter_path + '/' + f for f in file_list]
    if not os.path.exists(output):
        os.mkdir(output)
    output_id=0
    for inter_file_list in chunks(file_list, ceil(len(file_list) / task_split)):
        conn.root.reduce_task(inter_file_list, function,output,output_id)  # here we can assign it to worker
        output_id+=1
conn = rpyc.connect('localhost',18812)

#result = conn.root.get_time()
#print(result)
#print(conn.root.udf(word_count,[{"path":'text_text',"jobid":1}]))
#print(conn.root.udf(test,str({'f':add})))
test_input_path='input'
test_output_path='output'
inter_path=flatmap(test_input_path,WC)
flatReduce(test_output_path,reduce,inter_path)
conn.close()


