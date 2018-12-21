import rpyc
import time
import _thread
from threading import Thread, Lock

mutex = Lock()
mutex2 = Lock()
import os

class Job:
    status = None
# class for Worker with conn and status
class Worker:

    conn = None
    port_number = None
    id = None
    path = None
    ip = None

    def __init__(self):
        global port_number
        self.tasks = []
        self.status = 0
        self.port_number = port_number
        port_number += 1

class ReduceWorker:

    conn = None
    port_number = None
    id = None
    path = None
    ip = None
    partition = None

    def __init__(self):
        global port_number
        self.tasks = []
        self.status = 0
        self.port_number = port_number
        port_number += 1


# class for Task with data , info and priority
class Task:
    worker = None
    remote_func = None
    func = None
    result = None
    resource = None
    conn = None

    def __init__(self, info, priority):
        global job_id
        self.type = 0
        self.job_id = job_id
        self.info = info
        self.priority = priority


# class for Task Priority queue
class TaskPriorityQueue:

    def __init__(self):
        self.queue = list()
    # if you want you can set a maximum size for the queue

    def insert(self, task):
        # if queue is empty
        if self.size() == 0:
            # add the new node
            self.queue.append(task)
        else:
            # traverse the queue to find the right place for new node
            for x in range(0, self.size()):
                # if the priority of new node is greater
                if task.priority >= self.queue[x].priority:
                    # if we have traversed the complete queue
                    if x == (self.size()-1):
                        # add new node at the end
                        self.queue.insert(x+1, task)
                    else:
                        continue
                else:
                    self.queue.insert(x, task)
                    return True

    def delete(self):
        # remove the first node from the queue
        return self.queue.pop(0)

    def show(self):
        for x in self.queue:
            print(str(x.info)+" - "+str(x.priority))

    def size(self):
        return len(self.queue)

    def is_empty(self):
        if self.size() == 0:
            return True
        return False


class Resource:


    def __init__(self, worker, priority):
        global job_id
        self.worker = worker
        self.priority = priority


class ResourcePriorityQueue:

    def __init__(self):
        self.queue = list()
    # if you want you can set a maximum size for the queue

    def insert(self, resource):
        # if queue is empty
        if self.size() == 0:
            # add the new node
            self.queue.append(resource)
        else:
            # traverse the queue to find the right place for new node
            for x in range(0, self.size()):
                # if the priority of new node is greater
                if resource.priority >= self.queue[x].priority:
                    # if we have traversed the complete queue
                    if x == (self.size()-1):
                        # add new node at the end
                        self.queue.insert(x+1, resource)
                    else:
                        continue
                else:
                    self.queue.insert(x, resource)
                    return True

    def delete(self):
        # remove the first node from the queue
        return self.queue.pop(0)

    def show(self):
        for x in self.queue:
            print(str(x.info)+" - "+str(x.priority))

    def size(self):
        return len(self.queue)

    def is_empty(self):
        if self.size() == 0:
            return True
        return False


def scheduler(threadName):
    global tasks
    while True:
        if tasks.is_empty():
            continue;
        else:
            while not tasks.is_empty() and not resources.is_empty():
                    deleted_task = tasks.delete()
                    print("jobId: ", deleted_task.job_id, "| Type(0=M,1=R): ", deleted_task.type, "| splitNumber: ",
                          deleted_task.info, "| thread: ", threadName, "| priority: ", deleted_task.priority)
                    deleted_resource = resources.delete()
                    print(deleted_resource.worker.id)
                    #print("jobId: ", deleted_task.job_id, "| Type(0=M,1=R): ", deleted_task.type, "| splitNumber: ",
                          #deleted_task.info, "| thread: ", threadName, "| priority: ", deleted_task.priority, file=deleted_resource.worker.conn.modules.sys.stdout)
                    deleted_task.worker = deleted_resource.worker
                    #deleted_task.worker.tasks.append(deleted_task)
                    deleted_task.resource = deleted_resource
                    try:
                        deleted_task.conn = rpyc.classic.connect(deleted_resource.worker.ip, port=22222)
                    except:
                        print("Ddddddddddddddddddddddddddddd")
                    else:
                        deleted_task.conn.execute(wc_txt)
                        deleted_task.remote_func = deleted_task.conn.namespace['word_count_map']
                        #deleted_task.remote_func = deleted_task.conn.namespace['length_count_map']

                        deleted_task.func = rpyc.async_(deleted_task.remote_func)
                        deleted_task.result = deleted_task.func(deleted_task.info, deleted_resource.worker.id)
                        #deleted_task.result.add_callback(r_func(deleted_resource))
                        _thread.start_new_thread(result, ("SchedulerThread", deleted_task.worker, deleted_task.result, deleted_resource, deleted_task))

#def new_result():

def result(thread_name,worker, result, resource, task):
    while not result.ready:
        continue
    global Map_finished
    print(result.value)
    mutex.acquire()
    Map_finished += 1
    worker.tasks.append(task)

    print("number of finished map task changed" + str(Map_finished))
    mutex.release()

    resources.insert(resource)


def heartbeat(thread_name):
    global Map_finished
    while True:
        for worker in workers:
            try:
                ping_conn = rpyc.classic.connect(worker.ip, port=22222)
            except:
                workers.remove(worker)
                for resource in resources.queue:
                    if resource.worker == worker:
                        resources.queue.remove(resource)

                print(worker, "worker", worker.id, "died")
                for task in worker.tasks:
                    tasks.insert(task)
                    mutex.acquire()
                    Map_finished += -1
                    print("number of finished map task changed")
                    mutex.release()
            else:
                print(worker, "worker", worker.id, "is alive")

            #ping_conn.execute(heartbeat_txt)
            #remote_func = ping_conn.namespace['heartbeat']
            #func = rpyc.async_(remote_func)
            #result = func()
            #result.add_callback(hb_func(worker))
        #time.sleep(1)

def reduce_heartbeat(thread_name):
    global  reduce_finished
    while True:
        for worker in reduceworkers:
            try:
                ping_conn = rpyc.classic.connect(worker.ip, port=22222)
            except:
                mutex2.acquire()
                reduce_finished += -1
                print("number of finished reduce task changed, Number : " + str(reduce_finished))
                mutex2.release()
                reduceworkers[2].partition = worker.partition
                _thread.start_new_thread(reduce_thread, ("HeartBeatThread1", reduceworkers[2]))
                reduceworkers.remove(worker)
            else:
                print(worker, "worker", worker.id, "is alive")

            #ping_conn.execute(heartbeat_txt)
            #remote_func = ping_conn.namespace['heartbeat']
            #func = rpyc.async_(remote_func)
            #result = func()b
            #result.add_callback(hb_func(worker))
        #time.sleep(1)

def reduce_thread(thread_name, worker):
    global reduce_finished
    try:
        try:
            connn1 = rpyc.classic.connect(worker.ip, port=22222)
        except:
            print("Ddddddddddddddddddddddddddddd")
        else:
            connn1.execute(reduce_task_txt)
            remote_func1 = connn1.namespace['word_count_reduce']
            result1 = remote_func1(workers, worker.partition)
            print(result1)
    except:
        print(str(worker.id) +" reduce failed")
        #_thread.start_new_thread(reduce_thread, ("HeartBeatThread1", reduceworkers[2]))
    else:
        #print(worker, "worker", worker.id, "is alive")
        mutex2.acquire()
        reduce_finished += 1
        print("number of finished reduce task changed, Number : " + str(reduce_finished))
        mutex2.release()



def r_func(resource):
    global Map_finished
    print("done")
    Map_finished += 1
    resources.insert(resource)

def hb_func(worker):
    print("Worker", worker.id, "is alive")


LIVE = 0
FAILED = 1
MAPPING = 2
REDUCING = 3
NUMBER_OF_RESOURCE_PER_WORKER_NODE = 1
TASK_NUMBERS = 10
NUMBER_OF_TASKS = 10
NUMBER_OF_TASKS_PER_WORKER = 1
MAP_TASK = 0
REDUCE_TASK = 1
port_number = 22221
job_id = 0
NUMBER_OF_PARTITION = 2
tasks = TaskPriorityQueue()
resources = ResourcePriorityQueue()
workers = []
reduceworkers =[]
number_of_workers = 0
number_of_reduce_workers = 0
Map_finished = 0
reduce_finished = 0

_thread.start_new_thread(scheduler, ("SchedulerThread",))
_thread.start_new_thread(heartbeat, ("HeartBeatThread",))
_thread.start_new_thread(reduce_heartbeat, ("HeartBeatThread1",))



map_ips = ["52.3.129.76", "3.83.44.0", "3.83.24.246", "3.83.190.228"]
reduce_ips = ["3.85.205.160", "3.84.116.176", "54.225.25.73"]

conn_map = {}

hello_txt = """
def hello1():
    import os
    print("dgdddddddgd")
"""
wc_txt = """
def WC():
    from collections import Counter
    import time
    #test_path='/Users/azimafroozeh/PycharmProjects/DistributedSystem/text_1text'
    with open(test_path,'r') as f:
        text=f.read().lower()
    # split returns a list of words delimited by sequences of whitespace (including tabs, newlines, etc, like re's \s)
    word_list = text.split()
    time.sleep(10)
    #word_list=map(lambda x:x+'1',word_list)
    print(Counter(word_list).most_common())
    return "done"


def word_count_map(split_number, worker_id):
    from random import randint
    import time
    #time.sleep(randint(0, 9))
    print("running")
    import csv
    from collections import Counter
    import hashlib
    path = "/efs/"
    with open(path + "input/" + str(split_number) + ".txt", 'r') as f:
        text = f.read()
    text = text.lower()
    print(text)
    # split returns a list of words delimited by sequences of whitespace (including tabs, newlines, etc, like re's \s)
    key_values = {}
    # for key in text.split():
       # key_values[key] = 1;
    # word_list=map(lambda x:x+'1',word_list)
    path1 = "/home/ec2-user/"
    #key_values = Counter(word_list).most_common()
    f0 = open(path1 + "worker_" + str(worker_id) + "_intermediate_result/partition_0" + "/key_values_split_" + str(split_number) + ".txt", 'w')
    f1 = open(path1 + "worker_" + str(worker_id) + "_intermediate_result/partition_1" + "/key_values_split_" + str(split_number) + ".txt", 'w')
    
    print(f1)
    print(f0)
    for key in text.split():
        hash_object = hashlib.md5(bytes(key, 'utf-8'))
        if (int(hash_object.hexdigest(), 16) % 2) == 0:
            writer = csv.writer(f0, delimiter='\t')
            writer.writerow([key] + [1])
        else:
            writer = csv.writer(f1, delimiter='\t')
            writer.writerow([key] + [1])
    
    return "done11"
    
def read_all_csv(path):
    print("sending infromatiosn")
    import csv
    import os
    print(path)
    files = os.listdir(path)
    files = [path+'/'+f for f in files]
    #print(files)
    result = []
    for file in files:
        print(file)
        reader = csv.reader(open(file), delimiter='\t')
        result.extend([row for row in reader])
        
    #print(result)
    return result            

def hello1():
    import os
    print("dgdddddddgd") 
def local_test_length_count_map(split_number, worker_id):
    import csv
    from collections import Counter
    import hashlib
    path = "/Users/azimafroozeh/PycharmProjects/DistributedSystem/"
    with open(path + "input/" + str(split_number) + ".txt", 'r') as f:
        text = f.read()
    text = text.lower()
    # split returns a list of words delimited by sequences of whitespace (including tabs, newlines, etc, like re's \s)
    key_values = {}
    # for key in text.split():
       # key_values[key] = 1;
    # word_list=map(lambda x:x+'1',word_list)

    #key_values = Counter(word_list).most_common()
    f0 = open(path + "worker" + str(worker_id) + "/partition0" + "/key_values_split_" + str(split_number) + ".txt", 'w')
    f1 = open(path + "worker" + str(worker_id) + "/partition1" + "/key_values_split_" + str(split_number) + ".txt", 'w')

    for key in text.split():
        #hash_object = hashlib.md5(bytes(key, 'utf-8'))
        if len(key)%2 == 0:
            writer = csv.writer(f0, delimiter='\t')
            writer.writerow([len(key)] + [1])
        else:
            writer = csv.writer(f1, delimiter='\t')
            writer.writerow([len(key)] + [1])
            
def length_count_map(split_number, worker_id):
    from random import randint
    import time
    # time.sleep(randint(0, 9))
    print("running")
    import csv
    from collections import Counter
    import hashlib
    path = "/efs/"
    with open(path + "input/" + str(split_number) + ".txt", 'r') as f:
        text = f.read()
    text = text.lower()

    path1 = "/home/ec2-user/"
    # key_values = Counter(word_list).most_common()
    f0 = open(path1 + "worker_" + str(worker_id) + "_intermediate_result/partition_0" + "/key_values_split_" + str(
        split_number) + ".txt", 'w')
    f1 = open(path1 + "worker_" + str(worker_id) + "_intermediate_result/partition_1" + "/key_values_split_" + str(
        split_number) + ".txt", 'w')

    for key in text.split():
        #hash_object = hashlib.md5(bytes(key, 'utf-8'))
        if len(key)%2 == 0:
            writer = csv.writer(f0, delimiter='\t')
            writer.writerow([len(key)] + [1])
        else:
            writer = csv.writer(f1, delimiter='\t')
            writer.writerow([len(key)] + [1])

    return "done11"
"""
heartbeat_txt = """
def heartbeat():
    return "I'm alive"
"""

reduce_task_txt = """
def word_count_reduce(workers, partition):
    print("reduce task")
    print("test")
    import rpyc
    import csv
    import os
    import sys
    #import requests
    data = []
    result = {}
    print(partition)
    print(workers)
    #response  = requests.get('http://localhost:8000/key_values_split_8.txt')
    #print(response.text)
    for worker in workers:
        print(worker)
        path = worker.path + "/partition_"+  str(partition)
        print(path)
        try:
            conn = rpyc.classic.connect(worker.ip, port=22222)
        except:
            print("Ddddddddddddddddddddddddddddd")
        else:
            print("sfasfa")
            sum = 0
            remote_func = worker.conn.namespace['read_all_csv'] 
            try:
                data1 = remote_func(path)
                print("before")
                print("data1")
                a = str(data1)
                data2 = eval(a)
                print("after")
                #print(type(data1))
                #a = str(data1)
                #print(eval(a))
                #text_file = open("Output.txt", "w")
                #text_file.write(a)
                #text_file.close()
                for d in data2:
                    if(d is None):
                        print("dddddddddddddddddD")
                        continue
                    #print(str(d))
                    #print(d)
                    sum += len(str(d))
                    #print(sum)
                    key = d[0]
                    print(d[0])
                    data = d[1]
                    print(d[1])
                    if key in result:
                        result[key] += 1
                    else:
                        result[key] = 1
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)                
        
    #print(result)
    print("vb")
    f = open("/home/ec2-user/partition_" + str(partition),'w')
    f.write(str(result))
    f.close()
    return("dddddddooooooneee")
    
"""

reduce_task_txt1 = """

"""
reduce_task_y = """
def word_count_reduce(workers, partition):
    import rpyc
    print("dgdgd")
    data = []
    for worker in workers:
        path = worker.path + "/partition_"+  str(partition)
        try:
            conn = rpyc.classic.connect(worker.ip, worker.port_number)
        except:
            print("Ddddddddddddddddddddddddddddd")
        else:
            #remote_func = worker.conn.namespace['read_all_csv']
            data.extend(read_all_csv(path,conn))
    result = {}
    for key, value in data:
        if key in result:
            result[key] += 1
        else:
            result[key] = 1
    print(result)



        # produce final result
def hello3():
    print("hello3")


def read_all_csv(path):
    print("sending infromatiosn")
    import csv
    import os
    print(path)
    files = os.listdir(path)
    files = [path+'/'+f for f in files]
    #print(files)
    result = []
    for file in files:
        print(file)
        reader = csv.reader(open(file), delimiter='\t')
        result.extend([row for row in reader])
        
    #print(result)
    return result 


"""



while True:
    print("Enter a for adding worker, Enter e for exit program, Enter s for submit new job")
    command = input()
    if command == "a":
        try:
            conn = rpyc.classic.connect(map_ips[number_of_workers], port=22222)
        except:
            print("Worker is not running on port=", port_number)
        else:
            worker = Worker()
            worker.ip = map_ips[number_of_workers]
            worker.conn = conn
            worker.conn.execute(wc_txt)
            worker.id = number_of_workers
            workers.append(worker)
            number_of_workers += 1
            for i in range(NUMBER_OF_RESOURCE_PER_WORKER_NODE):
                resources.insert(Resource(worker, 0))
            ros = conn.modules.os
            # in real node
            pwd = ros.getcwd()
            #pwd = "/Users/azimafroozeh/PycharmProjects/DistributedSystem"
            parent_dic = "/home/ec2-user"+ "/worker_" + str(worker.id) + "_intermediate_result"
            print(parent_dic)
            if not ros.path.exists(parent_dic):
                ros.makedirs(parent_dic, 511 )
            ros.chdir(parent_dic)
            for i in range(2):
                partition_dic = parent_dic + "/partition_" + str(i)
                if not ros.path.exists(partition_dic):
                    ros.makedirs(partition_dic)
            worker.path = parent_dic
            print("Added Worker on Port Number", "22222")
            print("Number of workers: ", number_of_workers)
            print("")

    if command == "b":
        try:
            conn = rpyc.classic.connect(reduce_ips[number_of_reduce_workers], port=22222)
        except:
            print("Reduce Worker is not running on port=22222")
        else:
            worker = ReduceWorker()
            worker.ip = reduce_ips[number_of_reduce_workers]
            worker.conn = conn
            worker.partition = number_of_reduce_workers
            worker.conn.execute(reduce_task_txt)
            worker.id = number_of_reduce_workers
            reduceworkers.append(worker)
            number_of_reduce_workers += 1
            ros = conn.modules.os
            # in real node
            pwd = ros.getcwd()
            # pwd = "/Users/azimafroozeh/PycharmProjects/DistributedSystem"
            parent_dic = "/home/ec2-user" + "/output/"
            print(parent_dic)
            if not ros.path.exists(parent_dic):
                ros.makedirs(parent_dic, 511)
            ros.chdir(parent_dic)
            parent_dic = "/home/ec2-user" + "/output/"
            print(parent_dic)
            if not ros.path.exists(parent_dic):
                ros.makedirs(parent_dic, 511)
            ros.chdir(parent_dic)
            worker.path = parent_dic
            print("Added Worker on Port Number", "22222")
            print("Number of workers: ", number_of_workers)
            print("")

    if command == "c":
        try:
            conn = rpyc.classic.connect(reduce_ips[number_of_reduce_workers], port=22222)
        except:
            print("Reduce Worker is not running on port=22222")
        else:
            worker = ReduceWorker()
            worker.ip = map_ips[number_of_workers]
            worker.conn = conn
            worker.conn.execute(reduce_task_txt)
            worker.id = number_of_reduce_workers
            reduceworkers.append(worker)
            number_of_reduce_workers += 1

    elif command == "e":
        break
    elif command == 's':
        t0 = time.perf_counter()
        for i in range(NUMBER_OF_TASKS):
            tasks.insert(Task(i, 3))
        job_id += 1

        while Map_finished != NUMBER_OF_TASKS:
            #print("no")
            continue

        t1 = time.perf_counter()
        print("============================================")
        print("Map tasks finished, time is : " + str(t1 - t0))
        #print(str(t1 - t0))
        print("============================================")

        mutex.acquire()
        Map_finished = 0
        print("number of finished map task changed")
        mutex.release()

    elif command == 't':
        while not resources.is_empty():
            resource = resources.delete()
            print(resource.worker, resource.priority)
        while not tasks.is_empty():
            print(tasks.delete().job_id)

    elif command == 'r':
        try:
            connn1 = rpyc.classic.connect("localhost", port=22225)
            connn2 = rpyc.classic.connect("localhost", port=22226)
        except:
            print("Ddddddddddddddddddddddddddddd")
        else:
            connn2.execute(reduce_task_y)
            remote_func1 = connn2.namespace['word_count_reduce']
            #result = remote_func1(workers, 0)
            #for worker in workers:
                #print
            result1 = remote_func1(workers, 0)
            print(result1)
            connn1.execute(reduce_task_txt)
            remote_func2 = connn1.namespace['word_count_reduce']
            result2 = remote_func2(workers, 1)
            print(result2)
    # 1 map node + 2 reduce node
    # Delete Worker Intermediate Result
    # Press a to add one worker
    # Press t1 to compute
    elif command == "t1":
        Map_finished = 0
        t0 = time.perf_counter()
        print("t1: 1Map node + 2 reduce node starts at " + str(t0))
        for i in range(NUMBER_OF_TASKS):
            tasks.insert(Task(i, 3))
        job_id += 1

        while Map_finished != NUMBER_OF_TASKS:
            continue
        t1 = time.perf_counter()
        print(str(t1 - t0))

        try:
            connn1 = rpyc.classic.connect("3.84.180.136", port=22225)
            connn2 = rpyc.classic.connect("3.83.13.27", port=22226)
        except:
            print("Ddddddddddddddddddddddddddddd")
        else:
            connn2.execute(reduce_task_txt)
            remote_func1 = connn2.namespace['word_count_reduce']
            #result = remote_func1(workers, 0)
            #for worker in workers:
                #print
            result1 = remote_func1(workers, 0)
            print(result1)
            connn1.execute(reduce_task_txt)
            remote_func2 = connn1.namespace['word_count_reduce']
            result2 = remote_func2(workers, 1)
            print(result2)


        Map_finished = 0
    #input
    #map function
    elif command == 'd1':
        Map_finished = 0
        t0 = time.perf_counter()
        for i in range(NUMBER_OF_TASKS):
            tasks.insert(Task(i, 3))
        job_id += 1

        while Map_finished != (NUMBER_OF_TASKS):
            # print("no")
            continue

        t1 = time.perf_counter()
        print("============================================")
        print("Map tasks finished, time is : " + str(t1 - t0))
        # print(str(t1 - t0))
        print("============================================")
        Map_finished = 0

        _thread.start_new_thread(reduce_thread, ("HeartBeatThread1", reduceworkers[0]))
        _thread.start_new_thread(reduce_thread, ("HeartBeatThread1", reduceworkers[1]))



        while reduce_finished != 2:
            # print("no")
            continue

        t2 = time.perf_counter()
        print("============================================")
        print("Reduce tasks finished, time is : " + str(t2 - t1))
        # print(str(t1 - t0))
        print("============================================")

        Map_finished = 0



    else:
        continue

for worker in workers:
    print("asdafdasdddddddd", file = worker.conn.modules.sys.stdout)
# print(rsys.argv)
conn1.execute(wc_txt)
# print(conn.namespace)
remote_wc1 = conn1.namespace['WC']
# asleep = rpyc.async_(remote_wc1)
# print("Node1,testing eval", file=conn1.modules.sys.stdout)
# conn.eval('WC')
# remote_wc1()
asleep1 = rpyc.async_(remote_wc1)
res1 = asleep1()
# print(res1)
# print(res1.ready)


print("Node2", file=conn2.modules.sys.stdout)
conn2.execute(wc_txt)
remote_wc2 = conn2.namespace['WC']
# asleep2 = rpyc.async_(remote_wc2)§
asleep2 = rpyc.async_(remote_wc2)
res2 = asleep2()
# print(res2)
# print(res2.ready)

print(conn_map)

while not res1:
    continue
print(res1.value)
while not res2:
    continue
print(res2.value)


