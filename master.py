import rpyc
import time
import _thread
import os

class Job:
    status = None
# class for Worker with conn and status
class Worker:

    conn = None
    port_number = None
    id = None
    path = None

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
            time.sleep(5)
            continue;
        else:
            while not tasks.is_empty() and not resources.is_empty():
                    deleted_task = tasks.delete()
                    print("jobId: ", deleted_task.job_id, "| Type(0=M,1=R): ", deleted_task.type, "| splitNumber: ",
                          deleted_task.info, "| thread: ", threadName, "| priority: ", deleted_task.priority)
                    deleted_resource = resources.delete()
                    print("jobId: ", deleted_task.job_id, "| Type(0=M,1=R): ", deleted_task.type, "| splitNumber: ",
                          deleted_task.info, "| thread: ", threadName, "| priority: ", deleted_task.priority, file=deleted_resource.worker.conn.modules.sys.stdout)
                    deleted_task.worker = deleted_resource.worker
                    deleted_task.worker.tasks.append(deleted_task)
                    deleted_task.resource = deleted_resource
                    try:
                        deleted_task.conn = rpyc.classic.connect("localhost", port=deleted_resource.worker.port_number)
                    except:
                        print("Ddddddddddddddddddddddddddddd")
                    else:
                        deleted_task.conn.execute(wc_txt)
                        deleted_task.remote_func = deleted_task.conn.namespace['word_count_map']
                        deleted_task.func = rpyc.async_(deleted_task.remote_func)
                        deleted_task.result = deleted_task.func(deleted_task.info, deleted_resource.worker.id)
                        deleted_task.result.add_callback(r_func(deleted_resource))
                        #_thread.start_new_thread(result, ("SchedulerThread", deleted_task))


def result(thread_name, task):
    while not task.result:
        continue
    print(task.result.value)
    resources.insert(task.resource)


def heartbeat(thread_name):
    while True:
        for worker in workers:
            try:
                ping_conn = rpyc.classic.connect("localhost", port=worker.port_number)
            except:
                workers.remove(worker)
                for resource in resources.queue:
                    if resource.worker == worker:
                        resources.queue.remove(resource)

                print(worker, "worker", worker.id, "died")
                for task in worker.tasks:
                    tasks.insert(task)

            else:
                print(worker, "worker", worker.id, "is alive")

            #ping_conn.execute(heartbeat_txt)
            #remote_func = ping_conn.namespace['heartbeat']
            #func = rpyc.async_(remote_func)
            #result = func()
            #result.add_callback(hb_func(worker))
        time.sleep(4)


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
NUMBER_OF_RESOURCE_PER_WORKER_NODE = 5
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
number_of_workers = 0
Map_finished = 0

_thread.start_new_thread(scheduler, ("SchedulerThread",))
_thread.start_new_thread(heartbeat, ("HeartBeatThread",))

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
    print("running")
    import csv
    from collections import Counter
    import hashlib
    path = "/Users/azimafroozeh/PycharmProjects/DistributedSystem/"
    with open(path + "input/" + str(split_number) + ".txt", 'r') as f:
        text = f.read()
    text = text.lower()
    print(text)
    # split returns a list of words delimited by sequences of whitespace (including tabs, newlines, etc, like re's \s)
    key_values = {}
    # for key in text.split():
       # key_values[key] = 1;
    # word_list=map(lambda x:x+'1',word_list)

    #key_values = Counter(word_list).most_common()
    f0 = open(path + "worker_" + str(worker_id) + "_intermediate_result/partition_0" + "/key_values_split_" + str(split_number) + ".txt", 'w')
    f1 = open(path + "worker_" + str(worker_id) + "_intermediate_result/partition_1" + "/key_values_split_" + str(split_number) + ".txt", 'w')
    
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
        reader = csv.reader(open(file), delimiter='\t')
        result.extend([row for row in reader])
    print(result)
    return result  

def hello1():
    import os
    print("dgdddddddgd") 
"""
heartbeat_txt = """
def heartbeat():
    return "I'm alive"
"""

reduce_task_txt = """
def word_count_reduce(workers, partition):
    print("reduce task")
    import rpyc
    import csv
    data = []
    for worker in workers:
        path = worker.path + "/partition_"+  str(partition)
        try:
            conn = rpyc.classic.connect("localhost", port=worker.port_number)
        except:
            print("Ddddddddddddddddddddddddddddd")
        else:
            remote_func = worker.conn.namespace['read_all_csv']
            data1 = remote_func(path)
            print(data1)
            data.extend(data1)
    result = {}
    for key, value in data:
        if key in result:
            result[key] += 1
        else:
            result[key] = 1
    print(result)
    f = open("/Users/azimafroozeh/PycharmProjects/DistributedSystem/output" + "/partition_ " + str(partition),'w')
    f.write(str(result))
    f.close()
    
"""

reduce_task_txt1 = """

"""


while True:
    print("Enter a for adding worker, Enter e for exit program, Enter s for submit new job")
    command = input()
    if command == "a":
        try:
            conn = rpyc.classic.connect("localhost", port=port_number)
        except:
            print("Worker is not running on port=", port_number)
        else:
            worker = Worker()
            worker.conn = conn
            worker.conn.execute(wc_txt)
            worker.id = number_of_workers
            workers.append(worker)
            number_of_workers += 1
            for i in range(NUMBER_OF_RESOURCE_PER_WORKER_NODE):
                resources.insert(Resource(worker, i))
            ros = conn.modules.os
            # in real node
            # pwd = ros.getcwd()
            pwd = "/Users/azimafroozeh/PycharmProjects/DistributedSystem"
            parent_dic = pwd + "/worker_" + str(worker.id) + "_intermediate_result"
            print(parent_dic)
            if not ros.path.exists(parent_dic):
                ros.makedirs(parent_dic)
            ros.chdir(parent_dic)
            for i in range(2):
                partition_dic = parent_dic + "/partition_" + str(i)
                if not ros.path.exists(partition_dic):
                    ros.makedirs(partition_dic)
            worker.path = parent_dic
            print("Added Worker on Port Number", port_number - 1)
            print("Number of workers: ", number_of_workers)
            print("")

    elif command == "e":
        break
    elif command == 's':
        for i in range(NUMBER_OF_TASKS):
            tasks.insert(Task(i, 3))
        job_id += 1

    elif command == 't':
        while not resources.is_empty():
            resource = resources.delete()
            print(resource.worker, resource.priority)
        while not tasks.is_empty():
            print(tasks.delete().job_id)

    elif command == 'r':
        try:
            conn1 = rpyc.classic.connect("localhost", port=22225)
            conn2 = rpyc.classic.connect("localhost", port=22226)
        except:
            print("Ddddddddddddddddddddddddddddd")
        else:
            conn1.execute(reduce_task_txt)
            remote_func1 = conn1.namespace['word_count_reduce']
            remote_func1(workers, 0)
            conn2.execute(reduce_task_txt)
            remote_func2= conn2.namespace['word_count_reduce']
            remote_func2(workers, 1)
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

        try:
            conn1 = rpyc.classic.connect("localhost", port=22225)
            conn2 = rpyc.classic.connect("localhost", port=22226)
        except:
            print("error happened")
        else:
            conn1.execute(reduce_task_txt)
            remote_func1 = conn1.namespace['word_count_reduce']
            remote_func1(workers, 0)
            conn2.execute(reduce_task_txt)
            remote_func2= conn2.namespace['word_count_reduce']
            remote_func2(workers, 1)
        t1 = time.perf_counter()
        print("t1: 1Map node + 2 reduce node finished at" + str(t1))
        print("duration time = " + str(t1 - t0))
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
# asleep2 = rpyc.async_(remote_wc2)
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


