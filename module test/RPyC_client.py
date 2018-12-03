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

    def __init__(self):
        global port_number
        self.tasks = []
        self.status = 0
        self.port_number =None
        port_number += 1


# class for Task with data , info and priority
class Task:
    worker = None
    remote_func = None
    func = None
    result = None
    resource = None
    conn = None
    parameters=None

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
                    if x == (self.size() - 1):
                        # add new node at the end
                        self.queue.insert(x + 1, task)
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
            print(str(x.info) + " - " + str(x.priority))

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
                    if x == (self.size() - 1):
                        # add new node at the end
                        self.queue.insert(x + 1, resource)
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
            print(str(x.info) + " - " + str(x.priority))

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
            continue
        else:
            while not tasks.is_empty() and not resources.is_empty():
                deleted_task = tasks.delete()
                print("jobId: ", deleted_task.job_id, "| Type(0=M,1=R): ", deleted_task.type, "| splitNumber: ",
                      deleted_task.info, "| thread: ", threadName, "| priority: ", deleted_task.priority)
                deleted_resource = resources.delete()
                print("jobId: ", deleted_task.job_id, "| Type(0=M,1=R): ", deleted_task.type, "| splitNumber: ",
                      deleted_task.info, "| thread: ", threadName, "| priority: ", deleted_task.priority,
                      file=deleted_resource.worker.conn.modules.sys.stdout)
                deleted_task.worker = deleted_resource.worker
                deleted_task.worker.tasks.append(deleted_task)
                deleted_task.resource = deleted_resource
                try:
                    deleted_task.conn = rpyc.classic.connect("localhost", port=deleted_resource.worker.port_number)
                except:
                    print("Ddddddddddddddddddddddddddddd")
                else:
                    #deleted_task.conn.execute(wc_txt)
                    #deleted_task.remote_func = deleted_task.conn.namespace['WC1']
                    if deleted_task.remote_func=='map':

                        deleted_task.func = rpyc.async_(deleted_task.conn.root.map_task)
                    else:
                        deleted_task.func = rpyc.async_(deleted_task.conn.root.reduce_task)
                    deleted_task.result = deleted_task.func(*deleted_task.parameters)
                    deleted_task.result.add_callback(r_func(deleted_resource))

                    #deleted_task.remote_func=
                    # _thread.start_new_thread(result, ("SchedulerThread", deleted_task))


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

            # ping_conn.execute(heartbeat_txt)
            # remote_func = ping_conn.namespace['heartbeat']
            # func = rpyc.async_(remote_func)
            # result = func()
            # result.add_callback(hb_func(worker))
        time.sleep(4)


def r_func(resource):
    print("done")
    resources.insert(resource)


def hb_func(worker):
    print("Worker", worker.id, "is alive")

class MasterNode():
    def __init__(self,conf_path=".conf"):
        self.number_work=0
        with open(conf_path,'r') as f:
            try:
                conf=eval(f.read())
                self.conf = conf
            except:
                print('conf fail')
    def init(self):
        self.node_list = self.conf['Worker_List']
        for node_name in self.node_list:
            node=self.conf[node_name]
            print(node)
            try:
                conn = rpyc.classic.connect(node['IP'], port=int(node['port']))
            except:
                print("Worker is not running on port=", str(node['port']))
            else:
                worker = Worker()
                worker.conn = conn
                worker.id = self.number_work
                worker.port_number=node['port']
                workers.append(worker)
                self.number_work += 1
                for i in range(NUMBER_OF_RESOURCE_PER_WORKER_NODE):
                    resources.insert(Resource(worker, i))
                print("Added Worker Port Number", workers[self.number_work - 1].port_number)
                print("Number of workers: ", number_of_workers)
                print("")

    def chunks(self,l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]
    def flatmap(self,input, function, task_split=10, inter_split=5):
        from math import ceil
        file_list = os.listdir(input)
        file_list = [input + '/' + f for f in file_list]
        # print(file_list)
        inter_path = 'intermediate_data/' + str(int(time.time()))
        if not os.path.exists(inter_path):
            os.mkdir(inter_path)
        job_id=0
        for input_list in self.chunks(file_list, ceil(len(file_list) / task_split)):
            t=Task(job_id,3)
            job_id+=1
            t.remote_func='map'
            t.parameters=(input_list, function, inter_path, inter_split)

            tasks.insert(t)
            #map_task(input_list, function, inter_path, inter_split)  # here we can assign it to
        return inter_path

    def flatReduce(self,output, function, inter_path, task_split=2):
        from math import ceil
        file_list = os.listdir(inter_path)
        file_list = [inter_path + '/' + f for f in file_list]
        if not os.path.exists(output):
            os.mkdir(output)
        output_id = 0
        for inter_file_list in self.chunks(file_list, ceil(len(file_list) / task_split)):
            t=Task(job_id,3)
            job_id+=1
            t.remote_func = 'reduce'
            t.parameters=(inter_file_list, function, output, output_id)
            #conn.root.reduce_task(inter_file_list, function, output, output_id)  # here we can assign it to worker
            output_id += 1
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

tasks = TaskPriorityQueue()
resources = ResourcePriorityQueue()
workers = []
number_of_workers = 0
test_input_path='input'
test_output_path='output'
_thread.start_new_thread(scheduler, ("SchedulerThread",))
_thread.start_new_thread(heartbeat, ("HeartBeatThread",))
def WC(text):
    for char in '-.,\n':
        Text = text.replace(char, ' ')
    Text = Text.lower()
    word_list = Text.split()
    print(word_list)
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
conn_map = {}


mn=MasterNode()
while(1):
    if len(workers):
        break
    mn.init()

inter_path=mn.flatmap(test_input_path,WC)
while(1):
    if tasks.is_empty() and resources.size()==len(workers):
        break
mn.flatReduce(test_output_path,reduce,inter_path)

# while True:
#     print("Enter a for adding worker, Enter e for exit program, Enter s for submit new job")
#     command = input()
#     if command == "a":
#         try:
#             conn = rpyc.classic.connect("localhost", port=port_number)
#         except:
#             print("Worker is not running on port=", port_number)
#         else:
#             worker = Worker()
#             worker.conn = conn
#             worker.id = number_of_workers
#             workers.append(worker)
#             number_of_workers += 1
#             for i in range(NUMBER_OF_RESOURCE_PER_WORKER_NODE):
#                 resources.insert(Resource(worker, i))
#             print("Added Worker Port Number", workers[number_of_workers - 1].port_number)
#             print("Number of workers: ", number_of_workers)
#             print("")
#
#     elif command == "e":
#         break
#     elif command == 's':
#         for i in range(NUMBER_OF_TASKS):
#             tasks.insert(Task(i, 3))
#         job_id += 1
#
#     elif command == 't':
#         while not resources.is_empty():
#             resource = resources.delete()
#             print(resource.worker, resource.priority)
#         while not tasks.is_empty():
#             print(tasks.delete().job_id)
#
#     else:
#         continue
#
# for worker in workers:
#     print("asdafdasdddddddd", file=worker.conn.modules.sys.stdout)
