import rpyc
import sys
def index_reduce(workers, partition):
    import rpyc
    print("dgdgd")
    for worker in workers:
        path = worker.path + "/partition_"+  str(partition)
        try:
            conn = rpyc.classic.connect("localhost", worker.port_number)
        except:
            print("Ddddddddddddddddddddddddddddd")
        else:
            remote_func = conn.namespace['read_all_csv']
            data.extend(remote.func(path))
    result = {}
    for key, value in data:
        if key in result:
            result[key] += [value]
        else:
            result[key] = [value]
    print(result)



        # produce final result
reduce_task_txt = """
def hello1():
    print("reduce task")
"""
def hello3():
    print("hello3")


def read_all_csv(path):
    import csv
    import os
    files = os.listdir(path)
    files = [path+'/'+f for f in files]
    result = []
    for file in files:
        reader = csv.reader(open(file), delimiter='\t')
        result.extend([row for row in reader])
    return result
# reader=csv.reader(open('intermediate_data\\text\partition0\key_values_split_0.txt'),delimiter='\t')
# for row in reader:
#     print(row)
#print(read_all_csv("/Users/azimafroozeh/PycharmProjects/DistributedSystem/worker1/partition0"))
#word_count_reduce()
try:
    conn1 = rpyc.classic.connect("3.83.24.246", port=22222)
except:
    print(sys.exc_info()[0])
else:
    conn1.execute(reduce_task_txt)
    remote_func = conn1.namespace['hello1']
    remote_func()
