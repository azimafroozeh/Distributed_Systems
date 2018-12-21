def word_count_reduce(workers, partition):
    print("reduce task")
    print("test")
    import rpyc
    import csv
    import os
    import sys
    # import requests
    data = []
    result = {}
    print(partition)
    print(workers)
    # response  = requests.get('http://localhost:8000/key_values_split_8.txt')
    # print(response.text)
    for worker in workers:
        print(worker)
        path = worker.path + "/partition_" + str(partition)
        print(path)
        try:
            conn = rpyc.classic.connect(worker.ip, port=22222)
        except:
            print("Ddddddddddddddddddddddddddddd")
        else:
            print("sfasfa")
            sum = 0
            #remote_func = worker.conn.namespace['read_all_csv']
            try:
                data1 = read_all_csv(path,conn)
                print("before")
                print("data1")
                a = str(data1)
                data2 = eval(a)
                print("after")
                # print(type(data1))
                # a = str(data1)
                # print(eval(a))
                # text_file = open("Output.txt", "w")
                # text_file.write(a)
                # text_file.close()
                for d in data2:
                    if (d is None):
                        print("dddddddddddddddddD")
                        continue
                    # print(str(d))
                    # print(d)
                    sum += len(str(d))
                    # print(sum)
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

                # print(result)
    print("vb")
    f = open("/home/ec2-user/partition_" + str(partition), 'w')
    f.write(str(result))
    f.close()
    return ("dddddddooooooneee")


def read_all_csv(path,conn):
    print("sending infromatiosn")
    import csv
    import os
    print(path)
    files =conn.modules.os.listdir(path)
    files = [path + '/' + f for f in files]
    # print(files)
    result = []
    for file in files:
        print(file)
        reader = csv.reader(conn.builtins.open(file), delimiter='\t')
        result.extend([row for row in reader])

    # print(result)
    return result