def word_count_reduce(workers, partition):
    result = {}
    for worker in workers:
        print(worker)
        path = worker.path + "/partition_" + str(partition)
        print(path)
        remote_func = worker.conn.namespace['read_all_csv']
        data1 = remote_func(path)
        for d in data1:
            key = d[0]
            if key in result:
                result[key] += 1
            else:
                result[key] = 1
    f = open("/efs/partition_" + str(partition), 'w')
    f.write(str(result))
    f.close()
    return "finished"
