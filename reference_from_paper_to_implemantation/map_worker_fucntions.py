def word_count_map(split_number, worker_id):
    import csv
    import hashlib
    path = "/efs/"
    with open(path + "input/" + str(split_number) + ".txt", 'r') as f:
        text = f.read()
    text = text.lower()
    path1 = "/home/ec2-user/"
    #needs to be changed to support configurable number of partitions
    f0 = open(path1 + "worker_" + str(worker_id) + "_intermediate_result/partition_0" + "/key_values_split_" + str(
        split_number) + ".txt", 'w')
    f1 = open(path1 + "worker_" + str(worker_id) + "_intermediate_result/partition_1" + "/key_values_split_" + str(
        split_number) + ".txt", 'w')

    for key in text.split():
        hash_object = hashlib.md5(bytes(key, 'utf-8'))
        if (int(hash_object.hexdigest(), 16) % 2) == 0:
            writer = csv.writer(f0, delimiter='\t')
            writer.writerow([key] + [1])
        else:
            writer = csv.writer(f1, delimiter='\t')
            writer.writerow([key] + [1])

    return "finished"


def read_all_csv(path):
    import csv
    import os
    print(path)
    files = os.listdir(path)
    files = [path + '/' + f for f in files]
    result = []
    for file in files:
        print(file)
        reader = csv.reader(open(file), delimiter='\t')
        result.extend([row for row in reader])
    return result


def length_count_map(split_number, worker_id):
    import csv
    import hashlib
    path = "/efs/"
    with open(path + "input/" + str(split_number) + ".txt", 'r') as f:
        text = f.read()
    text = text.lower()
    path1 = "/home/ec2-user/"
    # needs to be changed to support configurable number of partitions
    f0 = open(path1 + "worker_" + str(worker_id) + "_intermediate_result/partition_0" + "/key_values_split_" + str(
        split_number) + ".txt", 'w')
    f1 = open(path1 + "worker_" + str(worker_id) + "_intermediate_result/partition_1" + "/key_values_split_" + str(
        split_number) + ".txt", 'w')

    for key in text.split():
        hash_object = hashlib.md5(bytes(key, 'utf-8'))
        if (int(hash_object.hexdigest(), 16) % 2) == 0:
            writer = csv.writer(f0, delimiter='\t')
            writer.writerow(len([key]) + [1])
        else:
            writer = csv.writer(f1, delimiter='\t')
            writer.writerow(len([key]) + [1])


def heartbeat():
    return "I'm alive"


