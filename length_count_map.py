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
    f0 = open(path1 + "worker_" + str(worker_id) + "intermediate_result/partition_0" + "/key_values_split" + str(
        split_number) + ".txt", 'w')
    f1 = open(path1 + "worker_" + str(worker_id) + "intermediate_result/partition_1" + "/key_values_split" + str(
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
