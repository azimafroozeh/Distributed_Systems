def word_count_map(split_number, worker_id):
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
        hash_object = hashlib.md5(bytes(key, 'utf-8'))
        if (int(hash_object.hexdigest(), 16) % 2) == 0:
            writer = csv.writer(f0, delimiter='\t')
            writer.writerow([key] + [1])
        else:
            writer = csv.writer(f1, delimiter='\t')
            writer.writerow([key] + [1])



j = 0
while j < 9:
    for i in range(1, 4):
        word_count_map(j, i)
        j += 1

