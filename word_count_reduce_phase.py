def word_count_reduce():
    from collections import Counter
    #for worker in workers:
        #path = worker.path + "/partition_"+  str(partition)
    data = read_all_csv("/Users/azimafroozeh/PycharmProjects/DistributedSystem/worker1/partition0")
    result = {}
    for key, value in data:
        if key in result:
            result[key] += 1
        else:
            result[key] = 1
    print(result)


        # produce final result



def read_all_csv(path):
    import csv
    import os
    files = os.listdir(path)
    files = [path+'/'+f for f in files]
    result = []
    for file in files:
        reader = csv.reader(open(file), delimiter='\t')
        result.extend([row for row in reader])
    return  result
# reader=csv.reader(open('intermediate_data\\text\partition0\key_values_split_0.txt'),delimiter='\t')
# for row in reader:
#     print(row)
#print(read_all_csv("/Users/azimafroozeh/PycharmProjects/DistributedSystem/worker1/partition0"))
word_count_reduce()