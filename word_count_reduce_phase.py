def word_count_reduce():
    #for worker in workers:
        #path = worker.path + "/partition_"+  str(partition)
        data = read_all_csv("intermediate_data\\text\partition0")
        # produce final result



def read_all_csv(path):
    import csv
    import os
    files=os.listdir(path)
    files=[path+'/'+f for f in files]
    result=[]
    for file in files:
        reader=csv.reader(open(file),delimiter='\t')
        result.extend([row for row in reader])
    output={}
    for row in result:
        if row[0] in output:
            output[row[0]]+=row[1]
        else:
            output[row[0]]= row[1]
    return output
# reader=csv.reader(open('intermediate_data\\text\partition0\key_values_split_0.txt'),delimiter='\t')
# for row in reader:
#     print(row)
#print(read_all_csv("intermediate_data\\text\partition0"))