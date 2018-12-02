import csv
import os
def read_all_csv(path):
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
print(read_all_csv("intermediate_data\\text\partition0"))