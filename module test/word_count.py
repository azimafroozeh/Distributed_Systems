from collections import Counter
import os
from math import ceil
import time
import hashlib
def WC(path):
    from collections import Counter

    with open(path,'r') as f:
        text=f.read()
    for char in '-.,\n':
        Text = text.replace(char, ' ')
    Text = Text.lower()
    # split returns a list of words delimited by sequences of whitespace (including tabs, newlines, etc, like re's \s)
    word_list = Text.split()
    #word_list=map(lambda x:x+'1',word_list)
    return Counter(word_list).most_common()
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
# test_path='text_text'
# test_output_path='output'
# Text="""
# bands which have connected them with another, and to assume among the powers of the earth, the separate and equal station to which the Laws of Nature and of Nature's God entitle them, a decent respect to the opinions of mankind requires that they should declare the causes which impel them to the separation.  We hold these truths to be
# self-evident, that all men are created equal, that they are endowed by their Creator with certain unalienable Rights, that among these are Life, Liberty and the pursuit of Happiness.--That to secure these rights, Governments are instituted among Men, deriving their just powers from the consent of the governed, --That whenever any Form of Government becomes destructive of these ends, it is the Right of the People to alter or to abolish it, and to institute new Government, laying its foundation on such principles and organizing its powers in such form, as to them shall seem most likely to effect their Safety and Happiness. """
# print(reduce(WC(test_path)))
def chunks(l,n):
    for i in range(0,len(l),n):
        yield l[i:i+n]
def key2interfile(key,inter_split):
    hash_object=hashlib.md5(bytes(key,'utf-8'))
    return int(hash_object.hexdigest(),16)%inter_split
def map_task(input_list,function,inter_path,inter_split=5):
    inter_list=[[] for i in range(inter_split) ]
    result=[]
    for file in input_list:
        result.extend(function(file))
    for pair in result:
        assigned_id=key2interfile(pair[0],inter_split)
        #print(assigned_id)
        inter_list[assigned_id].append(pair)
    #print(result)
    for i in range(inter_split):
        #print(inter_path)
        with open(str(inter_path)+'/'+str(i),'a+') as f:
            print(inter_list[i])
            f.write('|'.join(([str(j) for j in inter_list[i]]))+'|')
def reduce_task(inter_list,function,output,output_id):
    result=[]
    for file in inter_list:
        with open(file,'r') as f:
            pair_string=f.read()
            pair_list=pair_string.split('|')
            pair_list.remove('')
            pair_list=[eval(i) for i in pair_list]
            result.extend(function(pair_list))
    with open(output+'/'+str(output_id),'w') as f:
        f.write(str(result))
def flatmap(input,function,task_split=10,inter_split=5):
    file_list=os.listdir(input)
    file_list=[input+'/'+f for f in file_list]
    #print(file_list)
    inter_path = 'intermediate_data/' + str(int(time.time()))
    if not os.path.exists(inter_path):
        os.mkdir(inter_path)
    for input_list in chunks(file_list,ceil(len(file_list)/task_split)):
        map_task(input_list,function,inter_path,inter_split)# here we can assign it to
        '''
            here you can do:
            rpyc.root.udf(map_task,parameters)
        '''
    return inter_path

def flatReduce(output,function,inter_path,task_split=2):
    file_list = os.listdir(inter_path)
    file_list = [inter_path + '/' + f for f in file_list]
    if not os.path.exists(output):
        os.mkdir(output)
    output_id=0
    for inter_file_list in chunks(file_list, ceil(len(file_list) / task_split)):
        reduce_task(inter_file_list, function,output,output_id)  # here we can assign it to worker
        output_id+=1
test_input_path='input'
test_output_path='output'
inter_path=flatmap(test_input_path,WC)
flatReduce(test_output_path,reduce,inter_path)
