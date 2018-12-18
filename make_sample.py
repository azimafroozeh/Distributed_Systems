import shutil
source="module test\input\\text_text1"
dest='sample/100000times/'
for i in range(100000):
    shutil.copyfile(source,dest+str(i))