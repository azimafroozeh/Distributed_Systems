ORIGINALRESULT=""



#general nodes
python worker.py -p 22221
python worker.py -p 22222
python worker.py -p 22223
python worker.py -p 22224

#reducer
python worker.py -p 22225
python worker.py -p 22226

python master.py > log.txt
SERVER_PID=$!


diff <(cat result.txt) <(echo $ORIGINALRESULT)