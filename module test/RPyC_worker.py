import time
from rpyc import Service
from rpyc.utils.server import ThreadedServer


class TimeService(Service):

    def exposed_get_time(self):
        print('time strat')
        time.sleep(10)
        print('time end')
        return time.ctime()
    def exposed_heartbeat(self):
        print('heartbeat')
        return 'OK'
    def exposed_word_count(self,path):
        from collections import Counter
        with open(path, 'r') as f:
            text = f.read()
        for char in '-.,\n':
            Text = text.replace(char, ' ')
        Text = Text.lower()
        # split returns a list of words delimited by sequences of whitespace (including tabs, newlines, etc, like re's \s)
        word_list = Text.split()
        # word_list=map(lambda x:x+'1',word_list)
        return Counter(word_list).most_common()


s = ThreadedServer(service=TimeService, port=12233, auto_register=False)
s.start()