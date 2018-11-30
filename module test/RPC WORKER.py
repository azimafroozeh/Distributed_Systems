

from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn
import pickle
import marshal,types
import time



class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


def add(x, y):
    return x + y


def subtract(x, y):
    return x - y


def multiply(x, y):
    return x * y


def divide(x, y):
    return x / y
def heartbeat():
    print('heartbeat start')
    time.sleep(10)
    print('heatbeat end')
    return 'ok'
def universal_function(code_string,parameter):
    print(type(eval(code_string)))
    code=marshal.loads(eval(code_string))
    print('-----------')
    f=types.FunctionType(code,globals(),'UDF')
    return f(**parameter)

# def universal_function(path,parameter):
#     f=pickle.load(open(path,'rb'))
#     return f.add(**parameter)

server = ThreadXMLRPCServer(("localhost", 8000),allow_none=True)
print("Listening on port 8000...")
server.register_multicall_functions()
server.register_function(add, 'add')
server.register_function(subtract, 'subtract')
server.register_function(multiply, 'multiply')
server.register_function(divide, 'divide')
server.register_function(universal_function,'UDF')
server.register_function(heartbeat,'heartbeat')
server.serve_forever()