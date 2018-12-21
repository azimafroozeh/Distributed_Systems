class MapWorker:

    def __init__(self, id, ip, port_number, conn):
        self.id = id
        self.ip = ip
        self.port_number
        self.tasks = []
        self.status = 0
        self.port_number = port_number
        self.conn = conn


class ReduceWorker:

    def __init__(self, id, ip, port_number, conn):
        self.id = id
        self.ip = ip
        self.port_number
        self.tasks = []
        self.status = 0
        self.port_number = port_number
        self.conn = conn


class Task:

    def __init__(self, worker, conn, resource, info, priority):
        self.worker = worker
        self.conn = conn
        self.resource = resource
        self.type = 0
        self.info = info
        self.priority = priority


class Resource:

    def __init__(self, worker, priority):
        self.worker = worker
        self.priority = priority

