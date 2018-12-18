# A Map Reduce Framework

## Technologies and Libraries used

- The MapReduce framework was implemented using [Python 3.7](https://www.python.org/downloads/release/python-371/).
- The communication between the nodes is done with the use of Remote Procedure Call (RPC). We used an existent library to implement this, the [RPyC](https://rpyc.readthedocs.io/en/latest/install.html) library.

## Master/Worker
- Start the Worker nodes with the use of the RPyC Classic Server.
- The Master node manage the Workers node.
- New Workers are added manually by the user.

## Utilization

- The virtual environment with python 3.7 and required libraries is available at [venv](https://github.com/azimafroozeh/Distributed_Systems/tree/master/venv) .
- In this first prototype, both the Workers and Master node are running locally in a single machine.
- Start a Worker node with the command `python worker.py -p PORT_NUMBER`. Currently, the port number is hardcoded to start at 22221 and increment. 
- Start the Master node with the command `python master.py`
- In the Master node the user can:
  - Add new Workers: Type `a`. For this run correctly, the Worker node should have been started beforehand.
  - Add new jobs: Press `s`. It's possible to submit multiple jobs.

## General details

- When adding a new Worker, several functions that should be executed in the Worker node will be transformed to workers and the master node has access to every function in namespace of worker. 
- it's assumed that Each Worker node has five(this number can be changed easily and also it can be improved by some dynamic number based on the resource usage on worker
  ) resources available for map task to use. Thus, each Worker node can only run five maps/reduces tasks at any given time.
- A list of all Worker nodes is kept in the Master node.
- When adding a new job, it is submitted and split into 10 map tasks.
- To schedule the jobs, the Priority Queue data structure is used. 
- In order to manage resources , a second Priority Queue is used to manage the available resources. Currently all resources have a same priority.For load balancing different
priority number can be used, for example each resources of a worker can have priority as follows, `1 2 3 4 5`.
- The map phase will be executed on available resources. Therefore there is no limit on the number of workers and the implemented system can handle
many workers. For example if `n` workers added to framework, the system would be `n-1` tolerant.
- The reduce phase is executed in two different Worker nodes, Mainly because there is not exit a fault tolerance file system. After the 
Map phase competed, it assumed that the map workers can not be failed therefore reduce tasks can ask these workers to send them their
intermediate data.
- The result of the map phase is partitioned based on the hash value of key`hash(key) % n`. The `n` is the number of reduce tasks.
- After map phase is completed, the reduce nodes treat the map nodes as HDFS. To start the reduce phase, the reduce nodes requests to the map nodes to send the files containing the temporary results. The reduce node will use this data to produce the final result.

## Fault Tolerance

- Currently, if the system has `n` nodes, it's `n-1` fault tolerance. One of your question is that, can we assume that there are different nodes for map and reduce phase and failure just happen during map phase? in other words when map phase finished the 
worker used for map phase can not be failed and this is because the intermediate data are stored on local storage of these nodes.
Therefore if we use same worker for reduce phase the data will be lost. And also if we use separate workers for each node but still
after map phase completed, there is a possibility that map nodes can failed, the data will be lost.
- The failure detection is implemented by the sending continuous heartbeats.
- Every five seconds the Master node communicate with each Worker node by requesting to make a connection.
- If the Worker node answer with an acknowledges message, the worker is alive and running.
- Otherwise, the Master node assumes that the Worker node has failed. In that case:
  - All assigned tasks to this worker will be inserted in the schedule queue to be assigned to other Worker nodes.
  - The resources associated with failed worker will be deleted from resource queue. There is one possible improvement that master node only insert the unfinished tasks
  to queue of task and it can simply be implemented by keep tracking of only unfinished tasks. The task is finished when it sends
  `ACK` message to master node and this sending must be done after all needed functions.
 

## Improvements

- Provide the required functions to the Worker node before running it.
- Implement a load balancing mechanisms by assigning different priorities to the resources of a Worker node.

## Task List

- [x] The master node is currently using the one to one heartbeats to detect failures. This task should be executed asynchronously, to increase performance. In the case the Lab Supervisor approve the heartbeat implementation, it is possible to use implement this function by using asynchronous rpc and event triggering.
One improvement is that we use sequence heartbeat for heartbeat messages.
# Testing

## First test
 - instruction
   - type `a` for adding one map worker
   - type `t1` for executing wordcount example 
 
- commit 26
- duration time = 8.531974424000001 seconds
- one map worker 
- two reduce worker
- the input directory
- the output directory
- diff = 0
- accuracy = 100
