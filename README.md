# A Map Reduce Framework

## Technologies and Libraries used

- The MapReduce framework was implemented using [Python 3.7](https://www.python.org/downloads/release/python-371/).
- The communication between the nodes is done with the use of Remote Procedure Call (RPC). We used an existent library to implement this, the [RPyC](https://rpyc.readthedocs.io/en/latest/install.html) library.

## Master/Worker

- Start the Worker nodes with the use of the RPyC Classic Server.
- The Master node manage the Workers node.
- New Workers are added manually by the user.

## Utilization

- A Python and RPyC are available in the `venv` directory.
- In this first prototype, both the Workers and Master node are running locally in a single machine.
- Start a Worker node with the command `python worker.py -p PORT_NUMBER`. Current, the port number is hardcoded to start at 22221 and increment. 
- Start the Master node with the command `python master.py`
- In the Master node the user can:
  - Add new Workers: Press `A`. For this run correctly, the Worker node should have been started beforehand.
  - Add new jobs: Press `S`. Each job consists of 10 maps tasks.

## General details

- When adding a new Worker, several functions that should be executed in the Worker node will be sent to it with the RPyC library. 
- The Worker namespace is available to the Master node access it.
- Each Worker node has five resources available to use. Thus, each Worker node can only run five maps/reduces tasks at any given time.
- A list of all Worker nodes is kept in the Master node.
- When adding a new job, it is submitted and split into 10 tasks.
- To schedule the jobs to a given Worker node, the Priority Queue algorithm is applied.
- When assigning different tasks to a resource, a second Priority Queue is used to manage the available resources.
- The map phase is executed in three Worker nodes.
- The reduce phase is executed in two different Worker nodes.
- The result of the reduce phase is partitioned to two subdirectories. The directory is selected calculating `hash(key) % 2`. For example, the map task 1 will be stored in the partition0, the map task 2 will be stored in the partition1, and so on.
- After map phase is completed, the reduce nodes treat the map nodes as HDFS. To start the reduce phase, the reduce nodes requests to the map nodes to send the files containing the temporary results. The reduce node will use this data to produce the final result.

## Fault Tolerance

- Current, only the Worker nodes running map tasks are fault tolerant.
- That is because there is no replication system and the intermediate results are stored in the Worker local filesystem. Thus, for the sake of simplicity in this prototype, it is assumed that after the map phase is completed, these Workers nodes will not fail.
- The fault tolerance is provided by the execution of a continuous heartbeat.
- Every five seconds the Master node communicate with each Worker node by sending a TPC message.
- If the Worker node answer with an acknowledges message, the worker is alive and running.
- Otherwise, the Master node assumes that the Worker node has failed. In that case:
  - All the jobs assigned to this particular Worker node are deleted.
  - These jobs are inserted in the schedule queue to be assigned to other Worker nodes.
  - The number of resources used by the failed Worker node is deducted from the total number of resources available. This information is kept by the Resource Manager.

## Improvements

- Provide the required functions to the Worker node before running it.
- Implement a load balancing mechanisms by assigning different priorities to the resources of a Worker node.

## Task List

- [x] The master node is currently using the connection to send sequential heartbeats to each Worker node. This task should be executed synchronously, to increase performance. In the case the Lab Supervisor approve the heartbeat implementation, it is possible to use this implementation for heart beating is approved we asynchronous version of RPyC that trigger the event in the case of any irregularity.

- [x] For the heartbeat we can also use one sequence number to get sure that the retransmitted acknowledge message is not considered as new acknowledge message (this problem is already solved with using TCP).