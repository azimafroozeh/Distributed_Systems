# Map Reduce
## Report 1:
We mainly used RPyC for the MapReduce Implementation. To implement Worker,
we used RPyC Classic Server to receive tasks from the master node. In the 
master node, we add workers by typing a.(the worker on that IP and port 
number must be in running state).
We used localhost and different port numbers to run these nodes locally 
in our laptop. After the worker added, the various functions that need 
to be called on worker will be passed to the worker, and we have access 
to workers namespace. One improvement is that We can add this functions 
to worker node before running the worker node. We assumed that every
 worker has five resources, so it means it can run five map task at 
 the same time. We keep the list of workers and on different thread 
 every 5 seconds we try to make a connection, if we receive the TCP 
 ack message, so the worker is alive, if not we assume the worker is
  dead and we delete all assigned map task to job scheduler again and 
  removes 5 resources from our resource manager. We thought that every
   job consists of 10 map tasks. By pressing s the job will be submitted
    and will be parted to 10 tasks. We used Priority Queue to implement 
    task scheduler. For assigning different task to a resource, we used 
    Another Priority queue for resources management. Another improvement
     can be that we assign different priorities to the resources of a 
     worker to have a load balancing mechanism. For implementing 
     the Reduce phase, we assumed that we have two different
      reduce task node. The result of map reduce will be partitioned
       to two subfolders based on hash(key) % 2. Because we don't have
        a replicated file system, we assumed that the node failure could
         only happen during map phase or task phase. It means when the map
          tasks finished the workers will keep the data and this node will
           not fail until the end of the whole job. They play the rule of
            the file system. So we assume that we have two more workers 
for the reduce part of the system and previous workers are something 
like HDFS. The reduce node will ask these nodes to send the stored data.
 The partition0 subfolder is for task1, and the partition1 is for task2.
  The reduce node will use this data to produce the final result.
## Task List:
- [x] The master node use the connection to send heartbeats one bye one 
to every node. We need to 
do it asynchronously. If this implementation for heart beating is approved we
can use asynchronous version of RPyC that trigger the event on what ever happened.
- [x] For the heartbeat we can also use one sequence number 
to get sure that the ack message is not retransmitted or something like that.

