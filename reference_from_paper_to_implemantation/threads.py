def scheduler(threadName):
    while True:
        if tasks.is_empty():
            continue;
        else:
            while not tasks.is_empty() and not resources.is_empty():
                    deleted_task = tasks.delete()
                    deleted_resource = resources.delete()
                    deleted_task.worker = deleted_resource.worker
                    deleted_task.resource = deleted_resource
                    try:
                        deleted_task.remote_func = deleted_task.conn.namespace['word_count_map']
                        a_func = rpyc.async_(deleted_task.remote_func)
                        result = a_func(deleted_task.info, deleted_resource.worker.id)
                        _thread.start_new_thread(result, ("ResultThread", deleted_task.worker, result, deleted_resource, deleted_task))
                    except:
                        print("The map worker does not work correctly")


_thread.start_new_thread(scheduler, ("SchedulerThread",))


def result(thread_name, worker, result, resource, task):
    while not result.ready:
        continue
    print(result.value)
    map_finished_lock(1)
    worker.tasks.append(task)
    resources.insert(resource)


def heartbeat(thread_name):
    while True:
        for worker in workers:
            try:
                func = worker.conn.namespace['heartbeat']
                func()

            except:

                workers.remove(worker)
                for resource in resources.queue:
                    if resource.worker == worker:
                        resources.queue.remove(resource)

                for task in worker.tasks:
                    tasks.insert(task)
                    map_finished_lock(-1)


                if flag == 1:
                    goto(783)
            else:
                print(worker, "worker", worker.id, "is alive")

        time.sleep(5)

_thread.start_new_thread(heartbeat, ("HeartBeatThread",))




