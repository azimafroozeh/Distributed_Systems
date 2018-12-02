def word_count_reduce(workers, partition):
    for worker in workers:
        path = worker.path + "/partition_"+  str(partition)
        data = read_all_csv(path)
        # produce final result


