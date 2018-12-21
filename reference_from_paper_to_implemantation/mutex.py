map_mutex = Lock()
reduce_mutex = Lock()

finished_number_of_maps
finished_number_of_reduces


def map_finished_lock(change):
    global finished_number_of_maps
    map_mutex.acquire()
    finished_number_of_maps += change
    map_mutex.release()


def map_finished_lock(change):
    global finished_number_of_reduces
    reduce_mutex.acquire()
    finished_number_of_reduces += change
    reduce_mutex.release()
