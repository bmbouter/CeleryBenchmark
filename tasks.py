import timeit
import time
import os
import signal
import subprocess
from multiprocessing import Pool

from celery import Celery
from pymongo import MongoClient

CURRENT_TESTING = 'rabbitmq'

if CURRENT_TESTING == 'rabbitmq':
    app = Celery('tasks', broker='amqp://guest@localhost/')
elif CURRENT_TESTING == 'mongo':
    app = Celery('tasks', broker='mongodb://localhost:27017/test')


CELERY_IGNORE_RESULT = True

@app.task
def a_task():
    return None


def rabbitmq_purge_queue():
    subprocess.call(["python", "-m", "celery", "purge", "-f"])

def mongodb_purge_queue():
    client = MongoClient('localhost', 27017)
    db = client.test
    db.messages.remove()

def purge_queue():
    if CURRENT_TESTING == 'rabbitmq':
        return rabbitmq_purge_queue()
    elif CURRENT_TESTING == 'mongo':
        return mongodb_purge_queue()




def mongo_queue_depth():
    client = MongoClient('localhost', 27017)
    db = client.test
    return db.messages.count()

def rabbitmq_queue_depth():
    # This is a hack because PyRabbit was giving me a connection reset error
    proc = subprocess.Popen(["sudo", "rabbitmqctl", "list_queues"], stdout=subprocess.PIPE)
    stdout_value = proc.communicate()[0]
    queue_depth = int(stdout_value.split('celery\t')[1].split('\n')[0])
    return queue_depth

def queue_depth():
    if CURRENT_TESTING == 'rabbitmq':
        return rabbitmq_queue_depth()
    elif CURRENT_TESTING == 'mongo':
        return mongo_queue_depth()



def start_workers(num_workers):
    devnull = open(os.devnull, 'wb')
    return subprocess.Popen(["celery", "-A", "tasks", "worker", "-c", str(num_workers)], stdout=devnull, stderr=devnull).pid

def stop_workers(pid):
    os.kill(pid, signal.SIGTERM)



def dispatch_tasks(num):
    for i in range(num):
        a_task.delay()

def dispatch_test(tasks_to_run):
    runtime = timeit.timeit("dispatch_tasks(%s)" % tasks_to_run, number=1, setup="from __main__ import dispatch_tasks")
    tasks_per_sec = tasks_to_run / runtime
    return (tasks_to_run, runtime, tasks_per_sec)

def load_concurrent(num_processes):
    purge_queue()
    total_num_of_tasks = 100000
    tasks_per_process = total_num_of_tasks / num_processes
    pool = Pool(processes=num_processes)
    result = pool.map(dispatch_test, [tasks_per_process] * num_processes)
    task_rates = [r[2]for r in result]
    total_rate = sum(task_rates)
    print "dispatch tasks/sec = %.2f" % total_rate



def drain_test(num_workers):
    pid = start_workers(num_workers)
    while queue_depth() > 0:
        time.sleep(0.5)
    stop_workers(pid)

def drain_concurrent(num_workers):
    total_tasks = queue_depth()
    runtime = timeit.timeit("drain_test(%s)" % num_processes, number=1, setup="from __main__ import drain_test")
    drain_rate = total_tasks / runtime
    print "drain tasks/sec = %.2f" % drain_rate



if __name__ == "__main__":
    for i in range(10):
        num_processes = i+1
        print '\n\nNum Processes = %s' % num_processes
        load_concurrent(num_processes)
        drain_concurrent(num_processes)
