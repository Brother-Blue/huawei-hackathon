from inspect import getframeinfo, stack
from json import load as jsonLoad
from csv import writer, QUOTE_MINIMAL
import sys
from time import time
from collections import deque

BUFFER_SIZE: 3505
dagList: list = []

class Task:
    def __init__(self, id=0, exetime=0, type=0, deps=[]):
        self.id: int = id
        self.exeTime: int = exetime
        self.type: int = type
        # dependencies but we can't spell so yeah.
        self.deps: list = deps
    # def add_dep(self, taskID, transferTime):
    #     self.deps.append((taskID, transferTime,))
    def get_dep(self):
        return self.deps.pop()

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)


class DAG:
    def __init__(self, id=0, type=0, arrival=0, deadline=0):
        self.id: int = id
        self.type: int = type
        self.arrival: int = arrival
        self.deadline: int = deadline
        self.tasks: list = []

    def add_task(self, task):
        self.tasks.append(task)

    def add_tasks(self, task_list):
        for task in task_list:
            self.add_task(task)

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)


class Processor:
    pass

def calcTime(spent: float):
    return int((spent) * (10**3))

def debug(message):
    caller = getframeinfo(stack()[1][0])
    print("[Line]:%d -> %s" % (caller.lineno, message))

def read_file(filename="sample.json") -> dict:
    with open(filename, "r") as f:
        file = jsonLoad(f)
        f.close()
    return file


def reader_func(filename: str):
    global dagList
    dags = read_file(filename)
    #dagList = [DAG() for i in dags.keys()]
    for i, (dagId, vals) in enumerate(dags.items()):
        task_list = []
        taskKeys = list(filter(lambda key: "Task" in str(key), vals))
        dagList.append(DAG(
            int(dagId[3:20]),
            vals['Type'],
            vals['ArrivalTime'],
            vals['Deadline'],
            ))
        for task in taskKeys:
            t = Task(
                int(task[4:20]),
                vals[task]['EET'],
                vals[task]['Type'],
                list(
                    map(lambda kv: (int(kv[0][4:20]), kv[1]),
                        vals[task]["next"].items())))
            task_list.append(t)
        dagList[i].add_tasks(task_list)


def printer_func(filename="default.csv"):
    with open(filename, mode='w') as f:
        myWriter = writer(f,
                            delimiter=',',
                            quotechar='"',
                            quoting=QUOTE_MINIMAL)
        # for each processor loop
        # processor 1 every column in row has a Tuple(TaskID, Starttime, Finishtime)
        # output[i].taskIDs[j], output[i].startTime[
        #    j], output[i].startTime[j] + output[i].exeTime[j]
        myWriter.writerow([
            'Tuple(TaskID, Starttime, Finishtime)',
            'Tuple(TaskID, Starttime, Finishtime)',
            'Tuple(TaskID, Starttime, Finishtime)'
        ])
        # makespan
        writer.writerow([108])
        # stdev (standard deviation of processor loads)
        writer.writerow([0.082932])
        # ulitiy function
        writer.writerow([0.141])
        # execution time of the scheduler (in miliseconds)
        writer.writerow([3])


def scheduler():
    pass

def run():
    args = sys.argv
    reader_func(args[1])
    for dag in dagList:
        debug(id(dag))
        for task in dag.tasks:
            debug(task)
    b = time()
    scheduler()
    e = time()
    spent=(e-b)
    print("spent: %d" % calcTime(spent))
    #printer_func(args[2])

if __name__ == '__main__':
    run()