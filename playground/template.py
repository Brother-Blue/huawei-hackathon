from math import sqrt
from time import time
from json import load as jsonLoad
import csv
import sys
from queue import Queue
from dataclasses import dataclass, field

#### UTIL TO REMOVE ####
from inspect import getframeinfo, stack


def debug(message):
    caller = getframeinfo(stack()[1][0])
    print("[Line]:%d -> %s" % (caller.lineno, message))


#########END###########


class Node:
    def __init__(self, data=None, next=None):
        self.data = data
        self.next = next

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)


class LinkedListIterator:
    def __init__(self, head):
        self.current = head

    def __iter__(self):
        return self

    def __next__(self):
        if not self.current:
            raise StopIteration
        else:
            item = self.current.data
            self.current = self.current.next
            return item


class LinkedList:
    def __init__(self, nodes=None):
        self.head = None
        if nodes is not None and len(nodes) > 0:
            node = Node(data=nodes.pop(0))
            self.head = node
            for elem in nodes:
                node.next = Node(data=elem)
                node = node.next

    # def __iter__(self):
    #     return LinkedListIterator(self.head)

    def add(self, item):
        new_node = Node(data=item)
        #debug(new_node)
        if not self.head:
            self.head = new_node
            return

        tmp = self.head
        while tmp.next:
            tmp = tmp.next
        tmp = new_node

    def __iter__(self):
        node = self.head
        while node is not None:
            yield node
            node = node.next

    def isEmpty(self) -> bool:
        return self.head == None

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

    def __getitem__(self, i):
        idx = 0
        ptr = self.head
        if i == 0:
            return ptr.data
        while ptr:
            idx += 1
            ptr = ptr.next
            debug(ptr)
            if idx == i:
                return ptr.data


@dataclass
class Task(Node):
    def __init__(self):
        super().__init__(self)
        self.id: int = 0
        self.exeTime: int = 0
        self.type: int = 0

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

    #next: LinkedList = LinkedList()


@dataclass
class Dependency(Node):
    def __init__(self):
        super().__init__(self)
        # "afterID" can be executed only after finishing "beforeID"
        self.beforeID: int = 0
        self.afterID: int = 0
        self.transferTime: int = 0

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

    #next: dict = field(default_factory=dict)

class DAG():
    # id: int = 0
    # type: int = 0
    # arrivalTime: int = 0
    # deadlineTime: int = 0
    # taskList: LinkedList = LinkedList()
    # depList: LinkedList = LinkedList()
    # prevTask: Task = Task()
    # prevDep: Dependency = Dependency()
    # firstTask: Task = Task()
    # firstDep: Dependency = Dependency()
    def __init__(self):
        self.id: int = 0
        self.type: int = 0
        self.arrivalTime: int = 0
        self.deadlineTime: int = 0
        self.taskList: LinkedList = LinkedList()
        self.depList: LinkedList = LinkedList()
        self.prevTask: Task = Task()
        self.prevDep: Dependency = Dependency()
        self.firstTask: Task = Task()
        self.firstDep: Dependency = Dependency()

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)


@dataclass
class ProcessorSchedule:
    cache: Queue = field(default_factory=Queue)
    prevTypes: set = field(default_factory=set)
    numberOfTasks: int = 0  # place number of tasks scheduled for this processor here
    taskIDs: list = field(default_factory=list)
    startTime: list = field(
        default_factory=list
    )  # place starting times of scheduled tasks, so startTime[i] is start time of task with ID taskIDs[i] (all should be integers) (0 indexed)
    exeTime: list = field(
        default_factory=list
    )  # place actual execution times of tasks, so exeTime[i] is execution time of i-th task scheduled on this processor (0 indexed)


@dataclass
class TaskWithDeadline:
    taskId: int = 0
    dagDeadline: int = 0


@dataclass
class TaskWithFinishTime:
    taskId: int = 0
    finishTime: int = 0


N = 3505
numberOfProcessors = 8
tasksPerGraph = 103
historyOfProcessor = 4
dagList: list = []
dagCount: int = 0

begin: float
end: float

output = [ProcessorSchedule() for proc in range(numberOfProcessors)]


def calcTime(spent: float):
    return int((spent) * (10**3))


def read_file(filename) -> dict:
    with open(filename, "r") as f:
        file = jsonLoad(f)
        f.close()
    return file


def convert_key_name(d: dict):
    return {int(k[4:20]): v for k, v in d.items()}


def populate_tasks(d: dict) -> LinkedList:
    LL = LinkedList()
    taskKeys = list(filter(lambda key: "Task" in str(key), d))
    for task in taskKeys:
        newTask = Task()
        newTask.exeTime = d[task]["EET"]
        newTask.type = d[task]["Type"]
        newTask.next = dict(
            map(lambda kv: (int(kv[0][4:20]), kv[1]), d[task]["next"].items()))
        #newTask.next = {int(k[4:20]): v for k, v in d[task]["next"].items()}
        if task[4:20].isnumeric():
            newTask.id = int(task[4:20])
        LL.add(newTask)
    return LL


def dag_task(index: int, taskID: int, executionTime: int,
                     taskType: int, taskNext: dict):
    global taskList
    newTask = Task()
    newTask.id = taskID
    newTask.exeTime = executionTime
    newTask.taskType = taskType
    newTask.next = taskNext

    if dagList[index].taskList.isEmpty():
        dagList[index].prevTask = newTask
        dagList[index].firstTask = dagList[index].prevTask
    else:
        dagList[index].prevTask.next = newTask
        dagList[index].prevTask = dagList[index].prevTask.next
    return newTask


def dag_dependency(index: int, fromTaskID: int, toTaskID: int,
                            transferTime: int):
    global dagList
    newDep = Dependency()
    newDep.beforeID = fromTaskID
    newDep.afterID = toTaskID
    newDep.transferTime = transferTime

    if dagList[index].depList.isEmpty():
        dagList[index].prevDep = newDep
        dagList[index].firstDep = dagList[index].prevDep
    else:
        dagList[index].prevDep.next = newDep
        dagList[index].prevDep = dagList[index].prevDep.next
    return newDep


def reader_func(filename):
    global dagList
    dags = read_file(filename)
    dagList = [DAG() for key in dags.keys()]
    for i, (dagId, tasks) in enumerate(dags.items()):
        dagList[i].id = int(dagId[3:20])
        taskKeys = list(filter(lambda key: "Task" in str(key), tasks))
        tmpDepList = []
        tmpTaskList = []
        for task in taskKeys:
            # first set of tasks seen in dag
            newTask = Task()
            # newTask.exeTime = tasks[task]["EET"]
            # newTask.type = tasks[task]["Type"]
            # inner tasks aka
            # newTask.next = dict(
            taskNext = dict(
                map(lambda kv: (int(kv[0][4:20]), kv[1]),
                    tasks[task]["next"].items()))
            if task[4:20].isnumeric():
                newTask.id = int(task[4:20])

            for innerTask, transferTime in taskNext.items():
                tmpTaskList.append(
                    dag_task(i, newTask.id, innerTask, transferTime, taskNext))
                tmpDepList.append(dag_dependency(i, newTask.id, innerTask,
                                                   transferTime))
        dagList[i].taskList = LinkedList(tmpTaskList)
        dagList[i].depList = LinkedList(tmpDepList)


def printer_func(filename="default.csv"):
    with open(filename, mode='w') as f:
        writer = csv.writer(f,
                            delimiter=',',
                            quotechar='"',
                            quoting=csv.QUOTE_MINIMAL)
        # for each processor loop
        # processor 1 every column in row has a Tuple(TaskID, Starttime, Finishtime)
        # output[i].taskIDs[j], output[i].startTime[
        #    j], output[i].startTime[j] + output[i].exeTime[j]
        writer.writerow([
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


def schedule_task():
    pass


def schedule_dag(dag):
    """
    we have access to the entire dag 
    - 6-8 procs
    - cached reduce time by 10% eg 20 -> 18
    - maintain a dict of procs with a cache?
    - 

    Need:
    ProcessorChoice, earliestStart[proc][taskID], taskID, executionTime[taskID]

    
    dag -> deadline
    nextDag start at firsts deadline
    """    
    currentDependency = dag.firstDep
    debug(currentDependency.beforeID)
    inDegrees = []
    while currentDependency:
        debug(currentDependency)
        currentDependency = currentDependency.next
    # prevType = -1
    # while something:
    #     cached = False
    #     if dagList[index].type == prevType:
    #         cached = True
    
    
    
    
    
    # currentTask = currentDag.firstTask
    # dagArrivalTime = currentDag.arrivalTime


def scheduler():
    for dag in dagList:
        schedule_dag(dag)


def main():
    args = sys.argv
    reader_func(args[1])
    # for dag in dagList:
    #     debug(id(dag))
    #     for task in dag.taskList:
    #         debug(task.data)
    # for dag in dagList:
    #     debug(id(dag))
    #     for dep in dag.depList:
    #         debug(dep.data)
    begin = time()
    scheduler()
    end = time()
    spent = (end - begin)
    print("spent: %d" % calcTime(spent))
    printer_func(args[2])
    # print time for us

main()
