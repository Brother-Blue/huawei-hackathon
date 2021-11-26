from ctypes import c_longlong
from functools import cmp_to_key
import math
import time
import json
import sys
import numpy

## util ##
def p(args) -> None:
    print(args)
## end ##

N: int = 3505
numberOfProcessors: int = 3
historyOfProcessor: int = 4


class ll(int):
    def __new__(cls, n):
        return int.__new__(cls, c_longlong(n).value)

    def __add__(self, other):
        return ll(super().__add__(other))

    def __radd__(self, other):
        return ll(other.__add__(self))

    def __sub__(self, other):
        return ll(super().__sub__(other))

    def __rsub__(self, other):
        return ll(other.__sub__(self))


class Task:
    taskID: int
    executionTime: int
    taskType: int
    next: list


class Dependency:
    # "afterID" can be executed only after finishing "beforeID"
    beforeID: int
    afterID: int
    transferTime: int
    next: list


class DAG:
    dagID: int
    dagType: int
    arrivalTime: int
    deadlineTime: int
    listOfTasks: list  # list of all tasks (just their IDs and execution times, doesn't matter) of DAG
    listOfDependencies: list  # all edges (dependencies) of DAG (None if there are no dependencies)
    lastTask: Task
    lastDependency: Dependency
    firstTask: Task
    firstDependency: Dependency
    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

dagsInput = numpy.empty(N, dtype=DAG)
dagsCount: int = 0

# create a dag
def initialize(whichDag):
    dagsInput[whichDag] = DAG()
    dagsInput[whichDag].dagID = None
    dagsInput[whichDag].dagType = None
    dagsInput[whichDag].arrivalTime = None
    dagsInput[whichDag].deadlineTime = None
    dagsInput[whichDag].listOfTasks = None
    dagsInput[whichDag].listOfDependencies = None
    dagsInput[whichDag].lastDependency = None
    dagsInput[whichDag].lastTask = None
    dagsInput[whichDag].firstDependency = None
    dagsInput[whichDag].firstTask = None


def add_task_to_list(whichDag, taskID, executionTime, taskType):
    if dagsInput[whichDag].lastTask == None:
        dagsInput[whichDag].listOfTasks = Task()
        dagsInput[whichDag].lastTask = dagsInput[whichDag].listOfTasks
        dagsInput[whichDag].firstTask = dagsInput[whichDag].lastTask
    else:
        dagsInput[whichDag].lastTask.next = Task()
        dagsInput[whichDag].lastTask = dagsInput[whichDag].lastTask.next
    dagsInput[whichDag].lastTask.taskID = taskID
    dagsInput[whichDag].lastTask.executionTime = executionTime
    dagsInput[whichDag].lastTask.taskType = taskType
    dagsInput[whichDag].lastTask.next = None


def add_dependency_to_list(whichDag, beforeID, afterID, transferTime):
    if dagsInput[whichDag].lastDependency == None:
        dagsInput[whichDag].listOfDependencies = Dependency()
        dagsInput[whichDag].lastDependency = dagsInput[whichDag].listOfDependencies
        dagsInput[whichDag].firstDependency = dagsInput[whichDag].lastDependency
    else:
        dagsInput[whichDag].lastDependency.next = Dependency()
        dagsInput[whichDag].lastDependency = dagsInput[whichDag].lastDependency.next
    dagsInput[whichDag].lastDependency.beforeID = beforeID
    dagsInput[whichDag].lastDependency.afterID = afterID
    dagsInput[whichDag].lastDependency.transferTime = transferTime
    dagsInput[whichDag].lastDependency.next = None


def print_dag_tasks(whichDag):
    # "whichDag" is index of DAG in array "dagsInput"
    current: Task = dagsInput[whichDag].firstTask
    while current != None:
        p(f"{current.taskID}")
        current = current.next
    p("\n")


def print_dag_dependencies(whichDag):
    # "whichDag" is index of DAG in array "dagsInput"
    current: Dependency = dagsInput[whichDag].firstDependency
    while current != None:
        p(
            f"FROM: {current.beforeID} TO: {current.afterID} COST: {current.transferTime}\n"
        )
        current = current.next
    p("\n")


# EET -> executionTime
# Type -> taskType
# next -> next


# def reader_function(filename):
#     with open(filename, "r") as f:
#         file = json.load(f)
#         f.close()
#     # init
#     initialize(dagsCount)
#     for dag, tasks in file.items():
#         task: str 
#         for task in tasks:
#             # the value of the task
#             value = tasks[task]
#             # task is the key
#             p(f"{task} -> {value}")
#             isDict = type(tasks[task]) is dict
#             if isDict:
#                 taskID = int(task[4:20])

#                 if "next" in tasks[task]:
#                     exists: bool  = True
#                     edges = tasks[task]["next"]
#                 else:
#                     exists: bool  = False

#             if isDict and "EET" in tasks[task]:
#                 exeTime = tasks[task]["EET"]

#             if isDict and "Type" in tasks[task]:
#                 _type = tasks[task]["Type"]

#             add_task_to_list(dagsCount, taskID, exeTime, _type)
            
#             if exists:
#                 for k, v in edges.items():
#                     if v:
#                         transferTime: int = int(v)
#                         afterID: int = int(k[4:20])
#                         add_dependency_to_list(dagsCount, taskID, afterID, transferTime)

#             dagsInput[dagsCount].dagType = tasks["Type"]
#             dagsInput[dagsCount].arrivalTime = tasks["ArrivalTime"]
#             dagsInput[dagsCount].deadlineTime = tasks["Deadline"] + dagsInput[dagsCount].arrivalTime
#             dagsInput[dagsCount].dagID = int(dag[3:20])
#             dagsCount = dagsCount + 1

def reader_function(filename):
    global dagsCount
    global dagsCount
    global numberOfProcessors
    global historyOfProcessor
    global begin
    global end
    global output
    global dagsInput
    """
    Reads in DAG data from a given json file (see sample.json)
    Initializes all DAGs with the given data.
    """
    # Read sample json file
    f = open(filename, 'r')
    data = json.load(f)

    # For each top-level data point in data
    for d in data:
        # tasks for each dag
        tasks = [] 
        dag = DAG()
        # Needed to avoid out of bounds
        #dagsInput.append(None) 
        initialize(dagsCount)
        # Init dag attrs
        dag.dagType = data[d]['Type']
        dag.arrivalTime = data[d]['ArrivalTime']
        dag.deadlineTime = data[d]['Deadline']
        # Filter out dag keys where they contain 'Task'
        taskKeys = list(filter(lambda key: 'Task' in str(key), data[d].keys()))

        # For each Task key
        for index, dagTask in enumerate(taskKeys):
            task = Task()
            # Gets the task obj
            dagTask = data[d][dagTask]
            # Set task attrs
            task.taskID = taskKeys[index]
            task.executionTime = dagTask['EET']
            task.taskType = dagTask['Type']
            task.next = list(dagTask['next'])
            # Append to tasks list
            tasks.append(task)
        # Set dag listOfTasks to tasks
        dag.listOfTasks = tasks
        dagsCount =+ 1

    f.close()


class ProcessorSchedule:
    numberOfTasks : int                             # place number of tasks scheduled for this processor here
    taskIDs = numpy.empty(N, dtype=int)          # place IDs of tasks scheduled for this processor here, in order of their execution (start filling from taskIDs[0])
    startTime = numpy.empty(N, dtype=int)       # place starting times of scheduled tasks, so startTime[i] is start time of task with ID taskIDs[i] (all should be integers) (0 indexed)
    exeTime = numpy.empty(N, dtype=float)         # place actual execution times of tasks, so exeTime[i] is execution time of i-th task scheduled on this processor (0 indexed)

output: list = [ProcessorSchedule() for i in range(numberOfProcessors)]
class TaskWithDeadline:
    taskId: int
    dagDeadline: int

class TaskWithFinishTime:
    taskId: int
    finishTime: int

def cmp_aux(arg11, arg22):
    arg1: TaskWithDeadline = arg11  # (struct TaskWithDeadline *)arg11
    arg2: TaskWithDeadline = arg22  # (struct TaskWithDeadline *)arg22
    if (arg1.taskId) < (arg2.taskId):
        return -1
    if (arg1.taskId) == (arg2.taskId):
        return 0
    return 1


def cmp_aux2(arg11, arg22):
    arg1: TaskWithFinishTime = arg11  # (struct TaskWithFinishTime *)arg11
    arg2: TaskWithFinishTime = arg22  # (struct TaskWithFinishTime *)arg22
    if (arg1.taskId) < (arg2.taskId):
        return -1
    if (arg1.taskId) == (arg2.taskId):
        return 0
    return 1

begin = time 
end = time

def printer_function(filename: str) -> None:
    global dagsCount
    global numberOfProcessors
    global historyOfProcessor
    global begin
    global end
    global output
    global dagsInput
    with open(filename, "w") as f:
        for i in range(numberOfProcessors):
            for j in range(output[i].numberOfTasks):
                f.write(f"{output[i].taskIDs[j]} {output[i].startTime[j]} {output[i].startTime[j] + output[i].exeTime[j]},")
            f.write("\n")
        taskNumber: int = 0
        worstMakespan: float = 0
        for i in range(dagsCount):
            current: Task = dagsInput[i].firstTask
            while current != None:
                taskNumber += 1
                worstMakespan += current.executionTime
                current = current.next
            dep: Dependency = dagsInput[i].firstDependency
            while dep != None:
                worstMakespan += dep.transferTime
                dep = dep.next
        #makespan: ll = 0.0
        makespan: float = 0.0
        for i in range(numberOfProcessors):
            count: int = output[i].numberOfTasks
            if (count == 0):
                continue
            if (output[i].startTime[count - 1] + output[i].exeTime[count - 1] > makespan):
                makespan = output[i].startTime[count - 1]
        f.write(f"{makespan}\n")

        sumOfSquares: float = 0.0
        _sum: float = 0.0
        for i in range(numberOfProcessors):
            length: float = 0.0
            for j in range(output[i].numberOfTasks):
                p(output[i].exeTime[j])
                length += output[i].exeTime[j]
            #sumOfSquares += float(length * 1.0 / makespan) * float(length * 1.0 / makespan)
            #_sum += length / makespan
        sumOfSquares /= numberOfProcessors
        _sum /= numberOfProcessors
        _sum *= _sum
        sumOfSquares -= _sum
        stdev: float = math.sqrt(sumOfSquares)
        f.write(f"{stdev}\n")
        table1 = numpy.empty(N, dtype=TaskWithDeadline)
        table2 = numpy.empty(N, dtype=TaskWithFinishTime)
        done: int = 0
        for i in range(dagsCount):
            now: Task = dagsInput[i].listOfTasks
            while now != None:
                current: TaskWithDeadline = TaskWithDeadline()
                print(current)
                current.taskId = now.taskID
                current.dagDeadline = input[i].deadlineTime
                table1[done] = current
                done = done + 1
                now = now.next
        done = 0
        for i in range(numberOfProcessors):
            for j in range(output[i].numberOfTasks):
                current: TaskWithFinishTime = TaskWithFinishTime()
                current.taskId = output[i].taskIDs[j]
                current.finishTime = output[i].startTime[j] + output[i].exeTime[j]
                table2[done] = current
                done = done + 1
        p(table1)
        # sorted(table1)
        # sorted(table2)

        missedDeadlines: int = 0
        for i in range(taskNumber):
            if (table1[i].dagDeadline < table2[i].finishTime):
                missedDeadlines += 1
        costFunction: float = float(makespan) / worstMakespan * 10 + stdev
        costFunction = 1 / costFunction
        f.write(f"{costFunction}\n")
        time_spent: float = float(end - begin)
        spent: int = int(time_spent * 1000)
        f.write(f"{spent}\n")
        f.close()

inDegree = numpy.empty(N, dtype=object)                                         # auxiliary array ONLY for sample scheduler
topsortOrder = numpy.empty(N, dtype=object)                                     # auxiliary array ONLY for sample scheduler
earliestStart = numpy.empty(shape=[numberOfProcessors, N], dtype=object)        # auxiliary array ONLY for sample scheduler
executionTime = numpy.empty(N, dtype=object)                                     # auxiliary array ONLY for sample scheduler
taskType = numpy.empty(N, dtype=object)                                         # auxiliary array ONLY for sample scheduler


def schedule_task(procID: int, earliestPossibleStart: int, taskID: int, exeTime: int) -> int:
    lastFinish: int = 0
    taskCount: int = output[procID].numberOfTasks
    if taskCount > 0:
        lastFinish = output[procID].startTime[taskCount - 1] + output[procID].exeTime[taskCount - 1]
    if (earliestPossibleStart > lastFinish):
        lastFinish = earliestPossibleStart
    history: int = 0
    cache: bool = False
    j: int = taskCount - 1
    while j >= 0:
        if (taskType[output[procID].taskIDs[j]] == taskType[taskID]):
            cache = True
        history+=1
        if (history == historyOfProcessor):
            break
    if (cache):
        exeTime *= 9
        exeTime /= 10
    output[procID].startTime[taskCount] = lastFinish
    output[procID].exeTime[taskCount] = exeTime
    output[procID].taskIDs[taskCount] = taskID
    output[procID].numberOfTasks+=1
    return lastFinish + exeTime

def initialize_processors() -> None:
    for i in range(numberOfProcessors):
        output[i].numberOfTasks = 0

def schedule_dag(_id: int) -> None:
    current: Dependency = dagsInput[_id].firstDependency
    while current != None:
        _from: int = current.beforeID
        _to: int = current.afterID
        inDegree[_to]+=1
        current = current.next
    topsortSize: int = 0
    topsortPtr: int = 0
    ptr: Task = dagsInput[_id].firstTask
    arrivalTime: int = dagsInput[_id].arrivalTime
    while ptr != None:
        _id: int = ptr.taskID
        if inDegree[_id] == 0:
            topsortOrder[topsortSize] = _id
            topsortSize+=1
        executionTime[_id] = ptr.executionTime
        taskType[_id] = ptr.taskType
        for i in range(numberOfProcessors):
            earliestStart[i][_id] = arrivalTime
        ptr = ptr.next
    while topsortPtr < topsortSize:
        currentID: int = topsortOrder[topsortPtr]
        topsortPtr += 1
        _list: Dependency = dagsInput[_id].firstDependency
        while _list != None:
            _from: int = _list.beforeID
            _to: int = _list.afterID
            if currentID == _from:
                inDegree[_to]-=1
                if inDegree[_to] == 0:
                    topsortOrder[topsortSize] = _to
                    topsortSize += 1
            _list = _list.next
    for i in range(topsortSize):
        currentID: int = topsortOrder[i]
        finish: int = schedule_task(i % numberOfProcessors, earliestStart[i % numberOfProcessors][currentID], currentID, executionTime[currentID])
        _list = dagsInput[_id].firstDependency
        while _list != None:
            _from: int = _list.beforeID
            _to: int = _list.afterID
            if _from == currentID:
                for proc in range(numberOfProcessors):
                    bestTime: int = finish + _list.transferTime * (proc != (i % numberOfProcessors))
                    if bestTime > earliestStart[proc][_to]:
                        earliestStart[proc][_to] = bestTime
            _list = _list.next

def scheduler() -> None:
# fill this function, you have dags in input array, and you need to schedule them to processors in output array
    for i in range(dagsCount):
        #schedule dags one by one
        schedule_dag(i)

def main():
    argv = sys.argv
    p(f"args -> {argv}")
    reader_function(argv[1])
    initialize_processors()
    begin.time()
    scheduler()
    end.time()
    printer_function(argv[2])


main()
