# include <stdio.h>
# include <stdbool.h>
# include <stdlib.h>
# include <json-c/json.h>
# include <string.h>
# include <math.h>
# include <time.h>
# define ll long long int

import json
import math
import time
import sys
from decimal import Decimal
from typing import Any

N = 3505
numberOfProcessors = 3
historyOfProcessor = 4

## helper func for fprintf
def fprintf(stream):
    stream.write("blah %d" % 5)
    fprintf(sys.stderr)


## end #


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


dagInput = []
dagsCount = 0  # total number of DAGs (0 indexed in array "dagInput")


def initialize(whichDag):
    dagInput[whichDag] = DAG()
    dagInput[whichDag].listOfTasks = None
    dagInput[whichDag].listOfDependencies = None
    dagInput[whichDag].lastDependency = None
    dagInput[whichDag].lastTask = None
    dagInput[whichDag].firstDependency = None
    dagInput[whichDag].firstTask = None


def add_task_to_list(whichDag, taskID, executionTime, taskType):
    if dagInput[whichDag].lastTask == None:
        dagInput[whichDag].listOfTasks = Task()
        dagInput[whichDag].lastTask = dagInput[whichDag].listOfTasks
        dagInput[whichDag].firstTask = dagInput[whichDag].lastTask

    else:
        dagInput[whichDag].lastTask.next = Task()
        dagInput[whichDag].lastTask = dagInput[whichDag].lastTask.next

    dagInput[whichDag].lastTask.taskID = taskID
    dagInput[whichDag].lastTask.executionTime = executionTime
    dagInput[whichDag].lastTask.taskType = taskType
    dagInput[whichDag].lastTask.next = None
    return


def add_dependency_to_list(whichDag, beforeID, afterID, transferTime):
    if dagInput[whichDag].lastDependency == None:
        dagInput[whichDag].listOfDependencies = Dependency()
        dagInput[whichDag].lastDependency = dagInput[whichDag].listOfDependencies
        dagInput[whichDag].firstDependency = dagInput[whichDag].lastDependency

    else:
        dagInput[whichDag].lastDependency.next = Dependency()
        dagInput[whichDag].lastDependency = dagInput[whichDag].lastDependency.next

    dagInput[whichDag].lastDependency.beforeID = beforeID
    dagInput[whichDag].lastDependency.afterID = afterID
    dagInput[whichDag].lastDependency.transferTime = transferTime
    dagInput[whichDag].lastDependency.next = None
    return


def print_dag_tasks(whichDag):
    # "whichDag" is index of DAG in array "dagInput"
    current: Task = dagInput[whichDag].firstTask
    while current != None:
        print("%d ", current.taskID)
        current = current.next
    print("\n")


def print_dag_dependencies(whichDag):
    # "whichDag" is index of DAG in array "dagInput"
    current: Dependency = dagInput[whichDag].firstDependency
    while current != None:
        print(
            "FROM: %d TO: %d COST: %d\n",
            current.beforeID,
            current.afterID,
            current.transferTime,
        )
        current = current.next
    print("\n")


def reader_function(filename):
    """
    Reads in DAG data from a given json file (see sample.json)
    Initializes all DAGs with the given data.
    """
    # Read sample json file
    f = open('sample.json', 'r')
    data = json.load(f)

    # For each top-level data point in data
    for d in data:
        # tasks for each dag
        tasks = [] 
        dag = DAG()
        # Needed to avoid out of bounds
        dagInput.append(None) 
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
    pass
    # numberOfTasks; # place number of tasks scheduled for self processor here
    # taskIDs[N];    # place IDs of tasks scheduled for self processor here, order of their execution (start filling from taskIDs[0])
    # startTime[N];  # place starting times of scheduled tasks, startTime[i] is start time of task with ID taskIDs[i] (all should be integers) (0 indexed)
    # exeTime[N];    # place actual execution times of tasks, exeTime[i] is execution time of i-th task scheduled on self processor (0 indexed)


# numberOfProcessors is the size of arr
output = []  # arr of ProcessorSchedule

# auxiliary structures for output function (you dont have to read them)
class TaskWithDeadline:
    taskId: int
    dagDeadline: int


class TaskWithFinishTime:
    taskId: int
    finishTime: int


def cmp_aux(self, arg11, arg22):
    arg1: TaskWithDeadline = arg11  # (struct TaskWithDeadline *)arg11
    arg2: TaskWithDeadline = arg22  # (struct TaskWithDeadline *)arg22
    if (arg1.taskId) < (arg2.taskId):
        return -1
    if (arg1.taskId) == (arg2.taskId):
        return 0
    return 1


def cmp_aux2(self, arg11, arg22):
    arg1: TaskWithFinishTime = arg11  # (struct TaskWithFinishTime *)arg11
    arg2: TaskWithFinishTime = arg22  # (struct TaskWithFinishTime *)arg22
    if (arg1.taskId) < (arg2.taskId):
        return -1
    if (arg1.taskId) == (arg2.taskId):
        return 0
    return 1


# t0 = time.time()
# code_block
# t1 = time.time()

# total = t1-t0
# clock_t begin, end


def printer_function(self, filename):
    t0 = time.time()
    # call self function once output table is filled, will automaticly write data to file in correct format
    with open(filename, "w") as f:
        i: int = 0
        while i < numberOfProcessors:
            j: int = 0
            while j < output[i].numberOfTasks:
                fprintf(
                    f,
                    "%d %d %d,",
                    output[i].taskIDs[j],
                    output[i].startTime[j],
                    output[i].startTime[j] + output[i].exeTime[j],
                )
                j += 1
            fprintf(f, "\n")
            i += 1

    taskNumber: int = 0
    worstMakespan: int = 0
    # added #
    i: int = 0
    while i < dagsCount:
        # this one line needs to be fixed (input)
        current: Task = input[i].firstTask
        while current != None:
            taskNumber += 1
            worstMakespan += current.executionTime
            current = current.next

        dep: Dependency = dagInput[i].firstDependency
        while dep != None:
            worstMakespan += dep.transferTime
            dep = dep.next
        i += 1
    # end #
    # for (i = 0; i < dagsCount; i++):
    #     current: task = dagdagInput[i].firstTask
    #     while (current != None):
    #         taskNumber += 1
    #         worstMakespan += current.executionTime
    #         current = current.next

    #     dep: dependency = dagInput[i].firstDependency
    #     while (dep != None):
    #         worstMakespan += dep.transferTime
    #         dep = dep.next

    makespan: int = 0
    i: int = 0
    while i < numberOfProcessors:
        count: int = output[i].numberOfTasks
        if count == 0:
            continue
        if output[i].startTime[count - 1] + output[i].exeTime[count - 1] > makespan:
            makespan = output[i].startTime[count - 1] + output[i].exeTime[count - 1]
        i += 1
        # for (i = 0; i < numberOfProcessors; i++):
        #     count: int = output[i].numberOfTasks
        #     if count == 0:
        #         continue
        #     if output[i].startTime[count - 1] + output[i].exeTime[count - 1] > makespan:
        #         makespan = output[i].startTime[count - 1] + output[i].exeTime[count - 1]

        fprintf(f, "%lld\n", makespan)

        sumOfSquares: int = 0
        sum: int = 0
        i: int = 0
        while i < numberOfProcessors:
            length = 0
            j: int = 0
            while j < output[i].numberOfTasks:
                # for (j = 0; j < output[i].numberOfTasks; j++)
                length += output[i].exeTime[j]
                j += 1
            sumOfSquares += Decimal(length * 1.0 / makespan) * Decimal(
                length * 1.0 / makespan
            )
            sum += length / makespan
            i += 1
        # for (i = 0; i < numberOfProcessors; i++)
        #     length = 0
        #     for (j = 0; j < output[i].numberOfTasks; j++)
        #         length += output[i].exeTime[j]
        #     sumOfSquares += (double)(length * 1.0 / makespan) * (double)(length * 1.0 / makespan)
        #     sum += length / makespan

        sumOfSquares /= numberOfProcessors
        sum /= numberOfProcessors
        sum *= sum
        sumOfSquares -= sum
        stdev = math.sqrt(sumOfSquares)
        fprintf(f, "%0.6lf\n", stdev)

        # struct TaskWithDeadline table1[N]
        # struct TaskWithFinishTime table2[N]

        table1: TaskWithDeadline = []
        table2: TaskWithDeadline = []
        done = 0
        i: int = 0
        while i < dagsCount:
            now: Task = dagInput[i].listOfTasks
            while now != None:
                current: TaskWithDeadline
                current.taskId = now.taskID
                current.dagDeadline = dagInput[i].deadlineTime
                done += 1
                table1[done] = current
                now = now.next
            i += 1
        # for (i = 0; i < dagsCount; i++)
        #     struct task *now = dagInput[i].listOfTasks
        #     while (now != None)
        #         struct TaskWithDeadline current
        #         current.taskId = now.taskID
        #         current.dagDeadline = dagInput[i].deadlineTime
        #         table1[done++] = current
        #         now = now.next

    done: int = 0
    i: int = 0
    while i < numberOfProcessors:
        j: int = 0
        while j < output[i].numberOfTasks:
            current: TaskWithFinishTime
            current.taskId = output[i].taskIDs[j]
            current.finishTime = output[i].startTime[j] + output[i].exeTime[j]
            done += 1
            table2[done] = current
            j += 1
        i += 1
    # for (i = 0; i < numberOfProcessors; i++)
    #     for (j = 0; j < output[i].numberOfTasks; j++)
    #         current: TaskWithFinishTime
    #         current.taskId = output[i].taskIDs[j]
    #         current.finishTime = output[i].startTime[j] + output[i].exeTime[j]
    #         table2[done++] = current

    # qsort(table1, taskNumber, sizeof(struct TaskWithDeadline), cmp_aux)
    # qsort(table2, taskNumber, sizeof(struct TaskWithFinishTime), cmp_aux2)

    # sys.getsizeof(object[, default]):
    sorted(table1, taskNumber, sys.getsizeof(TaskWithDeadline), cmp_aux)
    sorted(table2, taskNumber, sys.getsizeof(TaskWithFinishTime), cmp_aux2)
    #qsort(table1, taskNumber, sys.getsizeof(TaskWithDeadline), cmp_aux)
    #qsort(table2, taskNumber, sys.getsizeof(TaskWithFinishTime), cmp_aux2)

    missedDeadlines = 0
    i: int = 0
    while i < taskNumber:
        # for (i = 0; i < taskNumber; i++)
        if table1[i].dagDeadline < table2[i].finishTime:
            missedDeadlines += 1

    # is the whole thing or just the first one
    costFunction = Decimal(makespan) / worstMakespan * 10 + stdev
    costFunction = 1 / costFunction
    fprintf(f, "%0.3lf\n", costFunction)
    t1 = time.time()
    time_spent = t1 - t0  # almost got it i think
    # time_spent = Decimal(end - begin) / CLOCKS_PER_SEC
    spent = int(time_spent * 1000)  # idk about the clocks_per_sec
    fprintf(f, "%d\n", spent)
    f.close()
    # fclose(f)


# int inDegree[N];                          # auxiliary array ONLY for sample scheduler
# int topsortOrder[N];                      # auxiliary array ONLY for sample scheduler
# int earliestStart[numberOfProcessors][N]; # auxiliary array ONLY for sample scheduler
# int executionTime[N];                     # auxiliary array ONLY for sample scheduler
# int taskType[N];                          # auxiliary array ONLY for sample scheduler

inDegree = []  # auxiliary array ONLY for sample scheduler
topsortOrder = []  # auxiliary array ONLY for sample scheduler
earliestStart = (
    []
)  # [numberOfProcessors][]  # auxiliary array ONLY for sample scheduler
executionTime = []  # auxiliary array ONLY for sample scheduler
taskType = []  # auxiliary array ONLY for sample scheduler


def schedule_task(self, procID, earliestPossibleStart, taskID, exeTime):
    # schedule task at processing node, previously scheduled tasks
    lastFinish = 0
    taskCount = output[procID].numberOfTasks
    if taskCount > 0:
        lastFinish = (
            output[procID].startTime[taskCount - 1]
            + output[procID].exeTime[taskCount - 1]
        )
    if earliestPossibleStart > lastFinish:
        lastFinish = earliestPossibleStart

    # check if task of the same was in the nearest past
    history = 0
    cache = False
    # added #
    j: int = taskCount - 1
    while j >= 0:
        if taskType[output[procID].taskIDs[j]] == taskType[taskID]:
            cache = True
        history += 1
        if history == historyOfProcessor:
            break
        j -= 1
    # end #
    # for (j = taskCount - 1; j >= 0; j--):
    #     if (taskType[output[procID].taskIDs[j]] == taskType[taskID]):
    #         cache = True
    #     history+=1
    #     if history == historyOfProcessor:
    #         break

    if cache:
        # possibly 10% shorter execution time
        exeTime *= 9
        exeTime /= 10

    output[procID].startTime[taskCount] = lastFinish
    output[procID].exeTime[taskCount] = exeTime
    output[procID].taskIDs[taskCount] = taskID
    output[procID].numberOfTasks += 1
    return lastFinish + exeTime


def initialize_processors(self):
    # added #
    i: int = 0
    for i in numberOfProcessors:
        output[i].numberOfTasks = 0
    # end #
    # for (i = 0; i < numberOfProcessors; i++)
    #     output[i].numberOfTasks = 0


def schedule_dag(self, id):
    # schedule all tasks from dagInput[id]
    # NOTE: ONLY IN THE SAMPLE TEST we assume that task ids are smaller than N (for simplicity of sample solution) which is NOT TRUE for general case!
    current: Dependency = dagInput[id].firstDependency
    while current != None:
        _from: int = current.beforeID
        _to: int = current.afterID
        inDegree[_to] += 1
        current = current.next

    topsortSize: int = 0
    topsortPtr: int = 0
    ptr: Task = dagInput[id].firstTask
    arrivalTime = dagInput[id].arrivalTime
    # initialize topsort array
    while ptr != None:
        id = ptr.taskID
        if inDegree[id] == 0:
            topsortSize += 1
            topsortOrder[topsortSize] = id

        executionTime[id] = ptr.executionTime
        taskType[id] = ptr.taskType
        i: int = 0
        while i < numberOfProcessors:
            # for (i = 0; i < numberOfProcessors; i++)
            earliestStart[i][id] = arrivalTime

        ptr = ptr.next

    # add all tasks to topsort array
    while topsortPtr < topsortSize:
        topsortPtr += 1
        currentID = topsortOrder[topsortPtr]
        list: Dependency = dagInput[id].firstDependency
        while list != None:
            _from = list.beforeID
            _to = list.afterID
            if currentID == _from:
                inDegree[_to] -= 1
                if inDegree[_to] == 0:
                    idx = topsortSize + 1
                    topsortOrder[idx] = _to

            list = list.next

    # topsort[0 .... topsortSize - 1] is list of task IDs sorted in topological order
    i: int = 0
    # while (i < topsortSize):
    while i < topsortSize:
        # for (i = 0; i < topsortSize; i+=1)
        currentID = topsortOrder[i]
        # we schedule tasks on the processors cyclically
        finish = schedule_task(
            i % numberOfProcessors,
            earliestStart[i % numberOfProcessors][currentID],
            currentID,
            executionTime[currentID],
        )
        list: Dependency = dagInput[id].firstDependency
        while list != None:
            _from = list.beforeID
            _to = list.afterID
            if _from == currentID:
                proc: int = 0
                while proc < numberOfProcessors:
                    # for (proc = 0; proc < numberOfProcessors; proc++)
                    bestTime = finish + list.transferTime * (
                        proc != (i % numberOfProcessors)
                    )
                    if bestTime > earliestStart[proc][_to]:
                        earliestStart[proc][_to] = bestTime
                    proc += 1
        i += 1

        list = list.next


def scheduler(self):
    # fill self function, have dags in dagInput array, you need to schedule them to processors in output array
    # for (i = 0; i < dagsCount; i++):
    i: int = 0
    while i < dagsCount:
        # schedule dags one by one
        schedule_dag(i)
        i += 1


# def main(self, argc, argv):
def main(*argv):
    print(argv)
    # fill output table[0 - 7] (8 processors), each structure put IDs of scheduled tasks as described above
    reader_function(argv[1])
    initialize_processors()
    t0 = time.time()
    scheduler()
    # fill self
    t1 = time.time()
    total = t1 - t0
    # begin = clock()
    # scheduler(); # fill self
    # end = clock()
    printer_function(argv[2])
    # return 0

main()