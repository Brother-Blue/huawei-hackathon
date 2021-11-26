from functools import cmp_to_key
import math
import time
import json
import sys
from dataclasses import dataclass, field

#### UTIL TO REMOVE ####
from inspect import  getframeinfo, stack
useDebug = True
def debug(message):
    if(useDebug):
        caller = getframeinfo(stack()[1][0])
        print("[Line]:%d -> %s" % (caller.lineno, message)) # python3 syntax print
    else:
        return
#########END###########

class Node:
    def __init__(self, data=None):
        self.data = data
        self.next = None

class LinkedList:
    def __init__(self):
        self.head = None

# TODO make sure everything is typed
# TODO swap f"" strings to this
# "'X is %s' % (x,)"


@dataclass
class Task:
    taskID: int = 0
    executionTime: int = 0
    taskType: int = 0
    next: list = field(default_factory=list)


@dataclass
class Dependency:
    # "afterID" can be executed only after finishing "beforeID"
    beforeID: int = 0
    afterID: int = 0
    transferTime: int = 0
    next: list = field(default_factory=list)


@dataclass
class DAG(Node):
    dagID: int = 0
    dagType: int = 0
    arrivalTime: int = 0
    deadlineTime: int = 0
    listOfTasks: list = field(
        default_factory=list
    )  # list of all tasks (just their IDs and execution times, doesn't matter) of DAG
    listOfDependencies: list = field(
        default_factory=list
    )  # all edges (dependencies) of DAG (None if there are no dependencies)
    lastTask: Task = Task()
    lastDependency: Dependency = Dependency()
    firstTask: Task = Task()
    firstDependency: Dependency = Dependency()


@dataclass
class ProcessorSchedule:
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


N: int = 3505  # The size of all the arrays
numProcessors: int = 3
histOfProcessor: int = 4
output: list = [ProcessorSchedule() for i in range(numProcessors)]
begin = time
end = time

dagsList: LinkedList = LinkedList()  # [DAG() for i in N]
dagsCount: int = 0

inDegree: list = [0] * N  # auxiliary array ONLY for sample scheduler
topsortOrder: list = [None] * N  # auxiliary array ONLY for sample scheduler
earliestStart: list = [
    [None] * N for i in range(numProcessors)
]  # [numProcessors][[None] * N]        # auxiliary array ONLY for sample scheduler
executionTime: list = [None] * N  # auxiliary array ONLY for sample scheduler
taskType: list = [None] * N  # auxiliary array ONLY for sample scheduler

# Ya like dags?
def add_task_to_list(dagIndex, taskID, executionTime, taskType):
    """
    Create a linked list if no task exists
    If a task exists, append it to the end of the list
    Set prev Task.next -> newTask
    Set newTask.next -> None
    """
    dag = DAG()
    if dagsList.head == None:
        dag.listOfTasks = Task()
        dag.lastTask = dag.listOfTasks
        dag.firstTask = dag.lastTask
        dagsList.head = dag
    # Append a new task to the list
    else:
        while dagsList.head.next != None:
            dag = dagsList.head.next
        dag.lastTask.next = Task()
        dag.lastTask = dag.lastTask.next
    # Add in the task details
    dag.lastTask.taskID = taskID
    dag.lastTask.executionTime = executionTime
    dag.lastTask.taskType = taskType
    # Update the 'next' pointer
    dag.next = None


def add_dependency_to_list(dag, beforeID, afterID, transferTime):
    """
    Create a linked list if no dependency exists
    If a dependency exists, append it to the end of the list
    Set prev dependency.next -> newTask
    Set newTask.next -> None
    """
    # Creating a new dependency if no task exists
    if dagsList[dag].lastDependency == None:
        dagsList[dag].listOfDependencies = Dependency()
        dagsList[dag].lastDependency = dagsList[dag].listOfDependencies
        dagsList[dag].firstDependency = dagsList[dag].lastDependency
    # Append a new dependency to the list
    else:
        dagsList[dag].lastDependency.next = Dependency()
        dagsList[dag].lastDependency = dagsList[dag].lastDependency.next
    # Add in the dependency details
    dagsList[dag].lastDependency.beforeID = beforeID
    dagsList[dag].lastDependency.afterID = afterID
    dagsList[dag].lastDependency.transferTime = transferTime
    # Update the 'next' pointer
    dagsList[dag].lastDependency.next = None


def print_dag_tasks(dag):
    # "dag" is index of DAG in array "dagsList"
    current: Task = dagsList[dag].firstTask
    while current != None:
        print(f"{current.taskID}")
        current = current.next
    print("\n")


def print_dag_dependencies(dag):
    # "dag" is index of DAG in array "dagsList"
    current: Dependency = dagsList[dagsList.index(dag)].firstDependency
    while current != None:
        print(
            f"FROM: {current.beforeID} TO: {current.afterID} TIME (microseconds): {current.transferTime}\n"
        )
        current = current.next
    print("\n")


def reader_function(filename):
    global dagsCount
    global numProcessors
    global histOfProcessor
    global begin
    global end
    global output
    global dagsList
    """
    Reads in DAG data from a given json file (see sample.json)
    Initializes all DAGs with the given data.
    """
    # Read sample json file
    with open(filename, "r") as f:
        data = json.load(f)

        # For each top-level data point in data
        for d in data:
            dag = DAG()
            # Needed to avoid out of bounds
            #dagsList.append(None)
            # initialize(dagsCount)
            #dagsList[dagsCount] = dag
            # Init dag attrs
            # TODO: Find out why they use [3:20]
            dag.dagID = int(d[3:20])  # "DAG0" -> 0
            dag.dagType = data[d]["Type"]
            dag.arrivalTime = data[d]["ArrivalTime"]
            dag.deadlineTime = data[d]["Deadline"]
            # Filter out dag keys where they contain 'Task'
            taskKeys = list(filter(lambda key: "Task" in str(key), data[d].keys()))
            # tasks for each dag
            tasks = [None] * len(taskKeys)
            # For each Task key
            for index, dagTask in enumerate(taskKeys):
                task = Task()
                # Gets the task obj
                dagTask = data[d][dagTask]
                # Set task attrs
                # TODO: Find out why they use [4:20]
                task.taskID = int(taskKeys[index][4:20])  # "TASK1" -> 1
                task.executionTime = dagTask["EET"]
                task.taskType = dagTask["Type"]
                task.next = list(dagTask["next"])  # Task2, Task4
                add_task_to_list(
                    dagsCount, task.taskID, task.executionTime, task.taskType
                )
                for innerTask in task.next:
                    transferTime = int(dagTask["next"].get(innerTask))
                    innerTaskID = int(innerTask[4:20])
                    add_dependency_to_list(
                        dagsCount, task.taskID, innerTaskID, transferTime
                    )
                    # print_dag_dependencies(dag)
                # Append to tasks list
                tasks[index] = task
            # Set dag listOfTasks
            dag.listOfTasks = tasks
            dagsCount = +1
        f.close()


def cmp_aux(arg11, arg22):
    """
    Compare task deadlines
    """
    arg1: TaskWithDeadline = arg11  # (struct TaskWithDeadline *)arg11
    arg2: TaskWithDeadline = arg22  # (struct TaskWithDeadline *)arg22
    if (arg1.taskId) < (arg2.taskId):
        return -1
    if (arg1.taskId) == (arg2.taskId):
        return 0
    return 1


def cmp_aux2(arg11, arg22):
    """
    Compare task finish times
    """
    arg1: TaskWithFinishTime = arg11  # (struct TaskWithFinishTime *)arg11
    arg2: TaskWithFinishTime = arg22  # (struct TaskWithFinishTime *)arg22
    if (arg1.taskId) < (arg2.taskId):
        return -1
    if (arg1.taskId) == (arg2.taskId):
        return 0
    return 1


def printer_function(filename: str) -> None:
    global dagsCount
    global numProcessors
    global histOfProcessor
    global begin
    global end
    global output
    global dagsList
    with open(filename, "w") as f:
        for i in range(numProcessors):
            for j in range(output[i].numberOfTasks):
                f.write(
                    f"{output[i].taskIDs[j]} {output[i].startTime[j]} {output[i].startTime[j] + output[i].exeTime[j]},"
                )
            f.write("\n")
        taskNumber: int = 0
        worstMakespan: float = 0
        for i in range(dagsCount):
            currentTask: Task = dagsList[i].firstTask
            debug(f"line:{get_line()} -> current:{dagsList[i].firstTask}")
            while currentTask != None:
                taskNumber += 1
                worstMakespan += currentTask.executionTime
                currentTask = currentTask.next
            dep: Dependency = dagsList[i].firstDependency
            while dep != None:
                worstMakespan += dep.transferTime
                dep = dep.next
        makespan: float = 0.0
        for i in range(numProcessors):
            count: int = output[i].numberOfTasks
            if count == 0:
                continue
            if output[i].startTime[count - 1] + output[i].exeTime[count - 1] > makespan:
                makespan = output[i].startTime[count - 1]
                debug(f"line:{get_linenumber()} -> makespan:{makespan}")
        f.write(f"{makespan}\n")

        sumOfSquares: float = 0.0
        _sum: float = 0.0
        for i in range(numProcessors):
            length: float = 0.0
            for j in range(output[i].numberOfTasks):
                p(output[i].exeTime[j])
                length += output[i].exeTime[j]
            debug(f"line:{get_linenumber()} -> makespan:{makespan}")
            # sumOfSquares += float(length * 1.0 / makespan) * float(length * 1.0 / makespan)
            # _sum += length / makespan
        sumOfSquares /= numProcessors
        _sum /= numProcessors
        _sum *= _sum
        sumOfSquares -= _sum
        stdev: float = math.sqrt(sumOfSquares)
        f.write(f"{stdev}\n")
        table1: list = [None] * N  # numpy.empty(N, dtype=TaskWithDeadline)
        table2: list = [None] * N  # numpy.empty(N, dtype=TaskWithFinishTime)
        done: int = 0
        for i in range(dagsCount):
            now: Task = dagsList[i].listOfTasks
            while now != None:
                currentTaskWithDeadline: TaskWithDeadline = TaskWithDeadline()
                currentTaskWithDeadline.taskID = now[0].taskID
                currentTaskWithDeadline.dagDeadline = dagsList[i].deadlineTime
                table1[done] = currentTaskWithDeadline
                done = done + 1
                now = now[0].next  # next is an array
        done = 0
        for i in range(numProcessors):
            for j in range(output[i].numberOfTasks):
                currentTaskWithFinishTime: TaskWithFinishTime = TaskWithFinishTime()
                currentTaskWithFinishTime.taskID = output[i].taskIDs[j]
                currentTaskWithFinishTime.finishTime = (
                    output[i].startTime[j] + output[i].exeTime[j]
                )
                table2[done] = currentTaskWithFinishTime
                done = done + 1

        # sorted(table1)
        # sorted(table2)

        missedDeadlines: int = 0
        for i in range(taskNumber):
            if table1[i].dagDeadline < table2[i].finishTime:
                missedDeadlines += 1
        costFunction: float = float(makespan) / worstMakespan * 10 + stdev
        costFunction = 1 / costFunction
        f.write(f"{costFunction}\n")
        time_spent: float = float(end - begin)
        spent: int = int(time_spent * 1000.0)
        f.write(f"{spent}\n")
        f.close()


def schedule_task(
    procID: int, earliestPossibleStart: int, taskID: int, exeTime: int
) -> int:
    endTime: int = 0  # execution time of all tasks
    taskCount: int = output[procID].numberOfTasks
    if taskCount > 0:
        endTime = (
            output[procID].startTime[taskCount - 1]
            + output[procID].exeTime[taskCount - 1]
        )
    if earliestPossibleStart > endTime:
        endTime = earliestPossibleStart
    history: int = 0  # counts tasks completed
    cache: bool = False
    j: int = taskCount - 1
    # debug(f"line:{get_linenumber()} -> j:{j}")
    while j >= 0:
        # debug(f"line:{get_linenumber()} -> taskID[j]:{taskType[output[procID].taskIDs[j]]}")
        # debug(f"line:{get_linenumber()} -> taskType[taskID]:{taskType[taskID]}")
        if taskType[output[procID].taskIDs[j]] == taskType[taskID]:
            cache = True
        history += 1
        if history == histOfProcessor:
            break
    if cache:
        exeTime *= 9
        exeTime /= 10
    output[procID].startTime[taskCount] = endTime
    output[procID].exeTime[taskCount] = exeTime
    output[procID].taskIDs[taskCount] = taskID
    output[procID].numberOfTasks += 1
    return endTime + exeTime


def initialize_processors() -> None:
    global output
    for i in range(numProcessors):
        output[i].numberOfTasks = 0


def schedule_dag(_dagID: int) -> None:
    global dagsList
    currentDependency: Dependency = dagsList[_dagID].firstDependency
    while currentDependency != None:
        # debug(dagsList[_dagID].__str__())
        # debug(dagsList[_dagID].firstDependency.beforeID)
        fromID = dagsList[_dagID].firstDependency.beforeID
        toID = dagsList[_dagID].firstDependency.afterID
        # c code -> inDegree[to]++;
        # The amount of parent tasks pointing to this one
        inDegree[toID] = inDegree[toID] + 1
        currentDependency = dagsList[_dagID].firstDependency.next
    topsortSize: int = 0
    topsortPtr: int = 0
    ptr: Task = dagsList[_dagID].firstTask
    arrivalTime: int = dagsList[_dagID].arrivalTime
    while ptr != None:
        _taskID: int = ptr.taskID
        if inDegree[_taskID] == 0:
            topsortOrder[topsortSize] = _taskID
            topsortSize += 1
        executionTime[_taskID] = ptr.executionTime
        taskType[_taskID] = ptr.taskType
        for i in range(numProcessors):
            earliestStart[i][_taskID] = arrivalTime
        ptr = ptr.next
    while topsortPtr < topsortSize:
        currentID: int = topsortOrder[topsortPtr]
        topsortPtr += 1
        # debug(dagsList)
        _list: Dependency = dagsList[_taskID].firstDependency
        while _list != None:
            fromID: int =  dagsList[_taskID].firstDependency.beforeID
            toID: int =  dagsList[_taskID].firstDependency.afterID
            if currentID == fromID:
                inDegree[toID] = inDegree[toID] - 1
                if inDegree[toID] == 0:
                    topsortOrder[topsortSize] = toID
                    topsortSize += 1
            _list = _list.next
    for i in range(topsortSize):
        currentID: int = topsortOrder[i]
        finish: int = schedule_task(
            i % numProcessors,
            earliestStart[i % numProcessors][currentID],
            currentID,
            executionTime[currentID],
        )
        _list = dagsList[_taskID].firstDependency
        while _list != None:
            fromID: int = _list.beforeID
            toID: int = _list.afterID
            if fromID == currentID:
                for proc in range(numProcessors):
                    bestTime: int = finish + _list.transferTime * (
                        proc != (i % numProcessors)
                    )
                    if bestTime > earliestStart[proc][toID]:
                        earliestStart[proc][toID] = bestTime
            _list = _list.next


def scheduler() -> None:
    # fill this function, you have dags in input array, and you need to schedule them to processors in output array
    for i in range(dagsCount):
        for task in dagsList[i].listOfTasks:
            output[i].taskIDs.append(task.taskID)
            output[i].exeTime.append(task.executionTime)
        schedule_dag(i)


def main():
    argv = sys.argv  # argparse is better but i needed to test for now
    # p(f"args -> {argv}")
    reader_function(argv[1])
    initialize_processors()
    begin.time()
    scheduler()
    end.time()
    printer_function(argv[2])


main()
