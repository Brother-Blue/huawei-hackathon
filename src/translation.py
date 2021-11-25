import numpy
from typing import Any

N = 3505
numberOfProcessors = 3
historyOfProcessor = 4
# ll long long int #### -> not sure how to define this right this second


class task:
  taskID: int
  executionTime: int
  taskType: int
  next: Any # pointer to next task

class dependency:
  beforeID: int
  afterID: int
  transferTime: int
  next: Any # pointer to next dependency
  # def __init__(self):
  #   pass

class DAG:
  dagID: int
  dagType: int
  arrivalTime: int
  deadlineTime: int
  listOfTasks: task # list of all tasks (just their IDs and execution times, order doesn't matter) of DAG
  listOfDependencies: dependency # all edges (dependencies) of DAG (NULL if there are no dependencies)
  lastTask: task
  lastDependency: dependency
  firstTask: task
  firstDependency: dependency
  # def __init__(self):
  #   pass

input = [] # use numpy later to speed it up? -> numpy.empty(N, dtype=object)
dagsCount: int = 0 # // total number of DAGs (0 indexed in array "input")

def initialize(whichDag: int) -> None:
    input[whichDag] = task()
    input[whichDag].listOfTasks = None
    input[whichDag].listOfDependencies = None
    input[whichDag].lastDependency = None
    input[whichDag].lastTask = None
    input[whichDag].firstDependency = None
    input[whichDag].firstTask = None

#run for every task
def add_task_to_list(whichDag: int, taskID: int, executionTime: int, taskType: int) -> None:
  if(input[whichDag].lastTask == None):
    input[whichDag].listOfTasks = task()
    input[whichDag].lastTask = input[whichDag].listOfTasks
    input[whichDag].firstTask = input[whichDag].lastTask
  else:
    input[whichDag].lastTask.next = task()
    input[whichDag].lastTask = input[whichDag].lastTask.next
  input[whichDag].lastTask.taskID = taskID
  input[whichDag].lastTask.executionTime = executionTime
  input[whichDag].lastTask.taskType = taskType
  input[whichDag].lastTask.next = None
  return

#run for every dependency
def add_dependency_to_list(whichDag: int, beforeID: int, afterID: int, transferTime: int) -> None:
  if(input[whichDag].lastDependency is None):
    input[whichDag].listOfDependencies = dependency()
    input[whichDag].lastDependency = input[whichDag].listOfDependencies
    input[whichDag].firstDependency = input[whichDag].lastDependency
  else:
    input[whichDag].lastDependency.next = dependency()
    input[whichDag].lastDependency = input[whichDag].lastDependency.next
  input[whichDag].lastDependency.beforeID = beforeID
  input[whichDag].lastDependency.afterID = afterID
  input[whichDag].lastDependency.transferTime = transferTime
  input[whichDag].lastDependency.next = None
  return

def print_dag_tasks(whichDag: int) -> None: # "whichDag" is index of DAG in array "input"
  current: task = input[whichDag].firstTask
  while(current != None):
    print("%d ", current.taskID)
    current = current.next
    print("\n")

def print_dag_dependencies(whichDag: int) -> None: # "whichDag" is index of DAG in array "input"
  current: dependency = input[whichDag].firstDependency
  while(current != None):
    print("FROM: %d TO: %d COST: %d\n", current.beforeID, current.afterID, current.transferTime)
    current = current.next
  print("\n")

## stopped here for now
## @ reader_funtion




inDegree[N]: int # auxiliary array ONLY for sample scheduler
topsortOrder[N]: int; #auxiliary array ONLY for sample scheduler
earliestStart[numberOfProcessors][N]: int; #auxiliary array ONLY for sample scheduler
executionTime[N]: int  #auxiliary array ONLY for sample scheduler
taskType[N]: int #auxiliary array ONLY for sample scheduler

def schedule_task(procID: int, earliestPossibleStart: int, taskID: int, execTime: int): int
    # schedule task at processing node, after previously scheduled tasks
    lastFinish: int = 0
    taskCount: int = output[procID].numberOfTasks
    if(taskCount > 0) lastFinish = output[procID].startTime[taskCount - 1] + output[procID].execTime[taskCount - 1]
    if(earliestPossibleStart > lastFinish) lastFinish = earliestPossibleStart

    # check if task of the same was in the nearest past
    history: int = 0
    cache: bool = false
    for(int j=taskCount-1;j>=0;j--){ #fix tis
        if(taskType[output[procID].taskIDs[j]] == taskType[taskID]) cache = true
        history += 1;
        if(history == historyOfProcessor) break;
    }
    if(cache){
        # possibly 10% shorter execution time
        execTime *= 9;
        execTime /= 10;
    }
    output[procID].startTime[taskCount] = lastFinish
    output[procID].execTime[taskCount] = execTime
    output[procID].taskIDs[taskCount] = taskID
    output[procID].numberOfTasks += 1
    return lastFinish + execTime


def initialize_processors():
    for i in range(numberOfProcessors): # CHECK THIS IS RIGHT
        //initializes processors with 0 tasks each
        output[i].numberOfTasks = 0;


def schedule_dag(id: int) -> None: #FIX THIS
    # schedule all tasks from input[id]
    # ONLY IN THE SAMPLE TEST we assume that task ids are smaller than N (for simplicity of sample solution) which is NOT TRUE for general case!
    current: dependency = input[id].firstDependency
    while(current != None):
        from: int = current.beforeID
        to: int = current.afterID
        inDegree[to] += 1
        current = current.next
    
    topsortSize: int = 0
    topsortPtr: int = 0
    ptr: task = input[id].firstTask
    arrivalTime: int = input[id].arrivalTime

    # initialize topsort array
    while(ptr != None):
        id: int = ptr.taskID
        if(inDegree[id] == 0):
            topsortOrder[topsortSize++] = id
        executionTime[id] = ptr.executionTime
        taskType[id] = ptr.taskType
        for i in range(numberOfProcessors):
            earliestStart[i][id] = arrivalTime
        ptr = ptr.next

    # add all tasks to topsort array
    while(topsortPtr < topsortSize){
        int currentID = topsortOrder[topsortPtr++]
        struct dependency * list = input[id].firstDependency
        while(list != None):
            from: int = list.beforeID
            to: int = list.afterID
            if(currentID == from):
                inDegree[to]--
                if(inDegree[to] == 0):
                    topsortOrder[topsortSize++] = to
            list = list.next

    # topsort[0 .... topsortSize - 1] is list of task IDs sorted in topological order
    for i in range(topsortSize): 
        int currentID = topsortOrder[i]
        # we schedule tasks on the processors cyclically
        int finish = schedule_task(i % numberOfProcessors, earliestStart[i % numberOfProcessors][currentID], currentID, executionTime[currentID]);
        list: dependency = input[id].firstDependency;
        while(list != None):
            from: int = list.beforeID;
            to: int = list.afterID;
            if(from == currentID):
                for(int proc=0;proc<numberOfProcessors;proc++):
                    bestTime: int = finish + list.transferTime * (proc != (i % numberOfProcessors));
                    if(bestTime > earliestStart[proc][to]):
                        earliestStart[proc][to] = bestTime;
            list = list.next;