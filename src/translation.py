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
  input[whichDag].astTask.taskType = taskType
  input[whichDag].lastTask.next = None
  return

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

def reader_function(*filename):
# This function reads input from the json file
    FILE *fp
    char buffer[1024 * 1024]
    fp = fopen(filename, "r")
    fread(buffer, sizeof(char),                 
    fclose(fp)

    // get DAGs descriptions one by one, then parse it into struct

    struct json_object *parsedJson = json_tokener_parse(buffer)
    json_object_object_foreach(parsedJson, key, val):
        enum json_type type
        type = json_object_get_type(val)
        if (type == json_type_object):
        {
            initialize(dagsCount)
            json_object_object_foreach(val, keyR, valR)
            {
                enum json_type typeR
                typeR = json_object_get_type(valR)
                if (typeR == json_type_object):
                {
                    char buffer[20]
                    int taskID = atoi(strncpy(buffer, &keyR[4], 20))

                    struct json_object *executionTime, *taskType
                    json_object_object_get_ex(valR, "EET", &executionTime)
                    json_object_object_get_ex(valR, "Type", &taskType)
                    int exeTime = json_object_get_int(executionTime)
                    int type = json_object_get_int(taskType)
                    add_task_to_list(dagsCount, taskID, exeTime, type)
                    struct json_object *edges
                    bool exists = json_object_object_get_ex(valR, "next", &edges)
                    if (exists):
                    {
                        json_object_object_foreach(edges, taskTo, time)
                        {
                            int transferTime = json_object_get_int(time)
                            char buffer[20]
                            int afterID = atoi(strncpy(buffer, &taskTo[4], 20))
                            add_dependency_to_list(dagsCount, taskID, afterID, transferTime)
                        }
                    }
                }
            }
            struct json_object *type, *arrivalTime, *relativeDeadline
            json_object_object_get_ex(val, "Type", &type)
            json_object_object_get_ex(val, "ArrivalTime", &arrivalTime)
            json_object_object_get_ex(val, "Deadline", &relativeDeadline)
            input[dagsCount]->dagType = json_object_get_int(type)
            input[dagsCount]->arrivalTime = json_object_get_int(arrivalTime)
            input[dagsCount]->deadlineTime = json_object_get_int(relativeDeadline)                                                   
            input[dagsCount]->dagID = atoi(strncpy(buffer, &key[3], 20))
            dagsCount+=1
        }
    }
