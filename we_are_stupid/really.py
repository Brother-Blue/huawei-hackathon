from inspect import getframeinfo, stack
import json
from csv import writer, QUOTE_MINIMAL
from math import sqrt
import sys
import os
import threading
from time import time
from collections import defaultdict

BUFFER_SIZE: 3505
numberOfProcessors: int = 0
dagList: list = []
output: list
historyOfProcessor: int = 4
numberOfDags: int = 0
timeSpent: int = 0

globalExeTime: int = 0

# the key in the dag dict will be the ID of the task O(1)
# also contains its own id 
# class Task:
#     def __init__(self, id,  type, exeTime):
#         self.id = id
#         self.type = type
#         self.exeTime = exeTime
#     # def __init__(self, id, type, exeTime):
#     #     self.id = id
#     #     self.type = type
#     #     self.exeTime = exeTime

class DAG:
    def __init__(self, id, type, arrival, deadline, vertices, startRef):
        self.offset = (vertices - startRef) + startRef
        self.id: int = id
        self.type: int = type
        self.arrival: int = arrival
        self.deadline: int = deadline
        self.graph = defaultdict(list)  # dictionary containing adjacency List
        self.V = vertices #* 2 # No. of vertices
        self.startRef = startRef # to account for the id changes
        self.taskList = {} # keys are the task id pointing to a task object
        # using the order we know what tasks need to be complete in what order after we sort
        # you can use the id as ref to the dict for a O(1) look up to get the task and give it to a proc
        self.order: list = []
        self.worstMakespan: int = 0
        self.makespan: int = 0
        #debug(self.id)
        #debug(self.startRef)
        #debug(offset)
        # all task EET and Tranfer Times summed
    # function to add an edge to graph
    def addEdge(self, u, v):
        #debug(round(u, -3))
        v = int(str(v)[-3:])
        u = int(str(u)[-3:])
        self.graph[u].append(v)
    def addVertex(self, id: int, task: dict):
        self.taskList[id] = task
    # A recursive function used by topologicalSort
    def topologicalSortUtil(self, v, visited, stack):
        #debug(v+truncate_int(self.startRef))
        # Mark the current node as visited.
        visited[v] = True
        # Recur for all the vertices adjacent to this vertex
        for i in self.graph[v]:  # no touchy
            try:
                if visited[i] == False:
                    self.topologicalSortUtil(i, visited, stack)
            except:
                #debug(f"dagId:{self.id} startRef:{self.startRef} V:{self.V} index:{i} visted:{len(visited)}")
                #debug(truncate_int(self.startRef))
                pass
        # Push current vertex to stack which stores result
        stack.append(v + self.startRef)
    # The function to do Topological Sort. It uses recursive
    # topologicalSortUtil()
    def topologicalSort(self):
        # Mark all the vertices as not visited
        visited = [False] * self.V
        stack = []
        # Call the recursive helper function to store Topological
        # Sort starting from all vertices one by one
        for i in range(self.V):
            if visited[i] == False:
                self.topologicalSortUtil(i, visited, stack)
        # Print contents of the stack
        #self.order = stack[::-1]  # return list in reverse order
        #self.order = stack
        # holders while we get the range
        order = []
        taskRange = []
        # loop through and get the range
        for i in range(self.startRef, (self.startRef+self.offset)):
            taskRange.append(i)
        # filter our stack on the range
        for i in stack[::-1]:
            if i in taskRange:
                order.append(i)
        # set our ref to the order we need to do the tasks in
        self.order = order[::-1] #stack[::-1]
        # if(self.id==100):
        #     debug(self.order)
    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

    
class ProcessorSchedule:
    def __init__(self):
        self.cache = [0] * 4
        self.taskCount: int = 0 # place number of tasks scheduled for this processor here
        self.taskIDs : list = []     # place IDs of tasks scheduled for this processor here, in order of their execution (start filling from taskIDs[0])
        self.startTime: list = []    # place starting times of scheduled tasks, so startTime[i] is start time of task with ID taskIDs[i] (all should be integers) (0 indexed)
        self.exeTime: list = []      # place actual execution times of tasks, so exeTime[i] is execution time of i-th task scheduled on this processor (0 indexed)
        self.lastFinishTime: int = 0
    def add_task(self, taskID):
        self.taskIDs.append(taskID)
        self.taskCount += 1
    #    self.get_previous_finish()

    # def set_last_finish(self, new_finish):
    #     self.lastFinishTime = new_finish

    def type_exists(self, taskType):
        return taskType in self.cache

    def cache_task(self, taskType):
        try:
            if not taskType in self.cache:
                self.cache.append(taskType)
        except IndexError:
            self.cache.pop()
            self.cache.append(taskType)

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)
# def truncate_int(n, keep=3):
#     if n < 0:  # account for '-'
#         keep += 1
#     s = str(n)
#     res = int(s[:keep] + '0'*(len(s) - keep))
#     if res < 100:
#         return 0
#         #debug(f"res:{res} is less then 100")
#     return res

def init_processors():
    global numberOfProcessors
    return [ProcessorSchedule() for proc in range(numberOfProcessors)]


def calcTime(spent: float):
    return [int((spent) * (10**3)), int((spent) * (10**6))]


def debug(message):
    caller = getframeinfo(stack()[1][0])
    print("[Line]:%d -> %s" % (caller.lineno, message))


def read_file(filename=os.path.join(os.getcwd(),"we_are_stupid", "sample.json")) -> dict:
    with open(filename, "r") as f:
        file = json.load(f)
        f.close()
    return file

def get_edges(d):
    return list(map(lambda kv: (int(kv[0][4:]), kv[1]),
        d.items()))

def reader_func(filename: str):
    global dagList
    global numberOfProcessors
    global numberOfDags
    global output
    testID = 0
    testCaseNum = filename.split(os.sep)[-1].split('.')[0][4:]
    if testCaseNum.isdigit():
        testID = int(testCaseNum)
    else:
        testID = None
    if not testID:
        numberOfProcessors = 3
    elif testID <= 6:
        numberOfProcessors = 8
    else:
        numberOfProcessors = 6
       
    output = init_processors()
    dags = read_file(filename)
    numberOfDags = len(dags.keys())
    for i, (dagId, vals) in enumerate(dags.items()):
        taskKeys = list(filter(lambda key: "Task" in str(key), vals))
        dag = DAG(
                int(dagId[3:]),
                vals['Type'],
                vals['ArrivalTime'],
                vals['Deadline'],
                len(taskKeys),
                int(taskKeys[0][4:])
            )
        for i, task in enumerate(taskKeys):
            edges = get_edges(vals[task]["next"])
            taskId = int(task[4:])
            dag.addVertex(
                taskId,
                {
                    "taskID": taskId,
                    "EET": vals[task]['EET'],
                    "Type": vals[task]['Type'],
                    "next": vals[task]['next'],
                    "edges": edges
                })
            edges = get_edges(vals[task]["next"])
            for edge in edges:  # node requies this node
                dag.addEdge(taskId, edge[0])
        dag.topologicalSort()

        # at this point we need to calculate the makespan of the dag
        dag_makespan = 0
        for task, vals in dag.taskList.items():
            transit = 0
            for v in vals["next"].values():
                transit += v
            # debug(transit)
            dag_makespan += (vals["EET"] + transit)
        dag.makespan = dag_makespan

        # then append
        dagList.append(dag)

    dagList = sorted(dagList, key=lambda dag: (dag.arrival, dag.deadline))
    

def calc_utility_function(makespan, stdev, worstCaseAppTime):
    normMakespan = makespan / worstCaseAppTime
    return 1 / ((10 * normalize_makespan(makespan)) + stdev)

def normalize_makespan(makespan):
    worstCaseDagSum = 0
    for dag in dagList:
        worstCaseDagSum += dag.worstMakespan
    return makespan / worstCaseDagSum

def printer_func(filename: str):
    with open(filename, mode='w') as f:
        w = writer(f, delimiter=',', quotechar='"', quoting=QUOTE_MINIMAL)
        # for each processor loop
        # processor 1 every column in row has a Tuple(TaskID, Starttime, Finishtime)
        # output[i].taskIDs[j], output[i].startTime[
        #    j], output[i].startTime[j] + output[i].exeTime[j]
        
       
        def daddy_makespan_owo():
            makespan: int = 0
            for procID in range(numberOfProcessors):
                if (output[procID].startTime[output[procID].taskCount - 1] + output[i].exeTime[output[procID].taskCount - 1] > makespan):
                    makespan = output[procID].startTime[output[procID].taskCount - 1] + output[procID].exeTime[output[procID].taskCount - 1]
            return makespan
        daddy_makespan = daddy_makespan_owo()
        ### WRITE STUFF TO FILE ###
        for procID in enumerate(output):
            w.writerow([
                'Tuple(TaskID, Starttime, Finishtime)',
                'Tuple(TaskID, Starttime, Finishtime)',
                'Tuple(TaskID, Starttime, Finishtime)'
            ])
        # Makespan is the time where all instances of all DAGs of the application are completed.
        w.writerow(daddy_makespan)
        # stdev (standard deviation of processor loads)
        w.writerow([getSTD(daddy_makespan)])
        # utiliy function
        w.writerow([calc_utility_function(1, 1, 1)])
        # execution time of the scheduler (in miliseconds)
        w.writerow([int((timeSpent) * (10**3))])

# ;)
def getSTD(makespan: int):
    stdev: int
    sumOfSquares: float = 0
    _sum: float = 0
    for i in range(numberOfProcessors):
        length: float = 0
        for j in range(output[i].taskCount):
            length += output[i].exeTime[j]
            sumOfSquares += (float(length * 1.0 / makespan)**2)
            _sum += length / makespan
    sumOfSquares /= numberOfProcessors
    _sum /= numberOfProcessors
    _sum *= _sum
    sumOfSquares -= _sum
    stdev = sqrt(sumOfSquares)
    return stdev

"""
The execution of tasks are non-preemptive, in the sense that a task that starts executing 
on a processor will not be interrupted by other tasks until its execution is completed.
"""
def schedule_task(procID: int, task: dict):
    # schedule task at processing node, after previously scheduled tasks
    
    debug(procID)
    lastFinish = output[procID].lastFinishTime

    debug(lastFinish)
    # Grab last 4 items in taskIDs list
    history = output[procID].taskIDs[-4:]
    cached = task["taskID"] in history
    debug(cached)
    taskID = task['taskID']
    exeTime = task["EET"]
    taskType = task["Type"]
    if (cached):
        # 10% shorter execution time if cached type
        exeTime *= 0.9
    debug(exeTime)
    ### check cache allocate depending makesure to take into account load balance
    # exetime, 
    output[procID].add_task(taskID)
    output[procID].exeTime.append(taskID)
    output[procID].startTime.append(lastFinish)
    output[procID].lastFinishTime += exeTime

def schedule_dag(dag: DAG):
    ### allocate depending makespan to take into account load balance
    tasksPopped = 0
    while dag.order:
        taskID = dag.order.pop()
        task = dag.taskList[taskID]
        procID = tasksPopped % numberOfDags
        schedule_task(procID, task)
        tasksPopped += 1

# Ignore me for now (:
threads = []
for proc in range(numberOfProcessors):
    """
    thread = threading.Thread(target=function_name_replace_me, args=(,))
    threads.append(thread)
    """
    pass

for thread in threads:
    """thread.start()"""
    pass

# Threads running
### Do stuff here while threads run

# Threads stop and return here
for thread in threads:
    """
    thread.join(timeout=1)
    """
    pass

def scheduler():
    for dag in dagList:
        schedule_dag(dag)

#################
## print funcs ##
#################
def print_dagInfo():   
    totalTaskCount = 0
    averageDagMakespan = 0
    for d in dagList:
        averageDagMakespan += d.worstMakespan
        totalTaskCount += len(d.taskList.keys())
        """A DAG’s deadline is relative to its release time which denoted by 𝑑_𝑖. 
        For example, if the deadline of a DAG is 3 and the release time of its ithinstance is 12, 
        it should be completed before 15."""
        #debug(f"arriv: {d.arrival} makespan: {d.makespan} deadline: {d.deadline}")
    """All time units are in micro second."""
    # debug(f"averageDagMakespan: {int(averageDagMakespan/numberOfDags)}")
    # debug(f"numberOfProcessors: {numberOfProcessors}")
    # debug(f"numberOfDags: {numberOfDags} totalTaskCount: {totalTaskCount}")

def print_processors():
    for i in output:
        debug(i)

def print_time(spent: float):
    t1 = calcTime(spent)
    print(f"spent: {t1[0]} - ms | {t1[1]} - μs")

def run():
    global timeSpent
    # required
    args = sys.argv
    reader_func(args[1])

    # utility for info
    # print_dagInfo()

    # required
    b = time()
    scheduler()
    e = time()
    timeSpent = (e - b)

    # utility for info
    print_time(timeSpent)
    # print_processors()
    # required
    printer_func(args[2])

if __name__ == '__main__':
    run()