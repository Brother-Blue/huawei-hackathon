from inspect import getframeinfo, stack
import json
from csv import writer, QUOTE_MINIMAL
import sys
import os
from time import time
from collections import defaultdict

BUFFER_SIZE: 3505
numberOfProcessors: int = 0
dagList: list = []
historyOfProcessor: int = 4

numberOfDags: int = 0

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
        self.order = order #stack[::-1]
        # if(self.id==100):
        #     debug(self.order)
    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

class ProcessorSchedule:
    numberOfTasks : int = 0 # place number of tasks scheduled for this processor here
    taskIDs : list = []     # place IDs of tasks scheduled for this processor here, in order of their execution (start filling from taskIDs[0])
    startTime: list = []    # place starting times of scheduled tasks, so startTime[i] is start time of task with ID taskIDs[i] (all should be integers) (0 indexed)
    exeTime: list = []      # place actual execution times of tasks, so exeTime[i] is execution time of i-th task scheduled on this processor (0 indexed)

# def truncate_int(n, keep=3):
#     if n < 0:  # account for '-'
#         keep += 1
#     s = str(n)
#     res = int(s[:keep] + '0'*(len(s) - keep))
#     if res < 100:
#         return 0
#         #debug(f"res:{res} is less then 100")
#     return res

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

def reader_func(filename: str = os.path.join(os.getcwd(), "we_are_stupid",
                                             "sample.json")):
    global dagList
    global numberOfProcessors
    global numberOfDags
    testID = 0
    if not filename[-7:-5].split()[0].isdigit():
        testID = int(filename[-6:-5].split()[0])
    else:
        testID = int(filename[-7:-5].split()[0])

    if testID <= 6:
        numberOfProcessors = 8
    else:
        numberOfProcessors = 6

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
            #debug(task)
            #dag.addVertex(int(task[4:20]))
            edges = get_edges(vals[task]["next"])
            taskId = int(task[4:])
            #debug(taskId)
            dag.addVertex(
                taskId, #int(task[4:]),
                {
                    "TaskID": int(task[4]),
                    "EET": vals[task]['EET'],
                    "Type": vals[task]['Type'],
                    "next": vals[task]['next'],
                    "edges": edges
                })
            edges = get_edges(vals[task]["next"])
            for edge in edges:  # node requies this node
                dag.addEdge(int(task[4:]), edge[0])
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

def calc_utility_function(makespan, stdev, worstCaseAppTime):
    normMakespan = makespan / worstCaseAppTime
    return 1 / ((10 * normalize_makespan(makespan)) + stdev)

def normalize_makespan(makespan):
    worstCaseDagSum = 1
    # for dag in dagList:
    #     worstCaseDagSum = getDagWorstCase
    return makespan / worstCaseDagSum

def printer_func(filename: str ="default.csv"):
    with open(filename, mode='w') as f:
        w = writer(f, delimiter=',', quotechar='"', quoting=QUOTE_MINIMAL)
        # for each processor loop
        # processor 1 every column in row has a Tuple(TaskID, Starttime, Finishtime)
        # output[i].taskIDs[j], output[i].startTime[
        #    j], output[i].startTime[j] + output[i].exeTime[j]
        w.writerow([
            'Tuple(TaskID, Starttime, Finishtime)',
            'Tuple(TaskID, Starttime, Finishtime)',
            'Tuple(TaskID, Starttime, Finishtime)'
        ])
        # makespan
        w.writerow([108])
        # stdev (standard deviation of processor loads)
        w.writerow([0.082932])
        # utiliy function
        w.writerow([calc_utility_function(1, 1, 1)])
        # execution time of the scheduler (in miliseconds)
        w.writerow([3])

def schedule_task(task: dict):
    pass

def schedule_dag(dag: DAG):
    if dag.id == 100:
        #debug(dag.id)
        #debug(dag.taskList.items())
        idx = dag.order.pop()

def scheduler():
    for dag in dagList:
        #debug(f"arriv: {dag.arrival} makespan: {dag.makespan} deadline: {dag.deadline}")
        #schedule_dag(dag)
        pass


def run():
    args = sys.argv
    reader_func(args[1])
    totalTaskCount = 0
    averageDagMakespan = 0
    for d in dagList:
        averageDagMakespan += d.makespan
        totalTaskCount += len(d.taskList.keys())
    debug(int(averageDagMakespan/numberOfDags))
    debug(f"numberOfProcessors: {numberOfProcessors}")
    debug(f"numberOfDags: {numberOfDags} totalTaskCount: {totalTaskCount}")
    b = time()
    scheduler()
    e = time()
    spent = (e - b)
    t1 = calcTime(spent)
    print(f"spent: {t1[0]} - ms | {t1[1]} - μs")
    printer_func(args[2])

if __name__ == '__main__':
    run()