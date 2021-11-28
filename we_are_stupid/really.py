from inspect import getframeinfo, stack
import json
from csv import writer, QUOTE_MINIMAL
import sys
import os
from time import time
from collections import defaultdict, deque  # deque

BUFFER_SIZE: 3505
dagList: list = []

class Task:
    def __init__(self, id, type, exeTime):
        self.id = id
        self.type = type
        self.exeTime = exeTime

class DAG:
    def __init__(self, id, type, arrival, deadline, vertices, startRef):
        self.id: int = id
        self.type: int = type
        self.arrival: int = arrival
        self.deadline: int = deadline
        self.graph = defaultdict(list)  # dictionary containing adjacency List
        self.V = vertices * 2 # No. of vertices
        self.startRef = startRef
        self.order: list = []
        # all task EET and Tranfer Times summed

    # function to add an edge to graph
    def addEdge(self, u, v):
        v = int(str(v)[-3:])
        u = int(str(u)[-3:])
        self.graph[u].append(v)

    # A recursive function used by topologicalSort

    def topologicalSortUtil(self, v, visited, stack):
        # Mark the current node as visited.
        visited[v] = True
        # Recur for all the vertices adjacent to this vertex
        for i in self.graph[v]:  # no touchy
            if visited[i] == False:
                self.topologicalSortUtil(i, visited, stack)
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
        self.order = stack[::-1]


    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

class Processor:
    pass


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
    return list(map(lambda kv: (int(kv[0][4:20]), kv[1]),
        d.items()))

def reader_func(filename: str = os.path.join(os.getcwd(), "we_are_stupid",
                                             "sample.json")):
    global dagList
    dags = read_file(filename)
    #dagList = [DAG() for i in dags.keys()]
    for i, (dagId, vals) in enumerate(dags.items()):
        taskKeys = list(filter(lambda key: "Task" in str(key), vals))
        dag = DAG(
                int(dagId[3:20]),
                vals['Type'],
                vals['ArrivalTime'],
                vals['Deadline'],
                len(taskKeys),
                int(taskKeys[0][4:20])
            )
        for i, task in enumerate(taskKeys):
            # dag.addVertex(i, int(task[4:20]), vals[task]['EET'],
            #                      vals[task]['Type'])
            edges = get_edges(vals[task]["next"])
            #debug(f"i:{i} task:{int(task[4:20])}")
            #debug(edges)
            e = list(filter(lambda edge: edge[0] == i, edges))
            for edge in edges:  # node requies this node
                dag.addEdge(int(task[4:]), edge[0])
        dag.topologicalSort()
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

def schedule_task(task: Task):
    pass

def schedule_dag(dag: DAG):
    scheduledTasks = dag.order
    while scheduledTasks:
        scheduledTasks.pop()

def scheduler():
    for dag in dagList:
        schedule_dag(dag)

def run():
    args = sys.argv
    reader_func(args[1])
    b = time()
    scheduler()
    e = time()
    spent = (e - b)
    t1 = calcTime(spent)
    print(f"spent: {t1[0]} - ms | {t1[1]} - μs")
    printer_func(args[2])

if __name__ == '__main__':
    run()