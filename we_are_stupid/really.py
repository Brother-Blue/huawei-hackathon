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
        self.V = vertices  # No. of vertices
        self.startRef = startRef
        self.order = deque

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
        self.order = stack


    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

# class DAG:
#     def __init__(self, size, id, type, arrival, deadline):
#         self.id: int = id
#         self.type: int = type
#         self.arrival: int = arrival
#         self.deadline: int = deadline
#         #self.tasks: list = []
#         self.graph = defaultdict(list) # dictionary containing adjacency List
#         self.verticies = {}
#         self.size: int = size
#         self.i = 0

#     def addVertex(self, idx: int, taskID: int, eet: int, taskType: int):
#         self.verticies[idx] = {
#             'TaskID': taskID,
#             'EET': eet,
#             'Type': taskType,
#         }
#     # def addEdge(self, u, v):
#     #     self.graph[u].append(v)

#     # function to add an edge to graph
#     def addEdge(self, idx, fromTask, toTask, transferTime):
#         # dagList = [] loop index != id | vertex is the task
#         self.graph[idx].append({
#             'from': fromTask,
#             'to': toTask,
#             'transferTime': transferTime
#         })
#         debug(self.graph[0])
#         self.i+=1

#     # A recursive function used by topologicalSort
#     # def topologicalSortUtil(self, vertex, visited, stack):
#     #     # Mark the current node as visited.
#     #     # debug(f"Vertex: {vertex}")
#     #     visited[vertex] = True
#     #     #debug(vertex)
#     #     # Recur for all the vertices adjacent to this vertex
#     #     for i in self.graph[vertex]:
#     #         # i is
#     #         debug(i)
#     #         idx = i["to"]
#     #         if not visited[idx]:
#     #             self.topologicalSortUtil(idx, visited, stack)
#     #         # if not visited[i['to']]:
#     #         #     self.topologicalSortUtil(i['to'], visited, stack)
#     #     stack.append(vertex)

#     #     # Push current vertex to stack which stores result
#     #     #stack.append(vertex)

#     # # def topologicalSortUtil(self, v, visited, stack):
#     # #     # Mark the current node as visited.
#     # #     visited[v] = True
#     # #     # Recur for all the vertices adjacent to this vertex
#     # #     for i in self.graph[v]:
#     # #         if visited[i] == False:
#     # #             self.topologicalSortUtil(i, visited, stack)
#     # #     # Push current vertex to stack which stores result
#     # #     stack.append(v)
#     # # The function to do Topological Sort. It uses recursive
#     # # topologicalSortUtil()


#     # # The function to do Topological Sort. It uses recursive
#     # # topologicalSortUtil()
#     # def topologicalSort(self):
#     #     # Mark all the vertices as not visited
#     #     visited = [False] * 10000000
#     #     stack = []
#     #     # Call the recursive helper function to store Topological
#     #     # Sort starting from all vertices one by one
#     #     for i, key in enumerate(self.graph):
#     #         if not visited[i]:
#     #             self.topologicalSortUtil(i, visited, stack)
#     #     # Print contents of the stack
#     #     stack = stack[::-1]  # return list in reverse order
#     #     #stack = set(stack)
#     #     debug(stack)

#     def __str__(self):
#         return str(self.__class__) + ": " + str(self.__dict__)

#     def topologicalSortUtil(self, v, visited, stack):
#         # Mark the current node as visited.
#         visited[v] = True
#         # Recur for all the vertices adjacent to this vertex
#         for i in range(self.size):
#             if visited[i] == False:
#                 self.topologicalSortUtil(i, visited, stack)
#         # Push current vertex to stack which stores result
#         stack.append(self.graph[v])
#     # The function to do Topological Sort. It uses recursive
#     # topologicalSortUtil()
#     def topologicalSort(self):
#         # Mark all the vertices as not visited
#         visited = [False] * self.size
#         stack = []
#         # Call the recursive helper function to store Topological
#         # Sort starting from all vertices one by one
#         for i in range(self.size):
#             #debug(self.graph)
#             if visited[i] == False:
#                 self.topologicalSortUtil(i, visited, stack)
#         # Print contents of the stack
#         return stack[::-1]  # return list in reverse order



class Processor:
    pass


def calcTime(spent: float):
    return int((spent) * (10**3))


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

        dagList.append(dag)



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
        # ulitiy function
        w.writerow([0.141])
        # execution time of the scheduler (in miliseconds)
        w.writerow([3])


def scheduler():
    pass


def run():
    args = sys.argv
    if (len(args) > 1):
        reader_func(args[1])
    else:
        reader_func()
    for dag in dagList:
        dag.topologicalSort()
    while len(dagList[1].order) != 0:
        debug(dagList[1].order.pop())

    print(dagList[0])

    b = time()
    scheduler()
    e = time()
    spent = (e - b)
    print("spent: %d" % calcTime(spent))
    if (len(args)) > 1:
        printer_func(args[2])
    else:
        printer_func()


if __name__ == '__main__':
    run()