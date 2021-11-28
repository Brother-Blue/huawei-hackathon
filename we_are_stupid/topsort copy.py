# Python program to print topological sorting of a DAG
from collections import defaultdict
# Class to represent a graph

class Task:
    pass


class Graph:
    def __init__(self, vertices, startRef):
        self.graph = defaultdict(list)  # dictionary containing adjacency List
        self.V = vertices  # No. of vertices
        self.startRef = startRef
    # function to add an edge to graph
    def addEdge(self, u, v):
        v1 = int(str(v)[-3:])
        u1 = int(str(u)[-3:])
        self.graph[u1].append(v1)
    # A recursive function used by topologicalSort

    def topologicalSortUtil(self, v, visited, stack):
        # Mark the current node as visited.
        visited[v] = True
        # Recur for all the vertices adjacent to this vertex
        for i in self.graph[v]: # no touchy
            if visited[i] == False:
                self.topologicalSortUtil(i, visited, stack)
        # Push current vertex to stack which stores result
        stack.append(v)
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
        print(stack)
        # Print contents of the stack
        print(stack[::-1])  # return list in reverse order

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

# # Driver Code

g = Graph(4, 1000)
g.addEdge(1001, 1003)
g.addEdge(1000, 1001)
g.addEdge(1002, 1003)
g.addEdge(1000, 1002)
print(g)

print("Following is a Topological Sort of the given graph")

# Function Call
g.topologicalSort()
# This code is contributed by Neelam Yadav