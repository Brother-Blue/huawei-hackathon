"""Remove the Helper Items"""
from debug import debug
"""End"""

from functools import wraps
from time import time

CLOCK = 0


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        global CLOCK
        ts = time()
        result = f(*args, **kw)
        te = time()
        CLOCK += int((te - ts) * 1000000)
        print('func:%r args:[%r, %r] took: %d µs' % \
          (f.__name__, args, kw, int((te-ts) * 1000000)))
        return result

    return wrap


def demoFunc():
    print("hello from demoFunc")

class Node:
    def __init__(self, val):
        self.data = val
        self.next = []


# P has LL has nodes[T]
# T = Node.next
"""
class ProcessorSchedule:
    numberOfTasks: int = 0  # place number of tasks scheduled for this processor here
    taskIDs: list = field(default_factory=list)
    startTime: list = field(
        default_factory=list
    )  # place starting times of scheduled tasks, so startTime[i] is start time of task with ID taskIDs[i] (all should be integers) (0 indexed)
    exeTime: list = field(
        default_factory=list
    )  # place actual execution times of tasks, so exeTime[i] is execution time of i-th task scheduled on this processor (0 indexed)


P0 has LL0
P1 has LL1
P2 has LL2
"""
class LinkedList():
    def __init__(self):
        self.head = Node(None)
        self.head.next = None

    def betterFunc(self):
        print("hello from LinkedList")

    def addNode(self, node):
        pass

a = LinkedList()

# https://www.geeksforgeeks.org/multilevel-linked-list/ 

"""
queue of dag -> processors b-tree -> stuff
directed acyclic graph

list tuples dict

def foo():
    print('bar')

dispatcher = {'foo': foo, 'bar': bar} <-- single funcs
dispatcher = {'foobar': [foo, bar], 'bazcat': [baz, cat]}

def fire_all(func_list):
    for f in func_list:
        f()

fire_all(dispatcher['foobar'])

Node
    val
    next []

List
    nodes []
    add()
    ...

n0 = Node(T0)
n1 = Node(T1)
n2 = Node(T2)
n3 = Node(T3)

n0.next.append(m1)
n1.next.append(n2)
n1.next.append(n3)
n2.next.append(n3)

Tree0 = DAG0
        /  \
      T0
     /
    T1 <--- cant run until T0 done
   / \
  |  T2 <--- cant run until T1 done
 | /
 T3 <--- cant run until T1 and T2 done
"""


""" comments
Have one master thread push items to a queue once they are ready for being processsed. 
Then have a pool of workers listen on the queue for tasks to work on. 
(Python provides a synchronized queue in the Queue module, renamed to lower-case queue in Python 3).
The master first creates a map from dependencies to dependent tasks. 
Every task that doesn't have any dependcies can go into the queue. 
Everytime a task is completed, the master uses the dictionary 
to figure out which dependent tasks there are, and puts them into the queue 
if all their depndencies are met now.


I suggest you take a look at multiprocessing.Pool() because I believe it exactly solves your problem. 
It runs N "worker processes" and as each worker finishes a task, another task is provided. 
And there is no need for "poison pills"; it is very simple.

I have always used the .map() method on the pool.
Python multiprocessing.Pool: when to use apply, apply_async or map? - https://stackoverflow.com/questions/8533318/multiprocessing-pool-when-to-use-apply-apply-async-or-map
EDIT: Here is an answer I wrote to another question, and I used multiprocessing.Pool() in my answer.
Parallel file matching, Python - https://stackoverflow.com/questions/7623211/parallel-file-matching-python/7624949#7624949
"""

""" links
https://github.com/xianghuzhao/paradag

"""

# @timing
# def takeTime():
#     for i in range(0, 100000000):
#         pass


# takeTime()
# debug(CLOCK)

# takeTime()
# debug(CLOCK)


class Node:
    def __init__(self, data=None):
        self.data = data
        self.red = False
        self.parent = None
        self.left = None
        self.right = None

class Tree:
    def __init__(self):
        self.nil = Node(0)
        self.nil.red = False
        self.nil.left = None
        self.nil.right = None
        self.root = self.nil

        def insert(self, key):
            node = Node(key)
            node.val = key
            node.left = self.nil
            node.right = self.nil
            node.red = True

            temp = self.root
            curNode = None

            while temp != self.nil:
                curNode = temp

                if node.val < temp.val:
                    temp = temp.left
                else:
                    temp = temp.right

            node.parent = curNode
            if curNode == None:
                self.root = node
            elif node.val < curNode.val:
                curNode.left = node
            else:
                curNode.right = node

            if node.parent == None:
                node.red = False
                return

            if node.parent.parent == None:
                return

            self.fixInsert(node)

        def minimum(self, node):
            while node.left != self.nil:
                node = node.left
            return node

        def rorate_left(self, node):
            temp = node.right
            node.right = temp.left
            if temp.left != self.nil:
                temp.left.parent = node

            temp.parent = node.parent
            if node.parent == None:
                self.root = temp
            elif node == node.parent.left:
                node.parent.left = temp
            else:
                node.parent.right = temp
            temp.left = node
            node.parent = temp

        def rotate_right(self, node):
            pass