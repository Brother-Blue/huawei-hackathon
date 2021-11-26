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
        CLOCK += int((te-ts) * 1000000)
        print('func:%r args:[%r, %r] took: %d µs' % \
          (f.__name__, args, kw, int((te-ts) * 1000000)))
        return result
    return wrap

@timing
def takeTime():
    for i in range(0, 100000000):
        pass


takeTime()
debug(CLOCK)

takeTime()
debug(CLOCK)
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