"""Remove the Helper Items"""
from debug import debug
"""End"""

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
            node.red = False

            root = self.root