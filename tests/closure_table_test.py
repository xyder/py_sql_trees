import unittest

from sql_tree_implementations import ClosureTree


class ClosureTest(unittest.TestCase):

    def setUp(self):
        """
        Create this structure:
        tree 1:
                  A
              /   |  \
             B    C   F
           /  \       |
          D   E       G

        tree 2:
          X
        """

        self.c_tree = ClosureTree()
        self.c_tree.add_node('A')
        self.c_tree.add_node('B', 1)
        self.c_tree.add_node('C', 1)
        self.c_tree.add_node('D', 2)
        self.c_tree.add_node('E', 2)
        self.c_tree.add_node('F', 1)
        self.c_tree.add_node('G', 6)
        self.c_tree.add_node('X')

    def test_print(self):
        """
        Just prints all the tables.
        """

        self.c_tree.print_tables()
