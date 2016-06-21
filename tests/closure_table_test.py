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
        self.c_tree.add_node('B', 'A', True)
        self.c_tree.add_node('C', 'A', True)
        self.c_tree.add_node('D', 'B', True)
        self.c_tree.add_node('E', 'B', True)
        self.c_tree.add_node('F', 'A', True)
        self.c_tree.add_node('G', 'F', True)
        self.c_tree.add_node('X')
        self.c_tree.add_node('Y', 'X', True)

    def test_print(self):
        """
        Just prints stuff.
        """

        self.c_tree.print_tables()
        print('Path for D:')
        self.c_tree.print_path(self.c_tree.get_first_id('Y'))
