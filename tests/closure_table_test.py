import cProfile
import unittest
from random import randint
from timeit import Timer

from sqlalchemy import select, func

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

        print('=========================')
        print('Move B under C.')
        self.c_tree.move_node(self.c_tree.get_first_id('B'), self.c_tree.get_first_id('C'))
        self.c_tree.view_tree()

        print('Path for D:')
        self.c_tree.print_path(self.c_tree.get_first_id('Y'))

    def _generate_tree(self, tree, node_id=None, max_depth=5, depth=0, branch_size=5):

        """
        Create a random tree recursively.
        :param tree: the tree in which the nodes will be added.
        :param node_id: the root node under which all the nodes will be added
        :param max_depth: the maximum absolute depth that will be reached
        :param depth: the current depth
        :param branch_size: the maximum number of children that can be generated for any node
        """

        if not node_id:
            node_id = tree.add_node('root')

        if depth < max_depth:
            for _ in range(randint(2, branch_size)):
                node_pk = tree.add_node('x', node_id)
                self._generate_tree(tree, node_pk, max_depth, depth=depth + 1)

    def _timed_move(self, tree, node_id, parent_id):
        """
        Executes the move node functionality and returns the execution time.
        :param tree: the tree in which the operation will be performed.
        :param node_id: the id of the node that will be moved
        :param parent_id: the id of the new parent to which the node will be attached
        :return: the execution time
        """

        t = Timer(lambda: tree.move_node(node_id, parent_id))
        return t.timeit(1)

    def test_random_move(self):
        """
        Test for the move node functionality.
        """

        print('=========================')

        # create tree randomly and recursively
        tree = ClosureTree()
        root_id = tree.add_node('root')
        self._generate_tree(tree, root_id)

        print('Stress test for moving nodes. Tree size: {}'.format(tree.node_count()))

        # generate 2 random nodes from the children of the root node
        r = tree.get_descendants(root_id)
        r = [x.descendant for x in r]
        max_index = len(r) - 1

        moving_node_id = r[randint(0, max_index)]
        parent_node_id = r[randint(0, max_index)]

        while parent_node_id == moving_node_id:
            parent_node_id = r[randint(0, max_index)]

        print('Moving {} under {}.'.format(moving_node_id, parent_node_id))

        # move the node to the new parent and time the execution
        t1_1 = self._timed_move(tree, moving_node_id, parent_node_id)

        # move the node back and time the execution
        t1_2 = self._timed_move(tree, moving_node_id, parent_node_id)

        print('Standard: moved node:\n\t-> {}\n\t<- {}'.format(t1_1, t1_2))
