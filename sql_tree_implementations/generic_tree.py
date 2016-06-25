from sqlalchemy import create_engine, MetaData, select, func


class GenericTree:

    def __init__(self):
        """
        Class instance initializer.
        Must define a "nodes" table.
        """

        self.engine = create_engine('sqlite:///:memory:')
        self.metadata = MetaData()

        self.nodes = None

    def node_count(self, connection=None):
        """
        Returns the number of nodes.
        :param connection: a database connection
        :return: the number of stored nodes.
        """

        connection = connection or self.engine.connect()
        return connection.execute(select([func.count()]).select_from(self.nodes)).fetchone()[0]

    def get_node(self, node_id, connection=None):
        """
        Retrieves the node from the Nodes table.
        :param node_id: the id of the node
        :param connection: a database connection
        :return: a row object or None of the node does not exist
        """

        connection = connection or self.engine.connect()

        return connection.execute(
            select(
                [self.nodes]
            ).where(
                self.nodes.c.id == node_id
            )
        ).fetchone()

    def node_exists(self, node_id, connection=None):
        """
        Checks if a node exists in the Nodes table.
        :param node_id: the id of the node
        :param connection: a database connection
        :return: True if the node exists, False otherwise
        """

        connection = connection or self.engine.connect()

        return self.get_node(node_id, connection) is not None

    def get_first_id(self, node_title, connection=None):
        """
        Retrieves the id of the first node found with the given title.
        :param node_title: the title of the node to be searched.
        :param connection: a database connection
        :return: the id of the node if found, or None otherwise
        """

        connection = connection or self.engine.connect()

        node = connection.execute(
            select(
                [self.nodes.c.id]
            ).where(
                self.nodes.c.title == node_title
            )
        ).fetchone()

        return node and node.id

    def add_node(self, title='', parent=None, by_title=False):
        """ Add a node. """
        raise NotImplementedError

    def is_root(self, node_id, connnection=None):
        """ Check if a node is root. """
        raise NotImplementedError

    def delete_node(self, node_id, connection=None):
        """ Delete a node. """
        raise NotImplementedError

    def move_node(self, node_id, new_parent_id, connection=None):
        """ Move a node and its subtree. """
        raise NotImplementedError

    def get_roots(self, connection=None):
        """ Get all nodes that are roots. """
        raise NotImplementedError

    def get_descendants(self, node_id, connection=None):
        """ Get the descendants of a node. """
        raise NotImplementedError

    def get_path(self, node_id, connection=None):
        """ Get a list of ancestors in order for a given node (the path of the node) """
        raise NotImplementedError
