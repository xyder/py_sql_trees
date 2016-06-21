from sqlalchemy import (create_engine, MetaData, Table, Column, Integer, Text, PrimaryKeyConstraint, ForeignKey,
                        select, union_all, bindparam, desc)


class ClosureTree:
    """
    Class to create a structure that can store trees using closure tables.
    """

    def __init__(self):
        """
        Instance initialization.
        """

        self.engine = create_engine('sqlite:///:memory:')
        self.metadata = MetaData()

        # add table objects
        self.nodes = Table(
            'nodes', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('title', Text, nullable=True)
        )

        self.paths = Table(
            'paths', self.metadata,
            Column('ancestor', Integer, ForeignKey('nodes.id'), nullable=False),
            Column('descendant', Integer, ForeignKey('nodes.id'), nullable=False),
            Column('depth', Integer, nullable=False),
            PrimaryKeyConstraint('ancestor', 'descendant', name='ad_pk')
        )

        # create tables
        self.metadata.create_all(self.engine)

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

    def node_exists(self, node_id, connection=None):
        """
        Checks if a node exists in the Nodes table.
        :param node_id: the id of the node
        :param connection: a database connection
        :return: True if the node exists, False otherwise
        """

        connection = connection or self.engine.connect()

        return self.get_node(node_id, connection) is not None

    def add_node(self, title='', parent=None, by_title=False):
        """
        Add a new child element to a parent.
        :param title: the title of the child element.
        :param parent: the parent of the child element.
        :param by_title: if True, it will use the first id found for the parent title
        """

        sel_stmt = []
        conn = self.engine.connect()

        # cover cases where id is sent as int, str or row object
        parent_id = parent
        try:
            parent_id = parent_id.id
        except AttributeError:
            pass

        if by_title:
            parent = self.get_first_id(parent)
            if not parent:
                raise Exception('Parent node does not exist.')

        if parent_id is not None:
            # check parent exists

            if not self.node_exists(parent_id):
                raise Exception('Parent node does not exist.')

            # store new node
            new_node_pk = conn.execute(self.nodes.insert(), {'title': title}).inserted_primary_key[0]

            # add new paths for all the ancestors of the parent node
            sel_stmt.append(
                select(
                    [self.paths.c.ancestor, bindparam('d1', new_node_pk), self.paths.c.depth + 1]
                ).where(
                    self.paths.c.descendant == parent_id
                )
            )
        else:
            # add new node
            new_node_pk = conn.execute(self.nodes.insert(), {'title': title}).inserted_primary_key[0]

        # add path to self
        sel_stmt.append(
            select(
                [bindparam('a2', new_node_pk), bindparam('d2', new_node_pk), bindparam('l2', 0)]
            )
        )

        # add paths
        conn.execute(self.paths.insert().from_select(['ancestor', 'descendant', 'depth'], union_all(*sel_stmt)))

    def get_roots(self, connection=None):
        """
        Get the root nodes.
        :param connection: a database connection
        :return: a ResultProxy instance with the root nodes
        """

        connection = connection or self.engine.connect()

        return connection.execute(
            select(
                [self.nodes.c.title, self.nodes.c.id.label('descendant')]
            ).where(
                self.nodes.c.id.notin_(
                    select([self.paths.c.descendant]).where(self.paths.c.depth > 0)
                )
            )
        )

    def get_descendants(self, node_id, connection=None):
        """
        Get the descendants of the given node.
        :param node_id: the id of the node
        :param connection: a database connection
        :return: a ResultProxy instance with the descendants of the node with id = node_id
        """

        connection = connection or self.engine.connect()

        return connection.execute(
            select(
                [self.paths, self.nodes.c.title]
            ).select_from(
                self.paths.join(self.nodes, self.nodes.c.id == self.paths.c.descendant)
            ).where(
                self.paths.c.ancestor == node_id
            ).where(
                self.paths.c.depth == '1'
            )
        )

    def get_ancestors(self, node_id, connection=None):
        """
        Retrieves the ancestors in descending order of depth (useful for building node location (path from root).
        :param node_id: the id of the node
        :param connection: a database connection
        :return: a ResultProxy containing the ancestors of the given node
        """

        connection = connection or self.engine.connect()

        return connection.execute(
            select(
                [self.paths, self.nodes.c.title]
            ).select_from(
                self.paths.join(self.nodes, self.nodes.c.id == self.paths.c.ancestor)
            ).where(
                self.paths.c.descendant == node_id
            ).order_by(
                desc(self.paths.c.depth)
            )
        )

    def print_path(self, node_id, connection=None):
        """
        Prints the ancestors of the node descending depth order.
        :param node_id: the id of the node.
        :param connection: a database connection
        """

        connection = connection or self.engine.connect()

        ancestors = self.get_ancestors(node_id, connection)

        print(' -> '.join(x.title for x in ancestors))

    def print_table(self, table, connection=None):
        """
        Print a table from the database.
        :param table: a SQLA Table
        :param connection: a database connection
        """

        connection = connection or self.engine.connect()
        result = connection.execute(select([table]))
        print(
            '-----------------------------------------------------------'
            '\nColumns:\n\t{}\nData:\n\t{}\n'
            '-----------------------------------------------------------'.format(
                table.columns, '\n\t'.join(str(row) for row in result)
            )
        )

        result.close()

    def view_tree(self, node=None, prefix=' ', connection=None):
        """
        Print a tree in a more visual style.
        :param node: the starting node of the tree.
        :param prefix: string that will be appeneded to the node and all its children
        :param connection: a database connection
        """

        connection = connection or self.engine.connect()

        if not node:
            # get roots
            roots = self.get_roots(connection)

            if not roots:
                print('No root nodes found.')
                return

            for node in roots:
                # print tree for each root
                self.view_tree(node, connection=connection)
                print()

            return
        else:
            node_title = node.title
            node_id = node.descendant

        # print the current node
        print('{}({}, {})'.format(prefix, node_id, node_title))

        # fetch the children for the current node
        children = self.get_descendants(node_id, connection)

        # print the tree for each node
        prefix += '.       '
        for child in children:
            self.view_tree(child, prefix=prefix, connection=connection)

    def print_tables(self):
        """
        Prints all tables and the visual tree representation.
        """

        conn = self.engine.connect()
        self.print_table(self.nodes, conn)
        self.print_table(self.paths, conn)
        self.view_tree(connection=conn)
