from sqlalchemy import (create_engine, MetaData, Table, Column, Integer, Text, PrimaryKeyConstraint, ForeignKey,
                        select, union_all, bindparam)


class ClosureTree:
    def __init__(self):
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

    def add_node(self, title='', parent=None):
        """
        Add a new child element to a parent.
        :param title: the title of the child element.
        :param parent: the parent of the child element
        """

        sel_stmt = []
        conn = self.engine.connect()

        # cover cases where id is sent as int, str or row object
        parent_id = parent
        try:
            parent_id = parent_id.id
        except AttributeError:
            pass

        if parent_id is not None:
            # check parent exists
            parent_node = conn.execute(
                select(
                    [self.nodes]
                ).where(
                    self.nodes.c.id == parent_id)
            ).fetchone()

            if not parent_node:
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

    def print_table(self, table):
        conn = self.engine.connect()
        result = conn.execute(select([table]))
        print(
            '-----------------------------------------------------------'
            '\nColumns:\n\t{}\nData:\n\t{}\n'
            '-----------------------------------------------------------'.format(
                table.columns, '\n\t'.join(str(row) for row in result)
            )
        )

        result.close()

    def view_tree(self, node=None, prefix=' '):
        """
        Print a tree in a more visual style.
        :param node: the starting node of the tree.
        :param prefix: string that will be appeneded to the node and all its children
        """

        conn = self.engine.connect()

        if not node:
            # get roots
            roots = conn.execute(
                select(
                    [self.nodes.c.title, self.nodes.c.id.label('descendant')]
                ).where(
                    self.nodes.c.id.notin_(
                        select([self.paths.c.descendant]).where(self.paths.c.depth > 0)
                    )
                )
            )

            if not roots:
                print('No root nodes found.')
                return

            for node in roots:
                # print tree for each root
                self.view_tree(node)
                print()

            return
        else:
            node_title = node.title
            node_id = node.descendant

        # print the current node
        print('{}({}, {})'.format(prefix, node_id, node_title))

        # fetch the children for the current node
        children = conn.execute(
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

        # print the tree for each node
        prefix += '.       '
        for child in children:
            self.view_tree(child, prefix=prefix)

    def print_tables(self):
        """
        Prints all tables and the visual tree representation.
        """

        self.print_table(self.nodes)
        self.print_table(self.paths)
        self.view_tree()
