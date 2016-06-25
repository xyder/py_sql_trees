from sql_tree_implementations.generic_tree import GenericTree


class NestedSetsTree(GenericTree):
    def delete_node(self, node_id, connection=None):
        pass

    def move_node(self, node_id, new_parent_id, connection=None):
        pass

    def is_root(self, node_id, connnection=None):
        pass

    def get_path(self, node_id, connection=None):
        pass

    def get_descendants(self, node_id, connection=None):
        pass

    def add_node(self, title='', parent=None, by_title=False):
        pass

    def get_roots(self, connection=None):
        pass
