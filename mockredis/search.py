from redis import ResponseError

class Search(object):
    def __init__(self, index_name="idx", search_indexes=None):
        self.index_name = index_name
        self.search_indexes = search_indexes if search_indexes is not None else {}

    def info(self):
        if self.index_name not in self.search_indexes:
            raise ResponseError()
        return self.search_indexes[self.index_name]

    def create_index(self, fields=None, **kwargs):
        if self.index_name in self.search_indexes:
            raise ResponseError()
        
        self.search_indexes[self.index_name] = True
