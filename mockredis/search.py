import re
from redis import ResponseError
from redis.commands.search.document import Document

# taken from redis-py
def to_string(s):
    if isinstance(s, str):
        return s
    elif isinstance(s, bytes):
        return s.decode("utf-8", "ignore")
    else:
        return s  # Not a string we care about

# simplified version, we don't need the real implementation since we control Results with search()
class Result(object):
    """Mock Result object containing search results"""
    def __init__(self, docs, total):
        self.docs = docs
        self.total = total

    def __repr__(self):
        return f"Result(total={self.total}, docs={len(self.docs)})"

class Search(object):
    """Mock RediSearch client"""

    def __init__(self, index_name="idx", search_indexes=None):
        self.index_name = index_name
        self.search_indexes = search_indexes if search_indexes is not None else {}

    def info(self):
        """Return index info"""
        if self.index_name not in self.search_indexes:
            raise ResponseError()
        index_data = self.search_indexes[self.index_name]
        return index_data.get("info", {})

    def create_index(self, fields=None, **kwargs):
        """Create a search index"""
        if self.index_name in self.search_indexes:
            raise ResponseError()

        self.search_indexes[self.index_name] = {
            "docs": {},
            "fields": fields or [],
            "info": {}
        }

    def add_document(self, doc_id, fields, **kwargs):
        """
        :param doc_id: Document ID
        :param fields: Dictionary of field names to values
        """
        if self.index_name not in self.search_indexes:
            raise ResponseError()

        index_data = self.search_indexes[self.index_name]
        index_data["docs"][doc_id] = fields

    def search(self, query):
        """
        Execute a search query against the index.

        Supports:
        - Range queries: @field:[min max]
        - Tag queries: @field:{value}
        - Wildcard: *
        - Sorting via query.sort_by()
        - Pagination via query.paging()

        :param query: Query object
        :return: Result object with matching documents
        """
        if self.index_name not in self.search_indexes:
            raise ResponseError()

        index_data = self.search_indexes[self.index_name]
        docs = index_data.get("docs", {})

        filtered_docs = []
        for doc_id, doc_data in docs.items():
            if self._matches_query(doc_data, query._query_string):
                filtered_docs.append((doc_id, doc_data))

        if query._sortby:
            sort_field = query._sortby.args[0]
            sort_asc = query._sortby.args[1] == "ASC"
            filtered_docs = self._sort_documents(filtered_docs, sort_field, sort_asc)

        total = len(filtered_docs)

        start = query._offset
        end = start + query._num
        paginated_docs = filtered_docs[start:end]

        # Convert to Document objects
        result_docs = [Document(doc_id, **doc_data) for doc_id, doc_data in paginated_docs]

        return Result(docs=result_docs, total=total)

    def _matches_query(self, doc_data, query_string):
        """
        Check if a document matches the query string.

        Supports basic query patterns:
        - * (wildcard - matches all)
        - @field:[min max] (range query)
        - @field:{value} (tag query)
        - Multiple conditions (space-separated, treated as AND)
        """
        query_string = query_string.strip()

        # Wildcard matches everything
        if query_string == "*":
            return True

        conditions = self._parse_query_conditions(query_string)

        for condition in conditions:
            if not self._matches_condition(doc_data, condition):
                return False

        return True

    def _parse_query_conditions(self, query_string):
        """Parse query string into individual conditions"""
        conditions = []

        # Match range queries: @field:[min max]
        range_pattern = r'@(\w+):\[([^\]]+)\]'
        for match in re.finditer(range_pattern, query_string):
            field = match.group(1)
            range_val = match.group(2)
            conditions.append({"type": "range", "field": field, "value": range_val})

        # Match tag queries: @field:{value}
        tag_pattern = r'@(\w+):\{([^\}]+)\}'
        for match in re.finditer(tag_pattern, query_string):
            field = match.group(1)
            tag_val = match.group(2)
            conditions.append({"type": "tag", "field": field, "value": tag_val})

        return conditions

    def _matches_condition(self, doc_data, condition):
        """Check if a document matches a single condition"""
        field = condition["field"]

        if field not in doc_data:
            return False

        doc_value = doc_data[field]

        if condition["type"] == "range":
            # Parse range: "min max"
            parts = condition["value"].split()
            if len(parts) != 2:
                return False

            try:
                min_val = float(parts[0])
                max_val = float(parts[1])
                doc_val_float = float(doc_value)
                return min_val <= doc_val_float <= max_val
            except (ValueError, TypeError):
                return False

        elif condition["type"] == "tag":
            # Exact match for tag
            return str(doc_value) == condition["value"]

        return False

    def _sort_documents(self, docs, sort_field, ascending=True):
        """Sort documents by a field"""

        def get_sort_key(doc_tuple):
            doc_id, doc_data = doc_tuple
            value = doc_data.get(sort_field)

            if value is None:
                return float('inf') if ascending else float('-inf')

            # Try to convert to number for numeric sorting
            try:
                return float(value)
            except (ValueError, TypeError):
                return str(value)

        return sorted(docs, key=get_sort_key, reverse=not ascending)

