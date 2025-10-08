from redis.commands.search.query import Query
from nose.tools import eq_, ok_

from mockredis.tests.fixtures import raises_response_error, setup, teardown


class TestSearch(object):
    '''search tests'''

    def setup(self):
        setup(self)

    def teardown(self):
        teardown(self)

    def test_create_index(self):
        '''Test creating a search index'''
        search = self.redis.ft("test_index")
        search.create_index()

        info = search.info()
        ok_(info is not None)

    @raises_response_error
    def test_create_duplicate_index_raises_error(self):
        '''Test that creating a duplicate index raises an error'''
        search = self.redis.ft("test_index")
        search.create_index()
        search.create_index()

    def test_add_and_search_documents(self):
        '''Test adding documents and searching'''
        search = self.redis.ft("certs")
        search.create_index()

        # Add test documents
        search.add_document("cert:1", {
            "not_after": 1700000000,
            "tls_mode": "enabled"
        })
        search.add_document("cert:2", {
            "not_after": 1800000000,
            "tls_mode": "disabled"
        })
        search.add_document("cert:3", {
            "not_after": 1900000000,
            "tls_mode": "enabled"
        })

        # Search for all documents
        q = Query("*")
        result = search.search(q)

        eq_(3, result.total)
        eq_(3, len(result.docs))

    def test_search_with_range_query(self):
        '''Test range queries like @field:[min max]'''
        search = self.redis.ft("certs")
        search.create_index()

        search.add_document("cert:1", {"not_after": 1700000000})
        search.add_document("cert:2", {"not_after": 1800000000})
        search.add_document("cert:3", {"not_after": 1900000000})

        # Search for certs expiring before 1750000000
        q = Query("@not_after:[0 1750000000]")
        result = search.search(q)

        eq_(1, result.total)
        eq_("cert:1", result.docs[0].id)

    def test_search_with_tag_query(self):
        '''Test tag queries like @field:{value}'''
        search = self.redis.ft("certs")
        search.create_index()

        search.add_document("cert:1", {"tls_mode": "enabled"})
        search.add_document("cert:2", {"tls_mode": "disabled"})
        search.add_document("cert:3", {"tls_mode": "enabled"})

        # Search for enabled certs
        q = Query("@tls_mode:{enabled}")
        result = search.search(q)

        eq_(2, result.total)
        for doc in result.docs:
            eq_("enabled", doc.tls_mode)

    def test_search_with_combined_filters(self):
        '''Test combining multiple filters'''
        search = self.redis.ft("certs")
        search.create_index()

        search.add_document("cert:1", {
            "not_after": 1700000000,
            "tls_mode": "enabled"
        })
        search.add_document("cert:2", {
            "not_after": 1800000000,
            "tls_mode": "disabled"
        })
        search.add_document("cert:3", {
            "not_after": 1750000000,
            "tls_mode": "enabled"
        })

        # Search for enabled certs expiring before 1760000000
        q = Query("@not_after:[0 1760000000] @tls_mode:{enabled}")
        result = search.search(q)

        eq_(2, result.total)

    def test_search_with_sorting(self):
        '''Test sorting search results'''
        search = self.redis.ft("certs")
        search.create_index()

        search.add_document("cert:1", {"not_after": 1800000000})
        search.add_document("cert:2", {"not_after": 1700000000})
        search.add_document("cert:3", {"not_after": 1900000000})

        # Sort by not_after ascending
        q = Query("*").sort_by("not_after", asc=True)
        result = search.search(q)

        eq_("cert:2", result.docs[0].id)
        eq_("cert:1", result.docs[1].id)
        eq_("cert:3", result.docs[2].id)

        # Sort by not_after descending
        q = Query("*").sort_by("not_after", asc=False)
        result = search.search(q)

        eq_("cert:3", result.docs[0].id)
        eq_("cert:1", result.docs[1].id)
        eq_("cert:2", result.docs[2].id)

    def test_search_with_pagination(self):
        '''Test pagination of search results'''
        search = self.redis.ft("certs")
        search.create_index()

        # Add 10 documents
        for i in range(10):
            search.add_document("cert:{0}".format(i), {"not_after": 1700000000 + i})

        # Get first page (3 items)
        q = Query("*").sort_by("not_after", asc=True).paging(0, 3)
        result = search.search(q)

        eq_(10, result.total)
        eq_(3, len(result.docs))
        eq_("cert:0", result.docs[0].id)

        # Get second page
        q = Query("*").sort_by("not_after", asc=True).paging(3, 3)
        result = search.search(q)

        eq_(10, result.total)
        eq_(3, len(result.docs))
        eq_("cert:3", result.docs[0].id)

    def test_document_attribute_access(self):
        '''Test that document fields can be accessed as attributes'''
        search = self.redis.ft("certs")
        search.create_index()

        search.add_document("cert:1", {
            "not_after": 1700000000,
            "tls_mode": "enabled",
            "domain": "example.com"
        })

        q = Query("*")
        result = search.search(q)

        doc = result.docs[0]
        eq_("cert:1", doc.id)
        eq_(1700000000, doc.not_after)
        eq_("enabled", doc.tls_mode)
        eq_("example.com", doc.domain)
    
