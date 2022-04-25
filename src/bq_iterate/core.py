from logging import info, error, debug

from google.cloud import bigquery

from .retry import default_retry

class BqRowsIterator:
    client = None

    def __init__(self, client, batch_size):
        self.batch_size = batch_size
        self.initialized = False
        self.nth_batch = 0
        if self.__class__.client is None:
            self.__class__.client = client
        
    def __iter__(self):
        return self

    def __next__(self):
        if not self.initialized:
            self.initialize()
            return self.row_iterable
        elif self.row_iterable.next_page_token is None:
            raise StopIteration
        else:
            self.call_next_batch()
            return self.row_iterable

    @default_retry
    def call_next_batch(self):
        page_token = (
            self.row_iterable.next_page_token if hasattr(self, "row_iterable") else None
        )
        debug(
            f"""Calling bq client list_rows with batch size : {self.batch_size}
            batch number :{self.nth_batch} with page_token :{page_token or "None"}"""
        )
        self.row_iterable = self.list_next_rows(page_token)
        self.nth_batch += 1

    @classmethod
    def update_client(cls, client):
      cls.__class__.client = client

    def list_next_rows(self, page_token= None):
        pass

    def initialize(self):
        self.row_iterable = self.list_next_rows()
        self.initialized = True

class BqQueryRowsIterator(BqRowsIterator):
    def __init__(self, client, query, batch_size):
        self.query = query
        super().__init__(client, batch_size)
        self.query_job = self.client.query(query)

    def list_next_rows(self, page_token= None):
        return self.client.list_rows(
            self.query_job.destination,
            max_results=self.batch_size,
            page_token=page_token,
        )

class BqTableRowsIterator(BqRowsIterator):
    def __init__(self, client, table_id, batch_size):
        self.table_id = table_id
        super().__init__(client, batch_size)

    def list_next_rows(self, page_token=None):      
        return self.client.list_rows(
            f"{self.table_id}",
            max_results=self.batch_size,
            page_token=page_token,
        )
  
def bq_iterator(iterator):
    from collections.abc import Iterable, Iterator
    from logging import info

    if isinstance(iterator, Iterable) and not isinstance(iterator, Iterator):
        iterator = iter(iterator)
    elif not isinstance(iterator, Iterator):
        raise Exception(f"{type(iterator)} not iterable")

    while (el := next(iterator, None)) is not None:
        yield el 