import asyncio
import datetime
import logging
from collections import defaultdict
from typing import Dict, Union, Tuple, Iterable

from aioch import Client
from clickhouse_driver import Client as BlockingClient
from asyncio import Queue

from prometheus_client import Counter
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type
import socket

logger = logging.getLogger(__name__)


_FLUSH_TASK_EXCEPTION_COUNTER = Counter(
    'ch_flush_task_exceptions', 'Number of exceptions', ['_type']
)

_FLUSH_TASK_INTERVAL = 60 * 10


class ClickhouseClient:
    def __init__(
        self,
        url,
        pool_size=4,
        send_receive_timeout=400,
        connect_timeout=200,
        sync_request_timout=50,
        **kwargs,
    ):
        self._url = url
        self._kwargs = kwargs
        self._pool_size = pool_size
        self._send_receive_timeout = send_receive_timeout
        self._connect_timeout = connect_timeout
        self._sync_request_timout = sync_request_timout

        self._clients: Queue[Client] = Queue(maxsize=self._pool_size)
        self._initialized = False

    async def init(self):
        for _ in range(self._pool_size):
            _client = self._create_client(
                self._url,
                self._send_receive_timeout,
                self._connect_timeout,
                self._sync_request_timout,
                **self._kwargs,
            )
            await self._clients.put(_client)
        self._initialized = True

    def _create_client(
        self, url, send_receive_timeout=400, connect_timeout=200, sync_request_timout=50, **kwargs
    ):
        _client = BlockingClient.from_url(url)
        timeouts = {
            'connect_timeout': connect_timeout,
            'send_receive_timeout': send_receive_timeout,
            'sync_request_timeout': sync_request_timout,
        }
        kwargs.update(timeouts)
        client = Client(_client=_client, loop=None, executor=None, **kwargs)
        return client

    async def __aenter__(self):
        if not self._initialized:
            await self.init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    async def disconnect(self):
        for _ in range(self._pool_size):
            client = await self._clients.get()
            await client.disconnect()

    @retry(
        wait=wait_fixed(3),
        reraise=True,
        stop=stop_after_attempt(10),
        retry=(retry_if_exception_type(socket.error) | retry_if_exception_type(EOFError)),
    )
    async def execute(
        self, query, values=None, as_dict=False, **kwargs
    ) -> Iterable[Union[Tuple, Dict]]:
        if not self._initialized:
            raise RuntimeError("sending execute before init")
        _client = await self._clients.get()
        try:
            if values:
                result = await _client.execute(query, values, with_column_types=as_dict, **kwargs)
            else:
                result = await _client.execute(query, with_column_types=as_dict, **kwargs)
        finally:
            await self._clients.put(_client)

        if as_dict and result:
            return self._process_with_columns_result(result)
        else:
            return result

    def _process_with_columns_result(self, result) -> Iterable[Dict]:
        rows, column_types = result
        columns = [col_type[0] for col_type in column_types]
        dict_rows = (dict(zip(columns, row)) for row in rows)
        return dict_rows

    async def executemany(self, query, values=None, as_dict=False) -> Iterable[Union[Tuple, Dict]]:
        return await self.execute(query, values, as_dict)

    @staticmethod
    def clickhouse_export_query(select_query, target_bucket, target_path):

        export_query = f"""INSERT INTO FUNCTION
                               s3(
                                   'https://{target_bucket}.s3.amazonaws.com/{target_path}',
                                   'Parquet'
                                ) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_compression_method='snappy' 
                        {select_query}
                    """
        return export_query


class ClickhouseBulkInsertClient:
    def __init__(
        self,
        client: ClickhouseClient,
        insert_batch_size=100_000_000,
        timeout=60,
        flush_task_interval=_FLUSH_TASK_INTERVAL,
    ):
        self._ch_client = client
        self._insert_batch_size = insert_batch_size
        self._values = defaultdict(list)
        self._last_flush = defaultdict(datetime.datetime.now)
        self._timeout = timeout
        self._lock = asyncio.Lock()
        self._flush_task_interval = flush_task_interval

        loop = asyncio.get_event_loop()
        self._flush_task = loop.create_task(self._create_flush_task())
        self._flush_task_ready = True

    def __del__(self):
        self._flush_task_ready = False
        try:
            logger.info("stopping clickhouse flushing task")
            self._flush_task.cancel()
        except Exception as e:
            _FLUSH_TASK_EXCEPTION_COUNTER.labels(_type=e.__class__.__name__.lower()).inc(1)
            logger.warning("skipping clickhouse flushing task cancellation: %s", e, exc_info=True)

    async def _create_flush_task(self):
        logger.info("stating clickhouse flushing task")
        while self._flush_task_ready:
            await asyncio.sleep(self._flush_task_interval)
            await self._async_flush()

    async def _async_flush(self):
        query = None
        try:
            async with self._lock:
                to_flush = {
                    query: values
                    for query, values in self._values.items()
                    if self._should_flush(query)
                }
                for query, values in to_flush.items():
                    if self._should_flush(query):
                        await self.flush_now(query)
        except Exception as e:
            _FLUSH_TASK_EXCEPTION_COUNTER.labels(_type=e.__class__.__name__.lower()).inc(1)
            logger.warning(
                "clickhouse flushing task failed for query: %s. Error: %s", query, e, exc_info=True
            )

    def _should_flush(self, query):
        reached_time = (
            self._last_flush[query] + datetime.timedelta(seconds=self._timeout)
            < datetime.datetime.now()
        )
        if (
            len(self._values[query]) > 0 and len(self._values[query]) >= self._insert_batch_size
        ) or reached_time:
            return True
        return False

    async def _flush(self, query, values):
        await self._ch_client.executemany(query, values)
        logger.debug("flushed %s records to clickhouse", len(values))

    async def insertmany(self, query, values):
        self._values[query].extend(values)
        async with self._lock:
            if self._should_flush(query):
                values = self._values.pop(query)
                await self._flush(query, values)


    async def flush_now(self, query):
        if query in self._values:
            values = self._values.pop(query)
            await self._flush(query, values)
            self._last_flush[query] = datetime.datetime.now()


