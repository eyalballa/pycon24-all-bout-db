from contextlib import asynccontextmanager

import asyncpg
import asyncio
import uuid
from datetime import datetime


class AsyncBatchWriter:
    def __init__(self, dsn, batch_size=1000, flush_interval=30):
        self.dsn = dsn
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.active_buffer = []
        self.inactive_buffer = []
        self.flushing = False
        self.last_flush_time = datetime.utcnow()
        self.lock = asyncio.Lock()
        self.conn = None

    async def connect(self):
        self.conn = await asyncpg.connect(self.dsn)

    async def disconnect(self):
        await self.flush_buffer()
        await self.conn.close()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    async def add_to_buffer(self, cid, attribute_name, attribute_value, last_seen):
        self.active_buffer.append((cid, attribute_name, attribute_value, last_seen))
        if (
                len(self.active_buffer) >= self.batch_size or
                (datetime.utcnow() - self.last_flush_time).total_seconds() >= self.flush_interval
        ):
            await self.swap_and_flush_buffers()

    async def swap_and_flush_buffers(self):
        if self.flushing:
            return

            # Acquire the lock and do a double-check
        async with self.lock:
            if self.flushing:  # Double-check after acquiring the lock
                return
            self.flushing = True

        self.active_buffer, self.inactive_buffer = self.inactive_buffer, self.active_buffer
        await self.flush_buffer()
        self.flushing = False

    async def flush_buffer(self):
        if not self.inactive_buffer:
            return
        await self.conn.executemany(
            """
            INSERT INTO attributes (cid, attribute_name, attribute_value, last_seen)
            VALUES ($1, $2, $3, $4)
            """,
            self.inactive_buffer,
        )
        self.inactive_buffer.clear()


class AsyncAggregationReader:
    def __init__(self, dsn):
        self.dsn = dsn
        self.conn = None

    async def connect(self):
        self.conn = await asyncpg.connect(self.dsn)

    async def disconnect(self):
        if self.conn:
            await self.conn.close()

    async def fetch_latest_attributes(self, cid):
        query = """
                SELECT 
                    attribute_name, 
                    attribute_value, 
                    last_seen
                FROM (
                    SELECT 
                        attribute_name, 
                        attribute_value, 
                        last_seen, 
                        ROW_NUMBER() OVER (PARTITION BY attribute_name ORDER BY last_seen DESC) as row_num
                    FROM attributes_hourly
                    WHERE cid = $1 AND last_seen >= NOW() - INTERVAL '30 days'
                ) AS ranked
                WHERE row_num = 1
                ORDER BY attribute_name;
                """
        return await self.conn.fetch(query, cid)


@asynccontextmanager
async def async_aggregation_reader(dsn):
    reader = AsyncAggregationReader(dsn)
    await reader.connect()
    try:
        yield reader
    finally:
        await reader.disconnect()


async def main_with_context(dsn):

    async with AsyncBatchWriter(dsn, batch_size=1000, flush_interval=30) as writer:
        # Adding records
        await run_workload_of_service(writer)

    # The last batch will be flushed and the connection closed automatically upon exiting the context


async def main_no_context(dsn):
    writer = AsyncBatchWriter(dsn, batch_size=1000, flush_interval=30)

    await writer.connect()

    await run_workload_of_service(writer)

    # No need for explicit flush before disconnect, as disconnect handles it
    await writer.disconnect()

async def run_workload_of_service(writer):
    for i in range(10000):
        await writer.add_to_buffer(
            str(uuid.uuid4()), "attribute_name", "attribute_value", datetime.utcnow()
        )

