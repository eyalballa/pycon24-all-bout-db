import time

import redis.asyncio as redis

class RedisClient:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis = None
        self.pipe = None
        self.batch_size = 1000
        self.current_batch = 0
        self.last_execute_time = None
        self.timeout_seconds = 30

    # async def set(self, key: str, value: str):
    #     await self.redis.set(key, value)

    async def get(self, key: str):
        return await self.redis.get(key)

    async def init(self):
        self.redis = redis.from_url(self.redis_url)
        self.redis_url = redis_url
        self.redis = None

    async def close(self):
        if self.redis:
            await self.redis.close()

    async def set(self, key: str, value: str):
        self.pipe.set(key, value)
        self.current_batch += 1

        # Check if either the batch size or the time condition is met
        current_time = time.time()
        if (
                self.current_batch >= self.batch_size or
                (current_time - self.last_execute_time) >= self.timeout_seconds
        ):
            await self.pipe.execute()
            self.current_batch = 0
            self.last_execute_time = current_time

    async def add_to_set(self, set_name: str, value: str):
        self.pipe.sadd(set_name, value)
        self.current_batch += 1

        # Check if either the batch size or the time condition is met
        current_time = time.time()
        if (
                self.current_batch >= self.batch_size or
                (current_time - self.last_execute_time) >= self.timeout_seconds
        ):
            await self.pipe.execute()
            self.current_batch = 0
            self.last_execute_time = current_time

    async def get_set_members(self, set_name: str):
        return await self.redis.smembers(set_name)

