import asyncio
import logging
import os
import random
from collections.abc import AsyncGenerator, AsyncIterator
from typing import List

import pytest
import tablestore
from tablestore import AsyncOTSClient, OTSClient


def pytest_addoption(parser):
    parser.addoption(
        "--run_mode",
        action="store",
        default="mixed",
        choices=["sync", "async", "mixed"],
        help="run mode: sync(use sync client), async(use async client) or mixed(randomly choose one from sync and async)",
    )


@pytest.fixture
def run_mode(request):
    return request.config.getoption("--run_mode")


class RandomClientRunner:
    """
    A client runner that randomly chooses between sync and async clients to execute API calls.
    This is designed to maintain backward compatibility with existing test code while allowing
    asynchronous logic to be invoked seamlessly. It prevents mistakes in code development by
    ensuring that both sync and async versions of APIs are tested without requiring changes
    to legacy test implementations.
    """

    RunMode2RandomChoiceList = {"sync": [True], "async": [False], "mixed": [True, False]}

    def __init__(
        self,
        sync_client_class,
        async_client_class,
        sync_client_kwargs: dict = None,
        async_client_kwargs: dict = None,
        run_mode: str = "mixed",
    ):
        self._run_mode = run_mode

        self._sync_client = sync_client_class(**sync_client_kwargs)
        self._async_client = async_client_class(**async_client_kwargs)

        self.logger = None
        self.set_logger()

        self.logger.info(f"RandomOTSClient run mode: {self._run_mode}")

        self.func_names = [
            func_name
            for func_name in dir(sync_client_class)
            if callable(getattr(sync_client_class, func_name)) and not func_name.startswith("__")
        ]

    def __getattr__(self, item):
        if item in self.func_names:
            return self.generate_func_by_name(item)

        # need to use inner client, we use the sync version, because we don't need to test the inner client, just for fitting older code
        if item == "_client":
            if hasattr(self._sync_client, "_client"):
                return self._sync_client._client

        if random.choice(RandomClientRunner.RunMode2RandomChoiceList[self._run_mode]):
            self.logger.info(f"use sync_client for attr {item}")
            return getattr(self._sync_client, item)
        else:
            self.logger.info(f"use async_client for attr {item}")
            return getattr(self._async_client, item)

    def set_logger(self):
        self.logger = logging.getLogger("APITestBase")
        self.logger.setLevel(logging.INFO)

        fh = logging.FileHandler("tablestore_for_agent_memory_test.log")
        fh.setLevel(logging.INFO)

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)

        self.logger.addHandler(fh)

    @staticmethod
    async def _run_async_func(coroutine_object):
        if isinstance(coroutine_object, AsyncGenerator):
            result = [item async for item in coroutine_object]
        else:
            result = await coroutine_object
            if isinstance(result, AsyncIterator):
                result = [item async for item in result]
        return result

    async def _run_async_client_func(self, func_name, *args, **kwargs):
        async_client_func = getattr(self._async_client, func_name)

        # if func is sync
        if not asyncio.iscoroutinefunction(async_client_func):
            return async_client_func(*args, **kwargs)

        # if func is async
        if hasattr(self._async_client, "__aenter__") and hasattr(self._async_client, "__aexit__"):
            async with self._async_client:
                coroutine_object = async_client_func(*args, **kwargs)
                result = await RandomClientRunner._run_async_func(coroutine_object)
        else:
            coroutine_object = async_client_func(*args, **kwargs)
            result = await RandomClientRunner._run_async_func(coroutine_object)

        return result

    def generate_func_by_name(self, func_name):
        def generated_func(*args, **kwargs):
            if random.choice(RandomClientRunner.RunMode2RandomChoiceList[self._run_mode]):
                self.logger.info(f"use sync_client for func {func_name}")
                return getattr(self._sync_client, func_name)(*args, **kwargs)
            else:
                self.logger.info(f"use async_client for func {func_name}")
                return asyncio.run(self._run_async_client_func(func_name, *args, **kwargs))

        return generated_func


@pytest.fixture
def tablestore_client():
    endpoint = os.getenv("tablestore_end_point")
    instance_name = os.getenv("tablestore_instance_name")
    access_key_id = os.getenv("tablestore_access_key_id")
    access_key_secret = os.getenv("tablestore_access_key_secret")
    if endpoint is None or instance_name is None or access_key_id is None or access_key_secret is None:
        pytest.skip(
            "endpoint is None or instance_name is None or " "access_key_id is None or access_key_secret is None"
        )

    return OTSClient(
        endpoint,
        access_key_id,
        access_key_secret,
        instance_name,
        retry_policy=tablestore.WriteRetryPolicy(),
    )


@pytest.fixture
def async_tablestore_client():
    endpoint = os.getenv("tablestore_end_point")
    instance_name = os.getenv("tablestore_instance_name")
    access_key_id = os.getenv("tablestore_access_key_id")
    access_key_secret = os.getenv("tablestore_access_key_secret")
    if endpoint is None or instance_name is None or access_key_id is None or access_key_secret is None:
        pytest.skip(
            "endpoint is None or instance_name is None or " "access_key_id is None or access_key_secret is None"
        )

    return AsyncOTSClient(
        endpoint,
        access_key_id,
        access_key_secret,
        instance_name,
        retry_policy=tablestore.WriteRetryPolicy(),
    )


@pytest.fixture
def embedding_model():
    return MockEmbedding(128)


class MockEmbedding:
    embed_dimension: int

    def __init__(self, embed_dimension: int) -> None:
        """Init params."""
        self.embed_dimension = embed_dimension

    @classmethod
    def class_name(cls) -> str:
        return "MockEmbedding"

    def _get_vector(self) -> List[float]:
        vecs: List[float] = []
        for _ in range(self.embed_dimension):
            vecs.append(random.uniform(0.1, 1))
        return vecs

    def embedding(self, query: str) -> List[float]:
        return self._get_vector()


def test_mock_embedding():
    embedding_model = MockEmbedding(4)
    vector = embedding_model.embedding("abc")
    assert len(vector) == 4
    print(vector)
