import os
import random
from typing import List

import pytest
import tablestore


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

    return tablestore.OTSClient(
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
        return [random.uniform(0.1, 1)] * self.embed_dimension

    def embedding(self, query: str) -> List[float]:
        return self._get_vector()
