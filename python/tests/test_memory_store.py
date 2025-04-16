import copy
import logging
import random

import pytest
import tablestore
from faker import Faker
from pydantic import ValidationError
from tablestore_for_agent_memory.base.common import MetaType, Order

from tablestore_for_agent_memory.base.base_memory_store import Message, Session
from tablestore_for_agent_memory.base.filter import Filters
from tablestore_for_agent_memory.memory.memory_store import MemoryStore
from tablestore_for_agent_memory.util.tablestore_helper import TablestoreHelper

fk_data = Faker(locale="zh_CN")
logger = logging.getLogger(__name__)


@pytest.fixture
def memory_store(tablestore_client: tablestore.OTSClient):

    session_secondary_index_meta = {
        "meta_string": MetaType.STRING,
        "meta_long": MetaType.INTEGER,
        "meta_double": MetaType.DOUBLE,
        "meta_boolean": MetaType.BOOLEAN,
        "meta_bytes": MetaType.BINARY,
    }

    session_search_index_schema = [
        tablestore.FieldSchema(
            "title",
            tablestore.FieldType.TEXT,
            analyzer=tablestore.AnalyzerType.FUZZY,
            analyzer_parameter=tablestore.FuzzyAnalyzerParameter(1, 4),
        ),
        tablestore.FieldSchema("meta_string", tablestore.FieldType.KEYWORD),
        tablestore.FieldSchema("meta_long", tablestore.FieldType.LONG),
        tablestore.FieldSchema("meta_double", tablestore.FieldType.DOUBLE),
        tablestore.FieldSchema("meta_boolean", tablestore.FieldType.BOOLEAN),
    ]

    message_search_index_schema = [
        tablestore.FieldSchema("meta_string", tablestore.FieldType.KEYWORD),
        tablestore.FieldSchema("meta_long", tablestore.FieldType.LONG),
        tablestore.FieldSchema("meta_double", tablestore.FieldType.DOUBLE),
        tablestore.FieldSchema("meta_boolean", tablestore.FieldType.BOOLEAN),
    ]

    memory_store = MemoryStore(
        tablestore_client=tablestore_client,
        session_secondary_index_meta=session_secondary_index_meta,
        session_search_index_schema=session_search_index_schema,
        message_search_index_schema=message_search_index_schema,
    )
    return memory_store


def random_session(user_id=fk_data.user_name()) -> Session:
    session = Session(user_id=user_id, session_id=fk_data.uuid4())
    session.metadata["title"] = random.choice(["abc", "def", "ghi", "abcd", "abcdef", "abcgh"])
    session.metadata["meta_string"] = fk_data.name()
    session.metadata["meta_long"] = random.randint(0, 9999999999999)
    session.metadata["meta_double"] = random.uniform(0.0, 1.0)
    session.metadata["meta_boolean"] = random.choice([True, False])
    session.metadata["meta_bytes"] = bytearray(fk_data.name(), encoding="utf8")
    return session


def random_message(session_id=fk_data.user_name()) -> Message:
    message = Message(session_id=session_id, message_id=fk_data.uuid4())
    message.create_time = random.randint(0, 999999999)
    message.content = " ".join(
        random.choices(
            ["abc", "def", "ghi", "abcd", "adef", "abcgh", "apple", "banana", "cherry"], k=random.randint(1, 10)
        )
    )
    message.metadata["meta_string"] = fk_data.name_male()
    message.metadata["meta_long"] = random.randint(0, 999999999)
    message.metadata["meta_double"] = random.uniform(1.0, 2.0)
    message.metadata["meta_boolean"] = random.choice([True, False])
    message.metadata["meta_bytes"] = bytearray(fk_data.city_name(), encoding="utf8")
    return message


def test_memory_store_init(memory_store):
    assert memory_store is not None
    memory_store._delete_table()
    memory_store.init_table()
    describe_table_response = memory_store._client.describe_table(memory_store._message_table_name)
    if memory_store._message_secondary_index_name in [
        index.index_name for index in describe_table_response.secondary_indexes
    ]:
        assert True
    else:
        assert False
    memory_store._delete_table()


def test_memory_store_session(memory_store):
    assert memory_store is not None
    memory_store._delete_table()
    memory_store.init_table()

    session = Session(user_id="1", session_id="2")
    session.metadata["meta_string"] = "4"
    session.metadata["meta_long"] = 5
    session.metadata["meta_double"] = 6.6
    session.metadata["meta_boolean"] = True
    session.metadata["meta_bytes"] = bytearray("China", encoding="utf8")

    memory_store.put_session(session)
    session_read = memory_store.get_session(user_id="1", session_id="2")
    assert session_read == session

    session_update = copy.deepcopy(session)
    session_update.metadata["meta_string"] = "updated_5"
    memory_store.update_session(session_update)
    session_read_after_update = memory_store.get_session(user_id="1", session_id="2")
    assert session_read_after_update == session_update
    assert session_read_after_update != session
    assert session_read_after_update.metadata["meta_string"] == "updated_5"

    memory_store.delete_session(user_id="1", session_id="2")
    session_read_after_delete = memory_store.get_session(user_id="1", session_id="2")
    assert session_read_after_delete is None

    session_update = copy.deepcopy(session)
    session_update.metadata["meta_string"] = "updated_6"
    memory_store.update_session(session_update)
    session_read_after_delete_and_update = memory_store.get_session(user_id="1", session_id="2")
    assert session_read_after_delete_and_update == session_update
    assert session_read_after_delete_and_update != session
    assert session_read_after_delete_and_update.metadata["meta_string"] == "updated_6"


def test_memory_store_message(memory_store):
    assert memory_store is not None
    memory_store._delete_table()
    memory_store.init_table()

    message = random_message()
    message.content = "123 abc"
    memory_store.put_message(message)
    message_read = memory_store.get_message(session_id=message.session_id, message_id=message.message_id)
    assert message_read == message
    message_read = memory_store.get_message(
        session_id=message.session_id, message_id=message.message_id, create_time=message.create_time
    )
    assert message_read == message
    message_read = memory_store.get_message(
        session_id=message.session_id, message_id=message.message_id, create_time=-1
    )
    assert message_read is None

    message_update = copy.deepcopy(message)
    message_update.content = "456 edf"
    memory_store.update_message(message_update)
    message_read_after_update = memory_store.get_message(session_id=message.session_id, message_id=message.message_id)
    assert message_read_after_update == message_update
    assert message_read_after_update != message
    assert message_read_after_update.content == "456 edf"

    message_update_new = copy.deepcopy(message)
    message_update_new.message_id = "message_update_new"
    message_update_new.content = "789 xyz"
    message_update_new.create_time = None
    memory_store.update_message(message_update_new)
    message_read_after_update_new = memory_store.get_message(
        session_id=message_update_new.session_id, message_id=message_update_new.message_id
    )
    assert message_read_after_update_new == message_update_new
    assert message_read_after_update_new.create_time is not None
    assert message_read_after_update_new.content == "789 xyz"

    memory_store.delete_message(session_id=message.session_id, message_id=message.message_id)
    message_read_after_delete = memory_store.get_message(session_id=message.session_id, message_id=message.message_id)
    assert message_read_after_delete is None

    memory_store.delete_message(
        session_id=message_update_new.session_id,
        message_id=message_update_new.message_id,
        create_time=message_update_new.create_time,
    )
    message_read_after_delete = memory_store.get_message(
        session_id=message_update_new.session_id, message_id=message_update_new.message_id
    )
    assert message_read_after_delete is None


def test_get_sessions(memory_store, tablestore_client):
    assert memory_store is not None
    memory_store._delete_table()
    memory_store.init_table()
    user_id_1_count = 0
    user_id_meta_boolean_true_count = 0
    user_id_meta_double_gt_half_1_count = 0
    user_id_meta_double_gt_half_1_and_meta_bool_true_count = 0
    total_count = random.randint(10, 99)
    update_time_and_session_id = []
    for i in range(total_count):
        user_id = random.choice(["1", "2"])
        session = random_session(user_id=user_id)
        session.update_time = random.randint(1, 100)
        memory_store.put_session(session)
        if user_id == "1":
            update_time_and_session_id.append((session.session_id, session.update_time))
            user_id_1_count += 1
            if session.metadata["meta_boolean"]:
                user_id_meta_boolean_true_count += 1
            if session.metadata["meta_double"] > 0.5:
                user_id_meta_double_gt_half_1_count += 1
            if session.metadata["meta_boolean"] and session.metadata["meta_double"] > 0.5:
                user_id_meta_double_gt_half_1_and_meta_bool_true_count += 1
    all_sessions = list(memory_store.list_all_sessions())
    assert len(all_sessions) == total_count

    sessions = list(memory_store.list_sessions(user_id="1"))
    assert len(sessions) == user_id_1_count

    sessions = list(memory_store.list_sessions(user_id="1", max_count=2))
    assert len(sessions) == min(2, user_id_1_count)

    sessions = list(memory_store.list_sessions(user_id="1", metadata_filter=Filters.eq("meta_boolean", True)))
    assert len(sessions) == user_id_meta_boolean_true_count

    sessions = list(memory_store.list_sessions(user_id="1", metadata_filter=Filters.not_eq("meta_boolean", False)))
    assert len(sessions) == user_id_meta_boolean_true_count

    sessions = list(
        memory_store.list_sessions(
            user_id="1",
            metadata_filter=Filters.logical_and([Filters.gt("meta_double", 0.5), Filters.eq("meta_boolean", True)]),
        )
    )
    assert len(sessions) == user_id_meta_double_gt_half_1_and_meta_bool_true_count

    sessions = list(
        memory_store.list_sessions(
            user_id="1",
            metadata_filter=Filters.logical_not(
                [
                    Filters.logical_and(
                        [
                            Filters.gt("meta_double", 0.5),
                            Filters.eq("meta_boolean", True),
                        ]
                    )
                ]
            ),
        )
    )
    assert len(sessions) == user_id_1_count - user_id_meta_double_gt_half_1_and_meta_bool_true_count

    update_time_and_session_id = sorted(update_time_and_session_id, key=lambda x: x[1], reverse=True)
    sessions = list(memory_store.list_recent_sessions(user_id="1"))
    assert len(sessions) == user_id_1_count
    assert len(sessions) == len(update_time_and_session_id)
    update_time_and_session_id_read = [(session.session_id, session.update_time) for session in sessions]
    assert [item[1] for item in update_time_and_session_id_read] == [item[1] for item in update_time_and_session_id]
    sessions = list(memory_store.list_recent_sessions(user_id="1", max_count=2))
    assert len(sessions) == min(2, user_id_1_count)

    sessions = list(memory_store.list_recent_sessions(user_id="1", metadata_filter=Filters.eq("meta_boolean", True)))
    assert len(sessions) == user_id_meta_boolean_true_count

    sessions = list(
        memory_store.list_recent_sessions(
            user_id="1",
            metadata_filter=Filters.logical_and([Filters.gt("meta_double", 0.5), Filters.eq("meta_boolean", True)]),
        )
    )
    assert len(sessions) == user_id_meta_double_gt_half_1_and_meta_bool_true_count

    sessions = list(
        memory_store.list_recent_sessions(
            user_id="1",
            metadata_filter=Filters.logical_not(
                [
                    Filters.logical_and(
                        [
                            Filters.gt("meta_double", 0.5),
                            Filters.eq("meta_boolean", True),
                        ]
                    )
                ]
            ),
        )
    )
    assert len(sessions) == user_id_1_count - user_id_meta_double_gt_half_1_and_meta_bool_true_count

    sessions = list(
        memory_store.list_recent_sessions(
            user_id="1",
            inclusive_end_update_time=50,
            metadata_filter=Filters.eq("meta_boolean", True),
        )
    )
    update_times = [item.update_time for item in sessions]
    assert all(x >= 50 for x in update_times), f"Not all elements are greater than 50, but is:{update_times}"

    sessions = list(memory_store.list_recent_sessions(user_id="1"))
    sessions_paginated = []
    token = None
    while True:
        sub_sessions, token = memory_store.list_recent_sessions_paginated(user_id="1", page_size=3, next_token=token)
        sessions_paginated.extend(sub_sessions)
        if token is None:
            break
    assert sessions_paginated == sessions

    memory_store.delete_sessions(user_id="1")
    sessions = list(memory_store.list_sessions(user_id="1"))
    assert len(sessions) == 0
    sessions = list(memory_store.list_recent_sessions(user_id="1"))
    assert len(sessions) == 0

    memory_store.delete_all_sessions()
    all_sessions = list(memory_store.list_all_sessions())
    assert len(all_sessions) == 0


def test_get_message(memory_store):
    assert memory_store is not None
    memory_store._delete_table()
    memory_store.init_table()
    session_id_1_count = 0
    session_id_meta_boolean_true_count = 0
    session_id_meta_double_gt_half_1_count = 0
    session_id_meta_double_gt_half_1_and_meta_bool_true_count = 0
    total_count = random.randint(10, 99)
    for i in range(total_count):
        session_id = random.choice(["1", "2"])
        message = random_message(session_id=session_id)
        message.create_time = random.randint(1, 100)
        memory_store.put_message(message)
        if session_id == "1":
            session_id_1_count += 1
            if message.metadata["meta_boolean"]:
                session_id_meta_boolean_true_count += 1
            if message.metadata["meta_double"] > 0.5:
                session_id_meta_double_gt_half_1_count += 1
            if message.metadata["meta_boolean"] and message.metadata["meta_double"] > 0.5:
                session_id_meta_double_gt_half_1_and_meta_bool_true_count += 1

    logger.info(f"session_id_1_count:{session_id_1_count}")

    all_message = list(memory_store.get_all_messages())
    assert len(all_message) == total_count

    messages = list(memory_store.get_messages(session_id="1"))
    assert len(messages) == session_id_1_count

    messages = list(memory_store.get_messages(session_id="1", max_count=5))
    assert len(messages) == min(5, session_id_1_count)

    for test_times in range(20):
        desc_start = random.randint(51, 95)
        desc_end = random.randint(1, 50)
        messages = list(
            memory_store.get_messages(
                session_id="1",
                inclusive_start_create_time=desc_start,
                inclusive_end_create_time=desc_end,
                order=Order.DESC,
            )
        )
        assert all(
            desc_end <= x.create_time <= desc_start for x in messages
        ), f"Not all elements assert true, detail is:{[x.create_time for x in messages]}"
        messages = list(
            memory_store.get_messages(session_id="1", inclusive_start_create_time=desc_start, order=Order.DESC)
        )
        assert all(
            x.create_time <= desc_start for x in messages
        ), f"Not all elements assert true, detail is:{[x.create_time for x in messages]}"
        messages = list(memory_store.get_messages(session_id="1", inclusive_end_create_time=desc_end, order=Order.DESC))
        assert all(
            desc_end <= x.create_time for x in messages
        ), f"Not all elements assert true, detail is:{[x.create_time for x in messages]}"

        asc_start = random.randint(1, 50)
        asc_end = random.randint(51, 95)
        messages = list(
            memory_store.get_messages(
                session_id="1",
                inclusive_start_create_time=asc_start,
                inclusive_end_create_time=asc_end,
                order=Order.ASC,
            )
        )
        assert all(
            asc_start <= x.create_time <= asc_end for x in messages
        ), f"Not all elements assert true, detail is:{[x.create_time for x in messages]}"
        messages = list(
            memory_store.get_messages(session_id="1", inclusive_start_create_time=asc_start, order=Order.ASC)
        )
        assert all(
            asc_start <= x.create_time for x in messages
        ), f"Not all elements assert true, detail is:{[x.create_time for x in messages]}"
        messages = list(memory_store.get_messages(session_id="1", inclusive_end_create_time=asc_end, order=Order.ASC))
        assert all(
            x.create_time <= asc_end for x in messages
        ), f"Not all elements assert true, detail is:{[x.create_time for x in messages]}"

        messages = list(
            memory_store.get_messages(
                session_id="1",
                inclusive_start_create_time=asc_start,
                inclusive_end_create_time=asc_end,
                order=Order.ASC,
                metadata_filter=Filters.logical_and([Filters.gt("meta_double", 0.5), Filters.eq("meta_boolean", True)]),
            )
        )
        assert all(
            asc_start <= x.create_time <= asc_end for x in messages
        ), f"Not all elements assert true, detail is:{[x.create_time for x in messages]}"
        assert all(
            x.metadata["meta_double"] > 0.5 for x in messages
        ), f"Not all elements assert true, detail is:{[x.metadata['meta_double'] for x in messages]}"
        assert all(
            x.metadata["meta_boolean"] for x in messages
        ), f"Not all elements assert true, detail is:{[x.metadata['meta_boolean'] for x in messages]}"

    messages = list(memory_store.get_messages(session_id="1", metadata_filter=Filters.eq("meta_boolean", True)))
    assert len(messages) == session_id_meta_boolean_true_count

    messages = list(memory_store.get_messages(session_id="1", metadata_filter=Filters.not_eq("meta_boolean", False)))
    assert len(messages) == session_id_meta_boolean_true_count

    messages = list(
        memory_store.get_messages(
            session_id="1",
            metadata_filter=Filters.logical_and([Filters.gt("meta_double", 0.5), Filters.eq("meta_boolean", True)]),
        )
    )
    assert len(messages) == session_id_meta_double_gt_half_1_and_meta_bool_true_count

    messages = list(
        memory_store.get_messages(
            session_id="1",
            metadata_filter=Filters.logical_not(
                [
                    Filters.logical_and(
                        [
                            Filters.gt("meta_double", 0.5),
                            Filters.eq("meta_boolean", True),
                        ]
                    )
                ]
            ),
        )
    )
    assert len(messages) == session_id_1_count - session_id_meta_double_gt_half_1_and_meta_bool_true_count

    memory_store.delete_messages(session_id="1")
    messages = list(memory_store.get_messages(session_id="1"))
    assert len(messages) == 0

    memory_store.delete_all_messages()
    all_messages = list(memory_store.get_all_messages())
    assert len(all_messages) == 0


def test_pydantic(memory_store):
    memory_store.init_table()
    memory_store.init_search_index()
    assert memory_store is not None
    memory_store.get_messages(session_id="1", batch_size=1)
    with pytest.raises(ValidationError):
        memory_store.get_messages(session_id="1", batch_size=6000)
    memory_store.list_recent_sessions(user_id="1")
    with pytest.raises(ValidationError):
        memory_store.search_sessions(limit=101)
    with pytest.raises(ValidationError):
        memory_store.search_messages(limit=101)
    with pytest.raises(ValidationError):
        memory_store.search_sessions(limit=0)
    with pytest.raises(ValidationError):
        memory_store.search_messages(limit=0)
    memory_store.search_sessions(limit=100)
    memory_store.search_messages(limit=100)


# noinspection DuplicatedCode
def test_session_search_index(memory_store):
    assert memory_store is not None
    memory_store._delete_table()
    memory_store.init_table()
    memory_store.init_search_index()
    memory_store.delete_all_sessions()
    memory_store.delete_all_messages()

    user_id_1_count = 0
    user_id_meta_boolean_true_count = 0
    user_id_meta_double_gt_half_1_count = 0
    user_id_meta_double_gt_half_1_and_meta_bool_true_count = 0
    total_count = random.randint(50, 99)
    for i in range(total_count):
        user_id = random.choice(["1", "2"])
        session = random_session(user_id=user_id)
        session.update_time = random.randint(1, 100)
        memory_store.put_session(session)
        if user_id == "1":
            user_id_1_count += 1
            if session.metadata["meta_boolean"]:
                user_id_meta_boolean_true_count += 1
            if session.metadata["meta_double"] > 0.5:
                user_id_meta_double_gt_half_1_count += 1
            if session.metadata["meta_boolean"] and session.metadata["meta_double"] > 0.5:
                user_id_meta_double_gt_half_1_and_meta_bool_true_count += 1
    all_sessions = list(memory_store.list_all_sessions())
    assert len(all_sessions) == total_count
    TablestoreHelper.wait_search_index_ready(
        tablestore_client=memory_store._client,
        table_name=memory_store._session_table_name,
        index_name=memory_store._session_search_index_name,
        total_count=total_count,
    )

    sessions, next_token = memory_store.search_sessions(limit=100)
    assert len(sessions) == total_count
    print(len(sessions))
    assert next_token is None, next_token

    sessions, next_token = memory_store.search_sessions(metadata_filter=Filters.all(), limit=100)
    assert next_token is None, next_token
    assert len(sessions) == total_count
    session = sessions[0]
    print(session)
    assert session.session_id is not None
    assert session.user_id is not None
    assert session.update_time is not None
    assert session.metadata is not None
    assert "meta_boolean" in session.metadata

    sessions, next_token = memory_store.search_sessions(metadata_filter=Filters.eq("user_id", "1"), limit=100)
    assert len(sessions) == user_id_1_count
    sessions, next_token = memory_store.search_sessions(metadata_filter=Filters.eq("user_id", "1"), limit=5)
    assert len(sessions) == 5
    assert all(
        session.user_id == "1" for session in sessions
    ), f"Not all elements assert true, detail is:{[session.user_id for session in sessions]}"
    assert next_token is not None
    print(next_token)
    while True:
        sub_sessions, next_token = memory_store.search_sessions(
            metadata_filter=Filters.eq("user_id", "1"), limit=5, next_token=next_token
        )
        sessions.extend(sub_sessions)
        if next_token is None:
            break
    print(len(sessions))

    sessions, next_token = memory_store.search_sessions(
        metadata_filter=Filters.logical_and([Filters.eq("user_id", "1"), Filters.gt("meta_double", 0.5)])
    )
    print("user_id_meta_double_gt_half_1_count", user_id_meta_double_gt_half_1_count)
    assert len(sessions) == user_id_meta_double_gt_half_1_count

    sessions, next_token = memory_store.search_sessions(
        metadata_filter=Filters.logical_and([Filters.eq("user_id", "1"), Filters.eq("meta_boolean", True)])
    )
    print("user_id_meta_boolean_true_count", user_id_meta_boolean_true_count)
    assert len(sessions) == user_id_meta_boolean_true_count

    sessions, next_token = memory_store.search_sessions(
        metadata_filter=Filters.logical_and(
            [Filters.eq("user_id", "1"), Filters.gt("meta_double", 0.5), Filters.eq("meta_boolean", True)]
        )
    )
    print(
        "user_id_meta_double_gt_half_1_and_meta_bool_true_count", user_id_meta_double_gt_half_1_and_meta_bool_true_count
    )
    assert len(sessions) == user_id_meta_double_gt_half_1_and_meta_bool_true_count

    sessions, next_token = memory_store.search_sessions(
        metadata_filter=Filters.logical_and([Filters.eq("user_id", "1"), Filters.text_match_phrase("title", "ab")]),
        limit=100,
    )
    assert next_token is None, next_token
    print(len(sessions))
    assert all(
        "abc" in session.metadata["title"] for session in sessions
    ), f"Not all elements assert true, detail is:{[session.metadata for session in sessions]}"
    assert all(
        session.user_id == "1" for session in sessions
    ), f"Not all elements assert true, detail is:{[session.user_id for session in sessions]}"


# noinspection DuplicatedCode
def test_message_search_index(memory_store):
    assert memory_store is not None
    memory_store._delete_table()
    memory_store.init_table()
    memory_store.init_search_index()
    memory_store.delete_all_sessions()
    memory_store.delete_all_messages()

    session_id_1_count = 0
    session_id_meta_boolean_true_count = 0
    session_id_meta_double_gt_half_1_count = 0
    session_id_meta_double_gt_half_1_and_meta_bool_true_count = 0
    total_count = random.randint(50, 99)
    for i in range(total_count):
        session_id = random.choice(["1", "2"])
        message = random_message(session_id=session_id)
        message.create_time = random.randint(1, 100)
        memory_store.put_message(message)
        if session_id == "1":
            session_id_1_count += 1
            if message.metadata["meta_boolean"]:
                session_id_meta_boolean_true_count += 1
            if message.metadata["meta_double"] > 0.5:
                session_id_meta_double_gt_half_1_count += 1
            if message.metadata["meta_boolean"] and message.metadata["meta_double"] > 0.5:
                session_id_meta_double_gt_half_1_and_meta_bool_true_count += 1

    logger.info(f"session_id_1_count:{session_id_1_count}")

    all_message = list(memory_store.get_all_messages())
    assert len(all_message) == total_count
    TablestoreHelper.wait_search_index_ready(
        tablestore_client=memory_store._client,
        table_name=memory_store._message_table_name,
        index_name=memory_store._message_search_index_name,
        total_count=total_count,
    )

    messages, next_token = memory_store.search_messages(limit=100)
    assert len(messages) == total_count
    print(len(messages))
    assert next_token is None, next_token

    messages, next_token = memory_store.search_messages(metadata_filter=Filters.all(), limit=100)
    assert next_token is None, next_token
    assert len(messages) == total_count
    message = messages[0]
    print(message)
    assert message.session_id is not None
    assert message.message_id is not None
    assert message.create_time is not None
    assert message.content is not None
    assert message.metadata is not None
    assert "meta_boolean" in message.metadata

    messages, next_token = memory_store.search_messages(metadata_filter=Filters.eq("session_id", "1"), limit=100)
    assert len(messages) == session_id_1_count
    messages, next_token = memory_store.search_messages(metadata_filter=Filters.eq("session_id", "1"), limit=5)
    assert len(messages) == 5
    assert all(
        session.session_id == "1" for session in messages
    ), f"Not all elements assert true, detail is:{[session.session_id for session in messages]}"
    assert next_token is not None
    print(next_token)
    while True:
        sub_messages, next_token = memory_store.search_messages(
            metadata_filter=Filters.eq("session_id", "1"), limit=5, next_token=next_token
        )
        messages.extend(sub_messages)
        if next_token is None:
            break
    print(len(messages))

    messages, next_token = memory_store.search_messages(
        metadata_filter=Filters.logical_and([Filters.eq("session_id", "1"), Filters.gt("meta_double", 0.5)])
    )
    print("session_id_meta_double_gt_half_1_count", session_id_meta_double_gt_half_1_count)
    assert len(messages) == session_id_meta_double_gt_half_1_count

    messages, next_token = memory_store.search_messages(
        metadata_filter=Filters.logical_and([Filters.eq("session_id", "1"), Filters.eq("meta_boolean", True)])
    )
    print("session_id_meta_boolean_true_count", session_id_meta_boolean_true_count)
    assert len(messages) == session_id_meta_boolean_true_count

    messages, next_token = memory_store.search_messages(
        metadata_filter=Filters.logical_and(
            [Filters.eq("session_id", "1"), Filters.gt("meta_double", 0.5), Filters.eq("meta_boolean", True)]
        )
    )
    print(
        "session_id_meta_double_gt_half_1_and_meta_bool_true_count",
        session_id_meta_double_gt_half_1_and_meta_bool_true_count,
    )
    assert len(messages) == session_id_meta_double_gt_half_1_and_meta_bool_true_count

    messages, next_token = memory_store.search_messages(
        metadata_filter=Filters.logical_and([Filters.eq("session_id", "1"), Filters.text_match("content", "abc")]),
        limit=100,
    )
    assert next_token is None, next_token
    print(len(messages))
    assert all(
        "abc" in session.content for session in messages
    ), f"Not all elements assert true, detail is:{[session.content for session in messages]}"
    assert all(
        session.session_id == "1" for session in messages
    ), f"Not all elements assert true, detail is:{[session.session_id for session in messages]}"
