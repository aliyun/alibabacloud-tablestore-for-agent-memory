from tablestore_for_agent_memory.base.base_memory_store import Session
from tablestore_for_agent_memory.util.tablestore_helper import TablestoreHelper


def test_meta_data_to_ots_columns():
    session = Session(user_id="1", session_id="2")
    session.update_time = 3
    session.metadata["key_str"] = "4"
    session.metadata["key_int"] = 5
    session.metadata["key_double"] = 6.6
    session.metadata["key_bool"] = True
    session.metadata["key_bytearray"] = bytearray("China", encoding="utf8")
    t = TablestoreHelper.meta_data_to_ots_columns(session.metadata)
    print(t)

    assert session.update_time == 3

    session = Session(user_id="1", session_id="2")
    print(session.update_time)


def test_encode_next_primary_key_token():
    primary_key = [
        ("user_id", "123"),
        (
            "update_time",
            1744090297594000,
        ),
        ("session_id", "123abc"),
    ]
    next_token = TablestoreHelper.encode_next_primary_key_token(primary_key)
    assert (
        next_token
        == "W1sidXNlcl9pZCIsICIxMjMiXSwgWyJ1cGRhdGVfdGltZSIsIDE3NDQwOTAyOTc1OTQwMDBdLCBbInNlc3Npb25faWQiLCAiMTIzYWJjIl1d"
    )

    decoded_primary_key = TablestoreHelper.decode_next_primary_key_token(next_token)
    assert decoded_primary_key == primary_key


def test_batch():
    id_list = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    documents = []
    total = len(id_list)
    batch_size = 4
    for start in range(0, total, batch_size):
        end = start + batch_size
        current_batch = id_list[start:end]
        print(current_batch, start, end)
        documents.extend(current_batch)
    assert len(documents) == total
    print(documents)
    assert documents == id_list
