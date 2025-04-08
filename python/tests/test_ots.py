import math

from tablestore_for_agent_memory.base.common import microseconds_timestamp

from tablestore_for_agent_memory.base.base_memory_store import Session
from tablestore_for_agent_memory.util.ots import (
    decode_next_primary_key_token,
    encode_next_primary_key_token,
    meta_data_to_ots_columns,
)


def test_meta_data_to_ots_columns():
    session = Session(user_id="1", session_id="2")
    session.update_time = 3
    session.metadata["key_str"] = "4"
    session.metadata["key_int"] = 5
    session.metadata["key_double"] = 6.6
    session.metadata["key_bool"] = True
    session.metadata["key_bytearray"] = bytearray("China", encoding="utf8")
    t = meta_data_to_ots_columns(session.metadata)
    print(t)

    assert session.update_time == 3

    session = Session(user_id="1", session_id="2")
    print(session.update_time)
    assert math.fabs(session.update_time - microseconds_timestamp()) < 60 * 1000000


def test_encode_next_primary_key_token():
    primary_key = [
        ("user_id", "123"),
        (
            "update_time",
            1744090297594000,
        ),
        ("session_id", "123abc"),
    ]
    next_token = encode_next_primary_key_token(primary_key)
    assert (
        next_token
        == "W1sidXNlcl9pZCIsICIxMjMiXSwgWyJ1cGRhdGVfdGltZSIsIDE3NDQwOTAyOTc1OTQwMDBdLCBbInNlc3Npb25faWQiLCAiMTIzYWJjIl1d"
    )

    decoded_primary_key = decode_next_primary_key_token(next_token)
    assert decoded_primary_key == primary_key
