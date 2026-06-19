from __future__ import annotations

from kokoro_agent.application.text_accumulator import TextAccumulator


def test_fresh_accumulator_is_not_started_and_takes_none() -> None:
    acc = TextAccumulator()
    assert acc.started() is False
    assert acc.take() is None


def test_append_returns_the_incremental_slice_not_the_cumulative_buffer() -> None:
    acc = TextAccumulator()
    assert acc.append("晴，") == "晴，"
    assert acc.append("适合") == "适合"
    assert acc.started() is True


def test_take_returns_full_accumulation_then_resets_to_none() -> None:
    acc = TextAccumulator()
    acc.append("晴，")
    acc.append("适合出门。")
    assert acc.take() == "晴，适合出门。"
    assert acc.take() is None
    assert acc.started() is False


def test_empty_string_append_still_marks_started() -> None:
    # 空 delta 也开启缓冲——区分「来过流但内容为空」与「从未流过」。
    acc = TextAccumulator()
    acc.append("")
    assert acc.started() is True
    assert acc.take() == ""
