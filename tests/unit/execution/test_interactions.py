"""Pure full-set HITL rules; no native execution, durable receipt or transport."""

from dataclasses import FrozenInstanceError, replace
from typing import Any, cast

import pytest

from kokoro_agent.domain.run.interactions import (
    Decision,
    InteractionConflict,
    InteractionState,
    IntentStatus,
    PendingGroup,
    PendingItem,
    Phase,
    Submission,
    ValidationIssue,
)


def groups(*, validation: bool = False) -> tuple[PendingGroup, ...]:
    return (
        PendingGroup(
            group_id="opaque-child-group",
            items=(
                PendingItem(
                    item_id="item-a",
                    request_id="same-business-id",
                    allowed_decisions=("submit", "reject"),
                    validation=ValidationIssue(instance_path=("name", 0))
                    if validation
                    else None,
                ),
                PendingItem(
                    item_id="item-b",
                    request_id="same-business-id",
                    allowed_decisions=("approve", "reject"),
                ),
            ),
        ),
        PendingGroup(
            group_id="opaque-root-group",
            items=(
                PendingItem(
                    item_id="item-c",
                    request_id="request-c",
                    allowed_decisions=("edit", "reject"),
                ),
            ),
        ),
    )


def waiting() -> InteractionState:
    return InteractionState().pause(pause_ref="pause-1", groups=groups())


def submission(**changes: Any) -> Submission:
    value = Submission(
        command_id="command-1",
        pause_revision=1,
        pause_ref="pause-1",
        decisions=(
            Decision(item_id="item-c", kind="edit", payload=b'{"value":1}'),
            Decision(item_id="item-a", kind="submit", payload=b'{"name":["ok"]}'),
            Decision(item_id="item-b", kind="approve"),
        ),
    )
    return replace(value, **changes)


def started() -> InteractionState:
    return (
        waiting()
        .accept(submission())
        .start(command_id="command-1", attempt_id="attempt-1")
    )


def test_full_set_keeps_group_and_item_order_not_arrival_order() -> None:
    before = waiting()
    after = before.accept(submission())
    assert before.phase is Phase.WAITING
    assert after.phase is Phase.RESUMING
    assert after.groups == before.groups
    assert after.interaction_revision == 2 and after.pause_revision == 1
    assert after.intent is not None
    assert after.intent.status is IntentStatus.ACCEPTED
    assert after.intent.can_dispatch
    assert tuple(group.group_id for group in after.intent.groups) == (
        "opaque-child-group",
        "opaque-root-group",
    )
    assert tuple(
        tuple(d.item_id for d in group.decisions) for group in after.intent.groups
    ) == (("item-a", "item-b"), ("item-c",))


@pytest.mark.parametrize(
    "fault", ["missing", "extra", "duplicate", "stale", "wrong_ref", "disallowed"]
)
def test_invalid_full_submission_rejects_entire_batch_without_transition(
    fault: str,
) -> None:
    before = waiting()
    command = submission()
    decisions = command.decisions
    if fault == "missing":
        command = replace(command, decisions=decisions[:-1])
    elif fault == "extra":
        command = replace(
            command, decisions=decisions + (Decision(item_id="extra", kind="approve"),)
        )
    elif fault == "duplicate":
        command = replace(command, decisions=decisions + (decisions[0],))
    elif fault == "stale":
        command = replace(command, pause_revision=2)
    elif fault == "wrong_ref":
        command = replace(command, pause_ref="another-pause")
    else:
        command = replace(
            command, decisions=(replace(decisions[0], kind="submit"), *decisions[1:])
        )
    with pytest.raises(InteractionConflict):
        before.accept(command)
    assert before == waiting()
    assert before.intent is None


def test_replay_normalizes_order_but_requires_exact_payload_and_identity() -> None:
    command = submission()
    state = waiting().accept(command)
    assert (
        state.accept(replace(command, decisions=tuple(reversed(command.decisions))))
        is state
    )
    assert state.intent is not None
    for changed in (
        replace(
            command,
            decisions=(
                replace(command.decisions[0], payload=b'{ "value": 1 }'),
                *command.decisions[1:],
            ),
        ),
        replace(command, pause_ref="different"),
        replace(command, pause_revision=2),
    ):
        with pytest.raises(InteractionConflict, match="command_conflict"):
            state.accept(changed)
    with pytest.raises(InteractionConflict, match="not_waiting"):
        state.accept(replace(command, command_id="command-2"))


def test_started_is_irreversible_and_exact_start_replay_never_dispatches_again() -> (
    None
):
    accepted = waiting().accept(submission())
    state = accepted.start(command_id="command-1", attempt_id="attempt-1")
    assert state.intent is not None
    assert state.intent.status is IntentStatus.DISPATCH_STARTED
    assert not state.intent.can_dispatch
    assert state.interaction_revision == accepted.interaction_revision
    assert state.start(command_id="command-1", attempt_id="attempt-1") is state
    assert state.accept(submission()) is state
    assert state.intent.status is IntentStatus.DISPATCH_STARTED
    with pytest.raises(InteractionConflict):
        state.start(command_id="command-1", attempt_id="attempt-2")
    with pytest.raises(InteractionConflict):
        state.start(command_id="other-command", attempt_id="attempt-1")


def test_unknown_preserves_pending_and_blocks_all_redispatch() -> None:
    state = started().unknown(command_id="command-1")
    assert state.phase is Phase.RESUMING and state.groups == groups()
    assert state.intent is not None and state.intent.status is IntentStatus.UNKNOWN
    assert not state.intent.can_dispatch
    assert state.accept(submission()) is state
    assert state.unknown(command_id="command-1") is state
    with pytest.raises(InteractionConflict):
        state.start(command_id="command-1", attempt_id="attempt-1")
    with pytest.raises(InteractionConflict):
        state.start(command_id="command-1", attempt_id="new-attempt")
    with pytest.raises(InteractionConflict):
        waiting().accept(submission()).unknown(command_id="command-1")


def test_same_ids_validation_reask_is_new_pause_not_replayed_old_waiting() -> None:
    state = started()
    next_state = state.pause(pause_ref="pause-2", groups=groups(validation=True))
    assert next_state.phase is Phase.WAITING
    assert next_state.pause_revision == 2 and next_state.interaction_revision == 3
    assert (
        next_state.groups[0].items[0].request_id == state.groups[0].items[0].request_id
    )
    assert next_state.groups[0].items[0].validation == ValidationIssue(
        instance_path=("name", 0)
    )
    assert (
        next_state.intent is not None
        and next_state.intent.status is IntentStatus.RECONCILED
    )
    assert (
        next_state.accept(submission()) is next_state
    )  # Old command replay never advances new round.
    with pytest.raises(InteractionConflict):
        next_state.accept(submission(command_id="new-command"))
    new = next_state.accept(
        submission(command_id="new-command", pause_revision=2, pause_ref="pause-2")
    )
    assert new.phase is Phase.RESUMING
    assert state.intent is not None
    state.intent.verify_replay(
        submission()
    )  # Durable owner may load older ledger independently.


def test_pause_replay_does_not_increment_and_new_pause_requires_dispatch() -> None:
    state = waiting()
    assert state.pause(pause_ref="pause-1", groups=groups()) is state
    with pytest.raises(InteractionConflict):
        state.pause(pause_ref="pause-1", groups=groups(validation=True))
    with pytest.raises(InteractionConflict):
        state.pause(pause_ref="pause-2", groups=groups())
    with pytest.raises(InteractionConflict):
        state.accept(submission()).pause(pause_ref="pause-2", groups=groups())
    with pytest.raises(InteractionConflict):
        started().pause(pause_ref="pause-1", groups=groups(validation=True))


@pytest.mark.parametrize(
    "origin", ["active", "waiting", "accepted", "started", "unknown"]
)
def test_terminal_is_absorbing_and_clears_whole_collection(origin: str) -> None:
    states = {
        "active": InteractionState(),
        "waiting": waiting(),
        "accepted": waiting().accept(submission()),
        "started": started(),
        "unknown": started().unknown(command_id="command-1"),
    }
    previous = states[origin]
    terminal = previous.terminal()
    assert terminal.phase is Phase.TERMINAL and terminal.groups == ()
    assert terminal.interaction_revision == previous.interaction_revision + 1
    assert terminal.terminal() is terminal
    assert terminal.pause(pause_ref="late-pause", groups=groups()) is terminal
    if terminal.intent is not None:
        assert terminal.intent.status is IntentStatus.TERMINAL
        assert not terminal.intent.can_dispatch
        assert terminal.accept(submission()) is terminal
        assert terminal.unknown(command_id="command-1") is terminal
    with pytest.raises(InteractionConflict):
        terminal.accept(submission(command_id="new-command"))
    with pytest.raises(InteractionConflict):
        terminal.start(command_id="command-1", attempt_id="late-attempt")


def test_reject_decision_does_not_cancel_run() -> None:
    command = submission(
        decisions=tuple(
            Decision(item_id=item.item_id, kind="reject")
            for group in groups()
            for item in group.items
        )
    )
    assert waiting().accept(command).phase is Phase.RESUMING


def test_duplicates_in_pending_groups_or_item_id_are_rejected() -> None:
    with pytest.raises(ValueError):
        InteractionState().pause(pause_ref="p", groups=(groups()[0], groups()[0]))
    with pytest.raises(ValueError):
        InteractionState().pause(
            pause_ref="p",
            groups=(
                groups()[0],
                PendingGroup(group_id="different", items=groups()[0].items),
            ),
        )


@pytest.mark.parametrize(
    "constructor",
    [
        lambda: PendingItem(item_id="", request_id="r", allowed_decisions=("approve",)),
        lambda: PendingItem(item_id="i", request_id="r", allowed_decisions=()),
        lambda: PendingItem(
            item_id="i", request_id="r", allowed_decisions=("approve", "approve")
        ),
        lambda: PendingGroup(group_id="g", items=()),
        lambda: PendingGroup(group_id="g", items=cast(Any, [groups()[0].items[0]])),
        lambda: Decision(item_id="i", kind=cast(Any, "unknown")),
        lambda: Decision(
            item_id="i", kind="approve", payload=cast(Any, bytearray(b"x"))
        ),
        lambda: Submission(
            command_id="c", pause_ref="p", pause_revision=cast(Any, True), decisions=()
        ),
        lambda: InteractionState(interaction_revision=-1),
        lambda: InteractionState(phase=Phase.WAITING),
        lambda: ValidationIssue(instance_path=cast(Any, ["mutable"])),
    ],
)
def test_invalid_mutable_or_inconsistent_values_fail_closed(constructor: Any) -> None:
    with pytest.raises(ValueError):
        constructor()


def test_values_are_frozen_and_private_payload_is_not_in_repr() -> None:
    command = submission()
    with pytest.raises(FrozenInstanceError):
        cast(Any, command).command_id = "changed"
    assert '{"name"' not in repr(command)
    state = waiting()
    with pytest.raises(FrozenInstanceError):
        cast(Any, state).phase = Phase.TERMINAL


@pytest.mark.parametrize(
    "corruption", ["future_intent", "resuming_without_accept_revision"]
)
def test_reconstructed_head_rejects_impossible_revision_relationship(
    corruption: str,
) -> None:
    with pytest.raises(ValueError):
        if corruption == "future_intent":
            replace(started().terminal(), pause_revision=0, pause_ref=None)
        else:
            replace(waiting().accept(submission()), interaction_revision=1)


@pytest.mark.parametrize(
    "fault", ["missing", "extra", "duplicate", "changed_kind", "changed_command"]
)
def test_saved_intent_replay_rejects_changed_full_set(fault: str) -> None:
    state = started()
    assert state.intent is not None
    command = submission()
    if fault == "missing":
        command = replace(command, decisions=command.decisions[:-1])
    elif fault == "extra":
        command = replace(
            command,
            decisions=(*command.decisions, Decision(item_id="extra", kind="reject")),
        )
    elif fault == "duplicate":
        command = replace(command, decisions=(*command.decisions, command.decisions[0]))
    elif fault == "changed_kind":
        command = replace(
            command,
            decisions=(
                replace(command.decisions[0], kind="reject"),
                *command.decisions[1:],
            ),
        )
    else:
        command = replace(command, command_id="other")
    with pytest.raises(InteractionConflict, match="command_conflict"):
        state.intent.verify_replay(command)
    assert state == started()
