"""Build host evidence only after verifying the exact Docker process exited.

Participants must come from supervisor assignment or that process's diagnostics.
This module does not read or modify Concord or Beacon state.
"""


def verified_exit_handoff(instance, before, after, participants, previous=None):
    if (
        not before["State"]["Running"]
        or after["State"]["Running"]
        or after["State"]["ExitCode"] != 137
        or before["Id"] != after["Id"]
        or before["State"]["StartedAt"] != after["State"]["StartedAt"]
        or not before["State"]["StartedAt"]
    ):
        raise ValueError("exact predecessor SIGKILL exit is not confirmed")
    if not participants or len(participants) > 256:
        raise ValueError("missing process endpoint/session evidence")
    identities = set()
    for row in participants:
        if set(row) != {"endpoint", "sessionId"} or any(
            not isinstance(v, str) or not v or v.strip() != v for v in row.values()
        ):
            raise ValueError("invalid process endpoint/session evidence")
        identity = (row["endpoint"], row["sessionId"])
        if identity in identities:
            raise ValueError("duplicate process identity")
        identities.add(identity)
    result = dict(
        schema="dev.deckr.runtime.restart_handoff.v1",
        instanceId=instance,
        predecessors=[],
    )
    if previous is not None:
        if (
            previous.get("schema") != result["schema"]
            or previous.get("instanceId") != instance
            or set(previous) != set(result)
        ):
            raise ValueError("previous handoff belongs to another instance or schema")
        result["predecessors"] = list(previous["predecessors"])
    predecessor = dict(
        predecessorId=before["Id"] + ":" + before["State"]["StartedAt"],
        exitCode=137,
        participants=participants,
    )
    # A repeat acknowledgement is idempotent; retain earlier unresolved exits.
    for row in result["predecessors"]:
        if row["predecessorId"] == predecessor["predecessorId"]:
            if row != predecessor:
                raise ValueError("predecessor evidence changed")
            return result
    if len(result["predecessors"]) >= 256:
        raise ValueError(
            "restart handoff capacity exhausted; confirm cleanup before pruning"
        )
    result["predecessors"].append(predecessor)
    return result
