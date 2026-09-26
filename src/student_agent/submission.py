from __future__ import annotations

import json
import re
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import OUTPUT_SCHEMA_VERSION, VARIANT_ID
from .cases import CaseSet
from .contracts import Contracts

SECRET_PATTERN = re.compile(r"sk-team-[A-Za-z0-9_-]{8,}")
MAX_FILE_BYTES = 1024 * 1024
MAX_SUBMISSION_BYTES = 12 * 1024 * 1024
REQUIRED_LIFECYCLE = (
    'case_received',
    'task_assigned',
    'tool_result_consumed',
    'handoff',
    'policy_decided',
    'verification_completed',
    'case_finalized',
)


def _validate_case_lifecycle(
    case_id: str,
    events: list[dict[str, Any]],
    evidence_refs: list[str],
    output_claims: list[dict[str, Any]] | None = None,
) -> None:
    event_types = [event['event_type'] for event in events]
    cursor = -1
    for required in REQUIRED_LIFECYCLE:
        try:
            cursor = event_types.index(required, cursor + 1)
        except ValueError as exc:
            raise ValueError(
                f'{case_id}: trace lifecycle is missing or out of order: {required}'
            ) from exc
    if event_types.count('case_received') != 1 or event_types.count('case_finalized') != 1:
        raise ValueError(f'{case_id}: trace must contain one receive and one finalize event')
    if event_types[0] != 'case_received' or event_types[-1] != 'case_finalized':
        raise ValueError(f'{case_id}: trace receive/finalize boundaries are invalid')
    submitted_refs = set(evidence_refs)
    for output_claim in output_claims or ():
        submitted_refs.update(output_claim.get('evidence_refs', ()))
    consumed = {
        ref
        for event in events
        if event['event_type'] == 'tool_result_consumed'
        for ref in event.get('evidence_refs', [])
    }
    if not submitted_refs <= consumed:
        raise ValueError(f'{case_id}: output evidence is not linked to consumed tool results')
    specialists = {'order-item-agent', 'payment-agent', 'shipment-agent'}
    expected_actors = {
        'case_received': 'coordinator',
        'task_assigned': 'coordinator',
        'policy_decided': 'policy-agent',
        'verification_completed': 'verifier-agent',
        'case_finalized': 'coordinator',
    }
    for event in events:
        event_type = event['event_type']
        expected_actor = expected_actors.get(event_type)
        if expected_actor and event.get('actor') != expected_actor:
            raise ValueError(f'{case_id}: invalid actor for {event_type}')
        if event_type == 'task_assigned' and event.get('target') not in (
            specialists | {'policy-agent'}
        ):
            raise ValueError(f'{case_id}: invalid task assignment target')
        if event_type == 'tool_result_consumed' and (
            event.get('actor') not in specialists | {'policy-agent'}
            or not event.get('tool_name')
            or not event.get('evidence_refs')
        ):
            raise ValueError(f'{case_id}: invalid tool consumption event')
        if event_type == 'handoff':
            actor, target = event.get('actor'), event.get('target')
            if not (
                actor in specialists and target == 'policy-agent'
                or actor == 'policy-agent' and target == 'verifier-agent'
            ):
                raise ValueError(f'{case_id}: invalid handoff')
        if event_type == 'verification_completed' and event.get('target') != 'coordinator':
            raise ValueError(f'{case_id}: verifier must hand result to coordinator')
    assigned = {
        event.get('target') for event in events if event['event_type'] == 'task_assigned'
    }
    if not specialists | {'policy-agent'} <= assigned:
        raise ValueError(f'{case_id}: specialist/policy task assignments are incomplete')
    handoffs = {
        (event.get('actor'), event.get('target'))
        for event in events
        if event['event_type'] == 'handoff'
    }
    required_handoffs = {
        *((actor, 'policy-agent') for actor in specialists),
        ('policy-agent', 'verifier-agent'),
    }
    if not required_handoffs <= handoffs:
        raise ValueError(f'{case_id}: agent handoffs are incomplete')
    if event_types.count('policy_decided') != 1 or event_types.count(
        'verification_completed'
    ) != 1:
        raise ValueError(f'{case_id}: policy and verification must complete exactly once')


def _json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{path}: invalid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return value


def build_manifest(case_set: CaseSet) -> dict[str, Any]:
    return {
        "schema_version": "day09-submission-manifest-v2",
        "competition_id": "day09-multiagent-mcp-a2a",
        "variant_id": VARIANT_ID,
        "case_set_version": case_set.version,
        "output_schema_version": OUTPUT_SCHEMA_VERSION,
        "trace_schema_version": "day09-trace-event-v1",
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "client": {"name": "day09-student-starter", "version": "0.1.0"},
    }


def validate_artifacts(
    root: Path, case_set: CaseSet, contracts: Contracts
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    outputs_root = root / "outputs"
    actual = {path.stem: path for path in outputs_root.glob("*.json") if path.is_file()}
    expected = set(case_set.case_ids)
    if set(actual) != expected:
        missing = sorted(expected - set(actual))
        extra = sorted(set(actual) - expected)
        raise ValueError(f"outputs do not match case-set; missing={missing}, extra={extra}")

    outputs: dict[str, dict[str, Any]] = {}
    for case_id in case_set.case_ids:
        output = _json_object(actual[case_id])
        contracts.validate_output(output, f"outputs/{case_id}.json")
        if output.get("case_id") != case_id:
            raise ValueError(f"outputs/{case_id}.json has a mismatched case_id")
        outputs[case_id] = output

    trace_path = root / "traces" / "trace.jsonl"
    try:
        trace_lines = trace_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError("traces/trace.jsonl is missing or not UTF-8") from exc
    normalized_lines: list[str] = []
    seen_events: set[str] = set()
    for number, line in enumerate(trace_lines, 1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"traces/trace.jsonl:{number}: invalid JSON") from exc
        contracts.validate_trace(event, f"traces/trace.jsonl:{number}")
        if event["case_id"] not in expected:
            raise ValueError(f"traces/trace.jsonl:{number}: case is outside this case-set")
        if event["event_id"] in seen_events:
            raise ValueError(f"traces/trace.jsonl:{number}: duplicate event_id")
        seen_events.add(event["event_id"])
        normalized_lines.append(json.dumps(event, ensure_ascii=False, separators=(",", ":")))

    serialized = [json.dumps(value, ensure_ascii=False) for value in outputs.values()]
    if SECRET_PATTERN.search("\n".join([*serialized, *normalized_lines])):
        raise ValueError("a Team API Key appears in output or trace")
    events_by_case: dict[str, list[dict[str, Any]]] = {case_id: [] for case_id in expected}
    for line in normalized_lines:
        event = json.loads(line)
        events_by_case[event['case_id']].append(event)
    for case_id, output in outputs.items():
        _validate_case_lifecycle(
            case_id,
            events_by_case[case_id],
            output['evidence_refs'],
            output.get('claim_assessments', []),
        )
    return outputs, normalized_lines


def package_submission(root: Path, destination: Path) -> Path:
    from .cases import load_case_set

    root = root.resolve()
    case_set = load_case_set(root)
    contracts = Contracts(root / "contracts" / "schemas")
    outputs, trace_lines = validate_artifacts(root, case_set, contracts)
    manifest = build_manifest(case_set)
    contracts.validate_manifest(manifest)

    payloads = {
        "manifest.json": json.dumps(manifest, separators=(",", ":")).encode(),
        "trace.jsonl": ("\n".join(trace_lines) + ("\n" if trace_lines else "")).encode(),
        **{
            f"outputs/{case_id}.json": json.dumps(
                outputs[case_id], ensure_ascii=False, separators=(",", ":")
            ).encode()
            for case_id in case_set.case_ids
        },
    }
    oversized = [name for name, payload in payloads.items() if len(payload) > MAX_FILE_BYTES]
    if oversized:
        raise ValueError(f"submission files exceed 1 MB: {oversized}")
    if sum(map(len, payloads.values())) > MAX_SUBMISSION_BYTES:
        raise ValueError("submission exceeds the 12 MB uncompressed limit")

    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in payloads.items():
            archive.writestr(name, payload)
    return destination
