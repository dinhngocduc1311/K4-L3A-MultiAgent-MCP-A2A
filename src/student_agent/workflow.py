from __future__ import annotations

import asyncio
import json
from collections.abc import Iterable, Mapping
from contextlib import suppress
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from itertools import islice, product
from typing import TYPE_CHECKING, Any

from .mcp_gateway import is_transient_mcp_error
from .trace import TraceWriter

if TYPE_CHECKING:
    from .mcp_gateway import EvidenceGateway

MCP_CALL_TIMEOUT_SECONDS = 45
MAX_ARGUMENT_SETS_PER_TOOL = 20
AGENT_CALL_BUDGETS = {
    'order-item-agent': 12,
    'payment-agent': 8,
    'shipment-agent': 7,
    'policy-agent': 3,
}
ACTION_ISSUES = {
    'canceled_order_paid',
    'unavailable_order_paid',
    'late_delivery_seller',
    'late_delivery_logistics',
    'payment_mismatch',
    'duplicate_charge',
    'refund_pending',
    'refund_failed',
}
ISSUE_DOMAINS = {
    'canceled_order_paid': {'order', 'payment', 'policy'},
    'unavailable_order_paid': {'order', 'item', 'payment', 'policy'},
    'late_delivery_seller': {'order', 'item', 'shipment', 'policy'},
    'late_delivery_logistics': {'order', 'shipment', 'policy'},
    'valid_split_payment': {'order', 'item', 'payment', 'policy'},
    'payment_mismatch': {'order', 'item', 'payment', 'policy'},
    'duplicate_charge': {'order', 'item', 'payment', 'refund', 'policy'},
    'refund_pending': {'order', 'item', 'payment', 'refund', 'policy'},
    'refund_failed': {'order', 'item', 'payment', 'refund', 'policy'},
}
ISSUE_TOOLS = {
    'canceled_order_paid': {
        'get_order', 'get_payment', 'get_payment_timeline',
    },
    'unavailable_order_paid': {
        'get_order', 'get_order_items', 'get_payment', 'get_payment_timeline',
        'get_sellers',
    },
    'late_delivery_seller': {
        'get_order', 'get_order_items', 'get_shipment', 'get_shipment_summary',
    },
    'late_delivery_logistics': {
        'get_order', 'get_shipment', 'get_shipment_summary',
    },
    'valid_split_payment': {
        'get_order', 'get_order_items', 'get_payment', 'get_payment_timeline',
    },
    'payment_mismatch': {
        'get_order', 'get_order_items', 'get_payment', 'get_payment_timeline',
    },
    'duplicate_charge': {
        'get_order', 'get_order_items', 'get_payment', 'get_payment_timeline',
    },
    'refund_pending': {
        'get_order', 'get_order_items', 'get_order_payments', 'get_payment',
        'get_payment_timeline', 'get_refund_timeline',
    },
    'refund_failed': {
        'get_order', 'get_order_items', 'get_order_payments', 'get_payment',
        'get_payment_timeline', 'get_refund_timeline',
    },
    'unsupported_claim': {
        'get_order', 'get_order_items', 'get_payment', 'get_payment_timeline',
    },
}
REQUIRED_DOMAIN_GROUPS = {
    'canceled_order_paid': ({'order', 'item'}, {'payment'}),
    'unavailable_order_paid': ({'order', 'item'}, {'payment'}),
    'late_delivery_seller': ({'order', 'shipment'}, {'order', 'item', 'seller'}),
    'late_delivery_logistics': ({'order', 'shipment'},),
    'valid_split_payment': ({'order', 'item'}, {'payment'}),
    'payment_mismatch': ({'order', 'item'}, {'payment'}),
    'duplicate_charge': ({'payment'},),
    'refund_pending': ({'payment', 'refund'},),
    'refund_failed': ({'payment', 'refund'},),
}
ISSUE_SUPPORT_KEYS = {
    'canceled_order_paid': {
        'order_id', 'order_status', 'status', 'payment_id', 'payment_reference',
        'payment_status', 'captured_total_brl', 'captured_amount', 'payment_value',
    },
    'unavailable_order_paid': {
        'order_id', 'order_status', 'item_id', 'item_status', 'availability_status',
        'status', 'payment_id', 'payment_reference', 'payment_status',
        'captured_total_brl', 'captured_amount', 'payment_value',
    },
    'late_delivery_seller': {
        'order_id', 'item_id', 'seller_id', 'shipment_id', 'status', 'shipment_status',
        'delivery_status', 'delivered_at', 'delivery_date', 'estimated_delivery_date',
        'promised_at', 'order_delivered_customer_date', 'order_estimated_delivery_date',
        'order_delivered_carrier_date', 'shipping_limit_date', 'seller_delay',
        'seller_late', 'late_by_seller', 'responsible_party', 'delay_owner',
    },
    'late_delivery_logistics': {
        'order_id', 'shipment_id', 'status', 'shipment_status', 'delivery_status',
        'delivered_at', 'delivery_date', 'estimated_delivery_date', 'promised_at',
        'order_delivered_customer_date', 'order_estimated_delivery_date',
        'order_delivered_carrier_date', 'shipping_limit_date',
    },
    'valid_split_payment': {
        'order_id', 'item_id', 'payment_id', 'payment_reference', 'payment_sequence',
        'payment_sequential', 'payment_value', 'captured_total_brl', 'order_total_brl',
        'order_total', 'order_value', 'total_amount', 'price', 'freight_value',
        'split_payment', 'is_split_payment',
    },
    'payment_mismatch': {
        'order_id', 'item_id', 'payment_id', 'payment_reference', 'payment_value',
        'captured_total_brl', 'captured_amount', 'payment_total', 'order_total_brl',
        'order_total', 'order_value', 'total_amount', 'price', 'freight_value',
        'payment_mismatch', 'capture_mismatch',
    },
    'duplicate_charge': {
        'payment_id', 'payment_reference', 'payment_sequence', 'payment_sequential',
        'payment_value', 'captured_total_brl', 'duplicate_charge', 'duplicate_capture',
        'duplicate_amount_brl', 'duplicate_charge_amount_brl', 'is_duplicate', 'status',
        'payment_status', 'order_id', 'order_status', 'item_id', 'order_item_id',
        'price', 'freight_value',
    },
    'refund_pending': {
        'payment_id', 'payment_reference', 'refund_id', 'refund_status', 'refund_state',
        'status', 'requested_refund_brl', 'refund_amount_brl', 'refund_total_brl',
        'refunded_total_brl', 'refunded_amount', 'payment_sequential', 'payment_value',
        'order_id', 'order_status', 'item_id', 'order_item_id', 'price', 'freight_value',
    },
    'refund_failed': {
        'payment_id', 'payment_reference', 'refund_id', 'refund_status', 'refund_state',
        'status', 'requested_refund_brl', 'refund_amount_brl', 'refund_total_brl',
        'refunded_total_brl', 'refunded_amount', 'payment_sequential', 'payment_value',
        'order_id', 'order_status', 'item_id', 'order_item_id', 'price', 'freight_value',
    },
}
ACTOR_DOMAINS = {
    'order-item-agent': {'order', 'item', 'seller', 'product'},
    'payment-agent': {'payment', 'refund'},
    'shipment-agent': {'shipment'},
    'policy-agent': {'policy'},
}
PARTY_BY_ISSUE = {
    'late_delivery_seller': 'seller',
    'late_delivery_logistics': 'logistics_provider',
    'payment_mismatch': 'payment_provider',
    'duplicate_charge': 'payment_provider',
    'refund_pending': 'payment_provider',
    'refund_failed': 'payment_provider',
    'canceled_order_paid': 'platform',
    'unavailable_order_paid': 'seller',
    'valid_split_payment': 'customer',
    'unsupported_claim': 'customer',
}
FALLBACK_ACTIONS = {
    'canceled_order_paid': ['ISSUE_REFUND'],
    'unavailable_order_paid': ['ISSUE_REFUND'],
    'late_delivery_seller': ['CONTACT_SELLER'],
    'late_delivery_logistics': ['CONTACT_LOGISTICS_PROVIDER'],
    'payment_mismatch': ['RECONCILE_PAYMENT'],
    'duplicate_charge': ['REFUND_DUPLICATE_CHARGE'],
    'refund_pending': ['ESCALATE_REFUND_STATUS'],
    'refund_failed': ['RETRY_OR_ESCALATE_REFUND'],
    'valid_split_payment': ['DOCUMENT_NO_ACTION'],
    'unsupported_claim': ['DOCUMENT_NO_ACTION'],
    'insufficient_evidence': ['MANUAL_INVESTIGATION'],
}
ISSUE_PRIORITY = (
    'refund_failed', 'refund_pending', 'duplicate_charge', 'canceled_order_paid',
    'unavailable_order_paid', 'payment_mismatch', 'late_delivery_seller',
    'late_delivery_logistics', 'insufficient_evidence', 'valid_split_payment',
    'unsupported_claim',
)
IDENTITY_KEYS = {
    'order': ('order_id', 'order_ids'),
    'item': ('item_id', 'item_ids', 'order_item_id'),
    'seller': ('seller_id', 'seller_ids'),
    'payment': ('payment_id', 'payment_reference', 'payment_references'),
    'refund': ('refund_id', 'refund_ids'),
    'shipment': ('shipment_id', 'shipment_ids'),
}


async def solve_case(
    case: dict[str, Any], gateway: EvidenceGateway, trace: TraceWriter
) -> dict[str, Any]:
    case_id = case.get('case_id')
    if not isinstance(case_id, str):
        raise ValueError('case.case_id must be a string')

    specs = await _discover_specs(gateway)
    assignments = _assign_tools(specs)
    requested_tools = _requested_tools(case)
    evidence: list[dict[str, Any]] = []
    for actor in ('order-item-agent', 'payment-agent', 'shipment-agent'):
        trace.emit(
            case_id=case_id,
            event_type='task_assigned',
            actor='coordinator',
            target=actor,
            decision_code='DOMAIN_EVIDENCE_REQUESTED',
        )
        collected = await _collect(
            case,
            gateway,
            trace,
            actor,
            [
                name
                for name in assignments[actor]
                if requested_tools is None or name in requested_tools
            ],
            specs,
            AGENT_CALL_BUDGETS[actor],
        )
        evidence = _unique_evidence([*evidence, *collected])
        trace.emit(
            case_id=case_id,
            event_type='handoff',
            actor=actor,
            target='policy-agent',
            decision_code='DOMAIN_REVIEW_COMPLETE',
            evidence_refs=_refs(collected)[:20],
        )

    issue, status, confidence, decision_evidence, decision_scopes = _assessment_scope(
        case, evidence
    )
    policy_case = {
        **case,
        'issue': issue,
        'primary_issue': issue,
        'policy': issue,
        'policy_code': issue,
        'policy_name': issue,
    }
    trace.emit(
        case_id=case_id,
        event_type='task_assigned',
        actor='coordinator',
        target='policy-agent',
        decision_code='POLICY_REVIEW_REQUESTED',
    )
    policy_evidence = await _collect(
        policy_case,
        gateway,
        trace,
        'policy-agent',
        assignments['policy-agent'],
        specs,
        AGENT_CALL_BUDGETS['policy-agent'],
    )
    evidence = _unique_evidence([*evidence, *policy_evidence])
    decision_pool = _unique_evidence([*decision_evidence, *policy_evidence])
    if not policy_evidence:
        issue, status, confidence = 'insufficient_evidence', 'needs_investigation', 0.25
    else:
        policy_status = _strings(_matching_policy(policy_evidence, issue), 'case_status')
        if policy_status and policy_status[0] in {
            'action_required', 'no_action', 'needs_investigation'
        }:
            status = policy_status[0]
    if policy_evidence and not all(
        _financially_complete(issue, _unique_evidence([*scope, *policy_evidence]))
        for scope in decision_scopes
    ):
        issue, status, confidence = 'insufficient_evidence', 'needs_investigation', 0.30
    conflicts = _data_conflicts(decision_pool)
    if any(conflict['selected_source'] is None for conflict in conflicts):
        issue, status, confidence = 'insufficient_evidence', 'needs_investigation', 0.30
    confidence = _calibrate_confidence(issue, confidence, decision_pool, conflicts)

    trace.emit(
        case_id=case_id,
        event_type='policy_decided',
        actor='policy-agent',
        decision_code=issue.upper(),
        evidence_refs=_refs(policy_evidence)[:20],
    )
    trace.emit(
        case_id=case_id,
        event_type='handoff',
        actor='policy-agent',
        target='verifier-agent',
        decision_code='CANDIDATE_READY',
        evidence_refs=_refs(evidence)[:20],
    )

    supporting = _supporting_evidence(case, decision_pool, issue)
    output = _build_output(
        case,
        supporting,
        issue,
        status,
        confidence,
        conflicts,
    )
    _verify_output(case, evidence, output, trace, gateway)
    trace.emit(
        case_id=case_id,
        event_type='verification_completed',
        actor='verifier-agent',
        target='coordinator',
        decision_code='CONTRACT_AND_INVARIANTS_PASSED',
        evidence_refs=output['evidence_refs'][:20],
    )
    return output


async def _discover_specs(gateway: EvidenceGateway) -> dict[str, dict[str, Any]]:
    loader = getattr(gateway, 'tool_specs', None)
    if loader:
        return await loader()
    names = await gateway.list_tools()
    schema_loader = getattr(gateway, 'tool_schemas', None)
    schemas = await schema_loader() if schema_loader else {}
    return {
        name: {'description': '', 'input_schema': schemas.get(name, {}), 'output_schema': {}}
        for name in names
    }


def _assign_tools(specs: Mapping[str, dict[str, Any]]) -> dict[str, list[str]]:
    # ponytail: metadata vocabulary covers the public domains; add an explicit server
    # domain annotation only if future tools stop naming/describing their domain.
    assignments = {
        'order-item-agent': [],
        'payment-agent': [],
        'shipment-agent': [],
        'policy-agent': [],
    }
    groups = (
        ('policy-agent', ('policy', 'rule', 'eligibility')),
        ('payment-agent', ('payment', 'refund', 'charge', 'transaction')),
        ('shipment-agent', ('shipment', 'delivery', 'logistic', 'tracking', 'freight')),
        ('order-item-agent', ('order', 'item', 'seller', 'product')),
    )
    for name, spec in sorted(specs.items()):
        schema = spec.get('input_schema') or {}
        properties = schema.get('properties', {}) if isinstance(schema, dict) else {}
        output_schema = spec.get('output_schema') or {}
        output_properties = (
            output_schema.get('properties', {}) if isinstance(output_schema, dict) else {}
        )
        domain_schema = output_properties.get('domain', {})
        domain = domain_schema.get('const') if isinstance(domain_schema, dict) else None
        if isinstance(domain, str):
            for actor, allowed_domains in ACTOR_DOMAINS.items():
                if domain in allowed_domains:
                    assignments[actor].append(name)
                    break
            else:
                continue
            continue
        haystack = ' '.join((name, str(spec.get('description', '')), *properties)).lower()
        for actor, tokens in groups:
            if any(token in haystack for token in tokens):
                assignments[actor].append(name)
                break
    return assignments


def _requested_tools(case: Mapping[str, Any]) -> set[str] | None:
    claims = case.get('customer_request', {}).get('claims', [])
    topics = [claim.get('topic') for claim in claims if isinstance(claim, dict)]
    topic = next((topic for topic in topics if topic in ISSUE_TOOLS), None)
    if topic is None:
        return None
    tools = set(ISSUE_TOOLS[topic])
    if topic == 'unsupported_claim' and 'shipment' in json.dumps(case).lower():
        tools.update(('get_shipment', 'get_shipment_summary'))
    return tools


async def _collect(
    case: dict[str, Any],
    gateway: EvidenceGateway,
    trace: TraceWriter,
    actor: str,
    tool_names: Iterable[str],
    specs: Mapping[str, dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    planned = []
    for tool_name in tool_names:
        schema = specs.get(tool_name, {}).get('input_schema') or {}
        argument_sets = _tool_argument_sets(case, schema)
        if not argument_sets:
            continue
        planned.append((tool_name, argument_sets))
    calls = [
        (tool_name, argument_sets[index])
        for index in range(max((len(argument_sets) for _, argument_sets in planned), default=0))
        for tool_name, argument_sets in planned
        if index < len(argument_sets)
    ][: max(limit, 0)]
    async def call_one(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any] | None:
        try:
            return await _call_with_retry(
                gateway, tool_name, case_id=case['case_id'], arguments=arguments
            )
        except RuntimeError:
            if any(token in tool_name.lower() for token in ('refund', 'history', 'product')):
                return None
            raise

    items = await asyncio.gather(*(
        call_one(tool_name, arguments) for tool_name, arguments in calls
    ))
    for (tool_name, _), item in zip(calls, items, strict=True):
        if item is None:
            continue
        if item.get('domain') not in ACTOR_DOMAINS[actor]:
            raise ValueError(f'{actor} received forbidden evidence domain from {tool_name}')
        collected.append(item)
        trace.emit(
            case_id=case['case_id'],
            event_type='tool_result_consumed',
            actor=actor,
            tool_name=tool_name,
            evidence_refs=[item['evidence_ref']],
        )
    return collected


def _tool_arguments(case: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any] | None:
    argument_sets = _tool_argument_sets(case, schema)
    return argument_sets[0] if argument_sets else None


def _tool_argument_sets(case: dict[str, Any], schema: dict[str, Any]) -> list[dict[str, Any]]:
    if not schema:
        return [{}]
    properties = schema.get('properties', {})
    required = set(schema.get('required', ())) - {'case_id'}
    values = _case_values(case)
    options: dict[str, list[Any]] = {}
    for name, property_schema in properties.items():
        if name == 'case_id' or name not in values:
            continue
        value = values[name]
        expected = property_schema.get('type') if isinstance(property_schema, dict) else None
        if expected == 'array' and not isinstance(value, list):
            options[name] = [[value]]
        elif expected == 'array':
            options[name] = [value]
        elif isinstance(value, list):
            options[name] = [item for item in value if item is not None]
        elif value is not None:
            options[name] = [value]
    if required - options.keys() or any(not option for option in options.values()):
        return []
    non_case_properties = set(properties) - {'case_id'}
    if non_case_properties and not options:
        return []
    names = list(options)
    combinations = product(*(options[name] for name in names)) if names else [()]
    combinations = islice(combinations, MAX_ARGUMENT_SETS_PER_TOOL)
    argument_sets = [dict(zip(names, values, strict=True)) for values in combinations]
    unique = {json.dumps(value, sort_keys=True, default=str): value for value in argument_sets}
    return list(unique.values())


def _case_values(value: Any) -> dict[str, Any]:
    found: dict[str, Any] = {}
    if isinstance(value, dict):
        for key, child in value.items():
            if isinstance(child, (str, int, float, list)) and not isinstance(child, bool):
                found.setdefault(key, child)
                if key.startswith('claimed_'):
                    found.setdefault(key.removeprefix('claimed_'), child)
                if key.endswith('s') and isinstance(child, list) and child:
                    found.setdefault(key[:-1], child)
                elif not key.endswith('s'):
                    found.setdefault(f'{key}s', [child])
            if isinstance(child, (dict, list)):
                for nested_key, nested_value in _case_values(child).items():
                    found.setdefault(nested_key, nested_value)
    elif isinstance(value, list):
        for child in value:
            for nested_key, nested_value in _case_values(child).items():
                found.setdefault(nested_key, nested_value)
    return found


async def _call_with_retry(
    gateway: EvidenceGateway,
    tool_name: str,
    *,
    case_id: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    for attempt in range(2):
        try:
            async with asyncio.timeout(MCP_CALL_TIMEOUT_SECONDS):
                return await gateway.call(tool_name, case_id=case_id, **arguments)
        except Exception as exc:
            if not is_transient_mcp_error(exc) or attempt:
                raise
            await asyncio.sleep(0.2)
    raise AssertionError('unreachable')


def _walk(value: Any) -> Iterable[tuple[str, Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            yield key.lower(), child
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _unique_evidence(evidence: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return list({item['evidence_ref']: item for item in evidence}.values())[:30]


def _refs(evidence: Iterable[dict[str, Any]]) -> list[str]:
    return list(dict.fromkeys(item['evidence_ref'] for item in evidence))


def _domain_items(
    evidence: Iterable[dict[str, Any]], domains: set[str] | None = None
) -> list[dict[str, Any]]:
    return [item for item in evidence if domains is None or item.get('domain') in domains]


def _values(
    evidence: Iterable[dict[str, Any]],
    keys: Iterable[str],
    domains: set[str] | None = None,
) -> list[Any]:
    wanted = set(keys)
    found: list[Any] = []
    for item in _domain_items(evidence, domains):
        found.extend(value for key, value in _walk(item.get('data')) if key in wanted)
    return found


def _normalized_values(
    evidence: Iterable[dict[str, Any]], keys: Iterable[str], domains: set[str] | None = None
) -> set[str]:
    return {
        str(value).strip().lower().replace('-', '_').replace(' ', '_')
        for value in _values(evidence, keys, domains)
        if isinstance(value, (str, int, float)) and not isinstance(value, bool)
    }


def _flag(
    evidence: Iterable[dict[str, Any]], keys: Iterable[str], domains: set[str] | None = None
) -> bool:
    return any(
        value is True
        or value == 1
        or isinstance(value, str) and value.strip().lower() in {'1', 'true', 'yes'}
        for value in _values(evidence, keys, domains)
        if not isinstance(value, (dict, list))
    )


def _decimals(
    evidence: Iterable[dict[str, Any]], keys: Iterable[str], domains: set[str] | None = None
) -> list[Decimal]:
    result: list[Decimal] = []
    for value in _values(evidence, keys, domains):
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            continue
        with suppress(InvalidOperation):
            number = Decimal(str(value))
            if number.is_finite():
                result.append(number)
    return result


def _money_total(
    evidence: Iterable[dict[str, Any]],
    total_keys: Iterable[str],
    line_keys: Iterable[str],
    domains: set[str],
) -> Decimal | None:
    items = _domain_items(evidence, domains)

    def identity(item: dict[str, Any]) -> tuple[str, ...] | None:
        domain = str(item.get('domain'))
        order_ids = _ids({}, [item], 'order_id', 'order_ids')
        for key in IDENTITY_KEYS.get(domain, ()):
            own = _ids({}, [item], key)
            if len(own) == 1:
                return domain, *(order_ids[:1]), key, own[0]
        if domain == 'payment':
            sequences = _ids({}, [item], 'payment_sequence', 'payment_sequential')
            if len(sequences) == 1:
                return domain, *(order_ids[:1]), 'sequence', sequences[0]
        if len(order_ids) == 1:
            return domain, order_ids[0]
        return None

    observations: list[tuple[str, tuple[str, ...] | None, Decimal]] = []
    for item in items:
        values = _decimals([item], total_keys, {str(item.get('domain'))})
        if values:
            value = values[0] if len(set(values)) == 1 else sum(values, Decimal(0))
            observations.append((str(item.get('domain')), identity(item), value))
    if observations:
        if any(domain == 'order' for domain, _, _ in observations):
            observations = [row for row in observations if row[0] == 'order']
        identified = {key: value for _, key, value in observations if key is not None}
        if identified:
            return sum(identified.values(), Decimal(0))
        anonymous = list(dict.fromkeys(value for _, _, value in observations))
        return anonymous[0] if len(anonymous) == 1 else None

    wanted_lines = set(line_keys)
    lines: dict[tuple[tuple[str, ...] | None, str, str], Decimal] = {}

    def collect_lines(
        value: Any,
        item_identity: tuple[str, ...] | None,
        path: tuple[str, ...] = (),
    ) -> None:
        if isinstance(value, dict):
            record_identity = next((
                f'{key}:{value[key]}'
                for key in (
                    'order_item_id', 'item_id', 'payment_id', 'payment_reference',
                    'payment_sequence', 'payment_sequential', 'refund_id', 'shipment_id',
                )
                if isinstance(value.get(key), (str, int))
                and not isinstance(value.get(key), bool)
            ), None)
            scope = record_identity or '/'.join(path) or '$'
            for key, child in value.items():
                if key in wanted_lines and not isinstance(child, bool) and isinstance(
                    child, (str, int, float)
                ):
                    with suppress(InvalidOperation):
                        number = Decimal(str(child))
                        if number.is_finite():
                            lines[item_identity, scope, key] = number
                if isinstance(child, (dict, list)):
                    collect_lines(child, item_identity, (*path, key))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                collect_lines(child, item_identity, (*path, str(index)))

    for item in items:
        collect_lines(item.get('data'), identity(item))
    return sum(lines.values(), Decimal(0)) if lines else None


def _claim_topics(case: Mapping[str, Any]) -> set[str]:
    # ponytail: keywords route claims to evidence only; they never establish ground truth.
    text = json.dumps(case, ensure_ascii=False, default=str).lower()
    topics: set[str] = set()
    if any(token in text for token in ('refund', 'hoàn tiền', 'hoan tien')):
        topics.add('refund')
    if any(token in text for token in ('payment', 'charge', 'paid', 'thanh toán', 'thanh toan')):
        topics.add('payment')
    if any(token in text for token in ('delivery', 'shipment', 'late', 'giao hàng', 'giao hang')):
        topics.add('shipment')
    if any(token in text for token in ('order', 'cancel', 'unavailable', 'đơn hàng', 'don hang')):
        topics.add('order')
    return topics


def _payment_count(evidence: Iterable[dict[str, Any]]) -> int:
    identifiers = ('payment_id', 'payment_reference', 'payment_sequence', 'payment_sequential')
    return max(
        (len(_normalized_values(evidence, (key,), {'payment'})) for key in identifiers),
        default=0,
    )


def _identity_tokens(item: Mapping[str, Any]) -> set[tuple[str, str]]:
    tokens: set[tuple[str, str]] = set()
    for identity, keys in IDENTITY_KEYS.items():
        if identity == 'seller':
            continue
        for value in _values([item], keys):
            candidates = value if isinstance(value, list) else [value]
            tokens.update(
                (identity, str(candidate))
                for candidate in candidates
                if isinstance(candidate, (str, int)) and not isinstance(candidate, bool)
            )
    return tokens


def _evidence_scopes(
    case: Mapping[str, Any], evidence: list[dict[str, Any]]
) -> list[list[dict[str, Any]]]:
    multiple_roots = any(
        len(set(_ids(case, evidence, *keys))) > 1
        for keys in (
            ('order_id', 'order_ids'),
            ('refund_id', 'refund_ids'),
            ('shipment_id', 'shipment_ids'),
        )
    )
    if not multiple_roots:
        return [evidence]
    tokens = [_identity_tokens(item) for item in evidence]
    parent = list(range(len(evidence)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    for left in range(len(evidence)):
        for right in range(left):
            if tokens[left] & tokens[right]:
                parent[find(left)] = find(right)
    grouped: dict[int, list[int]] = {}
    for index in range(len(evidence)):
        grouped.setdefault(find(index), []).append(index)
    scopes = [
        [evidence[index] for index in indexes]
        for indexes in grouped.values()
        if any(tokens[index] for index in indexes)
    ]
    detached = [
        evidence[index]
        for indexes in grouped.values()
        if not any(tokens[index] for index in indexes)
        for index in indexes
    ]
    for item in detached:
        seller_ids = set(_ids({}, [item], 'seller_id', 'seller_ids'))
        targets = [
            scope
            for scope in scopes
            if seller_ids & set(_ids({}, scope, 'seller_id', 'seller_ids'))
        ] if item.get('domain') == 'seller' and seller_ids else []
        if targets:
            for scope in targets:
                scope.append(item)
        else:
            scopes.append([item])
    return scopes


def _assessment_scope(
    case: Mapping[str, Any], evidence: list[dict[str, Any]]
) -> tuple[
    str,
    str,
    float,
    list[dict[str, Any]],
    list[list[dict[str, Any]]],
]:
    facts = _domain_items(evidence, set().union(*ISSUE_DOMAINS.values()) - {'policy'})
    if not facts:
        return 'insufficient_evidence', 'needs_investigation', 0.0, [], []
    priorities = {issue: index for index, issue in enumerate(ISSUE_PRIORITY)}
    candidates = [
        (_assessment_facts(case, scope), scope)
        for scope in _evidence_scopes(case, facts)
    ]
    result, _ = min(
        candidates,
        key=lambda candidate: (
            priorities[candidate[0][0]],
            -len({item.get('domain') for item in candidate[1]}),
            -len(candidate[1]),
        ),
    )
    matching = [scope for candidate, scope in candidates if candidate[0] == result[0]]
    confidence = min(candidate[2] for candidate, _ in candidates if candidate[0] == result[0])
    merged = _unique_evidence(item for scope in matching for item in scope)
    return result[0], result[1], confidence, merged, matching


def _assessment(case: Mapping[str, Any], evidence: list[dict[str, Any]]) -> tuple[str, str, float]:
    return _assessment_scope(case, evidence)[:3]


def _order_window(
    evidence: Iterable[dict[str, Any]],
) -> tuple[datetime, datetime] | None:
    values = _values(evidence, ('order_purchase_timestamp',), {'order'})
    for value in values:
        with suppress(ValueError, TypeError):
            start = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            return start, start + timedelta(days=21)
    return None


def _current_events(
    case: Mapping[str, Any],
    evidence: Iterable[dict[str, Any]],
    domains: set[str],
) -> list[dict[str, Any]]:
    del case
    items = list(evidence)
    window = _order_window(items)
    result: list[dict[str, Any]] = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            if 'event_at' in value and 'event_type' in value:
                with suppress(ValueError, TypeError):
                    event_at = datetime.fromisoformat(str(value['event_at']).replace('Z', '+00:00'))
                    if window is None or window[0] <= event_at <= window[1]:
                        result.append(value)
                return
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    for item in _domain_items(items, domains):
        visit(item.get('data'))
    return list({
        (
            event.get('order_id'), event.get('event_at'), event.get('event_type'),
            event.get('amount_brl'), event.get('status'), event.get('actor'),
        ): event
        for event in result
    }.values())


def _current_item_total(
    case: Mapping[str, Any], evidence: Iterable[dict[str, Any]]
) -> Decimal | None:
    del case
    items = list(evidence)
    window = _order_window(items)
    lines: dict[str, tuple[timedelta, Decimal]] = {}

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            if 'price' in value and 'shipping_limit_date' in value:
                with suppress(ValueError, TypeError, InvalidOperation):
                    timestamp = datetime.fromisoformat(
                        str(value['shipping_limit_date']).replace('Z', '+00:00')
                    )
                    if window is None or window[0] <= timestamp <= window[1]:
                        amount = Decimal(str(value['price'])) + Decimal(
                            str(value.get('freight_value', 0))
                        )
                        key = str(value.get('order_item_id', len(lines)))
                        distance = abs(timestamp - window[0]) if window else timedelta()
                        if key not in lines or distance < lines[key][0]:
                            lines[key] = (distance, amount)
                return
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    for item in _domain_items(items, {'item'}):
        visit(item.get('data'))
    return sum((amount for _, amount in lines.values()), Decimal(0)) if lines else None


def _assessment_facts(
    case: Mapping[str, Any], evidence: list[dict[str, Any]]
) -> tuple[str, str, float]:
    facts = _domain_items(evidence, set().union(*ISSUE_DOMAINS.values()) - {'policy'})
    domains = {item.get('domain') for item in facts}
    if not facts:
        return 'insufficient_evidence', 'needs_investigation', 0.0

    order_status = _normalized_values(facts, ('order_status', 'status'), {'order'})
    payment_status = _normalized_values(facts, ('payment_status', 'status'), {'payment'})
    payment_events = _current_events(case, facts, {'payment'})
    refund_events = _current_events(case, facts, {'refund'})
    shipment_events = _current_events(case, facts, {'shipment'})
    refund_status = {
        str(event.get('status', '')).lower() for event in refund_events
    }
    if not refund_events and _order_window(facts) is None:
        refund_status = _normalized_values(
            facts, ('refund_status', 'refund_state', 'status'), {'refund'}
        ) | _normalized_values(facts, ('refund_status', 'refund_state'), {'payment'})
    shipment_status = _normalized_values(
        facts, ('shipment_status', 'delivery_status', 'status'), {'shipment'}
    )
    captured_values = [
        Decimal(str(event['amount_brl']))
        for event in payment_events
        if str(event.get('event_type', '')).lower() == 'captured'
        and event.get('amount_brl') is not None
    ]
    captured = sum(captured_values, Decimal(0)) if captured_values else None
    order_total = _current_item_total(case, facts)
    if captured is None:
        captured = _money_total(
            facts,
            ('captured_total_brl', 'paid_total', 'payment_total', 'total_paid_brl'),
            ('captured_amount', 'payment_value', 'amount_brl'),
            {'payment'},
        )
    if order_total is None:
        order_total = _money_total(
            facts,
            ('order_total_brl', 'order_total', 'order_value', 'total_amount', 'total_brl'),
            ('price', 'freight_value'),
            {'order', 'item'},
        )
    paid = captured is not None and captured > 0 or bool(
        payment_status & {'paid', 'captured', 'approved', 'settled', 'authorized'}
    )
    canceled = bool(order_status & {'canceled', 'cancelled'})
    item_status = _normalized_values(
        facts, ('item_status', 'availability_status', 'status'), {'item'}
    )
    unavailable = 'unavailable' in order_status or 'unavailable' in item_status

    if refund_status & {'failed', 'rejected', 'error'}:
        return 'refund_failed', 'action_required', 0.95
    if refund_status & {'pending', 'processing', 'awaiting'}:
        return 'refund_pending', 'action_required', 0.92
    payment_count = len(captured_values) or _payment_count(facts)
    capture_times = [
        datetime.fromisoformat(str(event['event_at']).replace('Z', '+00:00'))
        for event in payment_events
        if str(event.get('event_type', '')).lower() == 'captured'
    ]
    clustered_captures = (
        not capture_times or max(capture_times) - min(capture_times) <= timedelta(days=1)
    )
    duplicate = _flag(
        facts, ('duplicate_charge', 'duplicate_capture', 'is_duplicate'), {'payment'}
    ) or bool(payment_status & {'duplicate', 'duplicate_charge', 'duplicate_capture'})
    duplicate = duplicate or (
        payment_count > 1
        and clustered_captures
        and captured is not None
        and order_total is not None
        and captured > order_total + Decimal('0.01')
    )
    if canceled or unavailable:
        if 'payment' not in domains:
            return 'insufficient_evidence', 'needs_investigation', 0.30
        if paid:
            issue = 'canceled_order_paid' if canceled else 'unavailable_order_paid'
            return issue, 'action_required', 0.95
    if duplicate:
        return 'duplicate_charge', 'action_required', 0.96
    mismatch = any(
        str(event.get('event_type', '')).lower() in {
            'reconciliation_mismatch', 'payment_mismatch', 'capture_mismatch'
        }
        for event in payment_events
    ) or _flag(facts, ('payment_mismatch', 'capture_mismatch'), {'payment'})
    mismatch = mismatch or (not payment_events and
        captured is not None
        and order_total is not None
        and abs(captured - order_total) > Decimal('0.01')
    )
    if mismatch:
        return 'payment_mismatch', 'action_required', 0.92
    if _is_late(facts) or bool(shipment_status & {'late', 'delayed', 'overdue'}):
        seller_delay = _flag(
            facts,
            ('seller_delay', 'seller_late', 'late_by_seller'),
            {'order', 'item', 'seller', 'shipment'},
        )
        responsible = _normalized_values(
            facts, ('responsible_party', 'delay_owner'), {'order', 'item', 'seller', 'shipment'}
        )
        responsible |= {
            str(event.get('actor', '')).lower()
            for event in shipment_events
            if str(event.get('event_type', '')).lower() == 'delivered_late'
        }
        issue = (
            'late_delivery_seller' if 'seller' in responsible
            else 'late_delivery_logistics' if 'logistics_provider' in responsible
            else 'late_delivery_seller' if seller_delay or _seller_late(facts)
            else 'late_delivery_logistics'
        )
        return issue, 'action_required', 0.90
    split = _flag(facts, ('split_payment', 'is_split_payment'), {'payment'}) or payment_count > 1
    reconciled = order_total is None or abs(captured - order_total) <= Decimal('0.01')
    if split and captured is not None and reconciled:
        return 'valid_split_payment', 'no_action', 0.93

    topics = _claim_topics(case)
    missing = (
        ('payment' in topics and 'payment' not in domains)
        or ('refund' in topics and not domains.intersection({'payment', 'refund'}))
        or ('shipment' in topics and 'shipment' not in domains)
        or ('order' in topics and not domains.intersection({'order', 'item'}))
    )
    if missing:
        return 'insufficient_evidence', 'needs_investigation', 0.25
    return 'unsupported_claim', 'no_action', 0.80


def _calibrate_confidence(
    issue: str,
    proposed: float,
    evidence: Iterable[dict[str, Any]],
    conflicts: Iterable[dict[str, Any]],
) -> float:
    items = list(evidence)
    domains = {str(item.get('domain')) for item in items}
    score = Decimal(str(proposed)) if issue == 'insufficient_evidence' else Decimal('0.97')
    groups = REQUIRED_DOMAIN_GROUPS.get(issue, ())
    if groups:
        coverage = Decimal(sum(bool(domains & group) for group in groups)) / Decimal(len(groups))
        score = min(score, Decimal('0.55') + Decimal('0.42') * coverage)
    if issue not in {'insufficient_evidence'} and 'policy' not in domains:
        score = min(score, Decimal('0.75'))
    warning_count = sum(len(item.get('warnings', ())) for item in items)
    score -= min(Decimal(warning_count) * Decimal('0.02'), Decimal('0.08'))
    conflict_items = list(conflicts)
    score -= Decimal('0.05') * sum(
        conflict.get('selected_source') is not None for conflict in conflict_items
    )
    score -= Decimal('0.20') * sum(
        conflict.get('selected_source') is None for conflict in conflict_items
    )
    if issue == 'insufficient_evidence':
        score = min(score, Decimal('0.35'))
    return float(max(Decimal(0), min(score, Decimal('0.97'))).quantize(Decimal('0.01')))


def _is_late(evidence: Iterable[dict[str, Any]]) -> bool:
    dates: dict[str, datetime] = {}
    wanted = {
        'delivered_at',
        'delivery_date',
        'estimated_delivery_date',
        'promised_at',
        'order_delivered_customer_date',
        'order_estimated_delivery_date',
    }
    for key, value in (
        pair
        for item in _domain_items(evidence, {'order', 'shipment'})
        for pair in _walk(item.get('data'))
    ):
        if key in wanted:
            with suppress(ValueError):
                dates[key] = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    delivered = (
        dates.get('delivered_at')
        or dates.get('delivery_date')
        or dates.get('order_delivered_customer_date')
    )
    promised = (
        dates.get('estimated_delivery_date')
        or dates.get('promised_at')
        or dates.get('order_estimated_delivery_date')
    )
    try:
        return delivered is not None and promised is not None and delivered > promised
    except TypeError:
        return False


def _seller_late(evidence: Iterable[dict[str, Any]]) -> bool:
    carrier_dates: list[datetime] = []
    shipping_limits: list[datetime] = []
    for item in _domain_items(evidence, {'order', 'item', 'shipment'}):
        for key, value in _walk(item.get('data')):
            target = (
                carrier_dates
                if key in {'carrier_at', 'order_delivered_carrier_date'}
                else shipping_limits
                if key in {'shipping_limit_date', 'ship_by'}
                else None
            )
            if target is not None:
                with suppress(ValueError):
                    target.append(datetime.fromisoformat(str(value).replace('Z', '+00:00')))
    try:
        return bool(carrier_dates and shipping_limits and max(carrier_dates) > min(shipping_limits))
    except TypeError:
        return False


def _supporting_evidence(
    case: Mapping[str, Any], evidence: list[dict[str, Any]], issue: str
) -> list[dict[str, Any]]:
    domains = ISSUE_DOMAINS.get(issue)
    if domains is None:
        topics = _claim_topics(case)
        domains = {'policy'}
        if 'order' in topics:
            domains |= {'order', 'item'}
        if 'payment' in topics:
            domains.add('payment')
        if 'refund' in topics:
            domains |= {'payment', 'refund'}
        if 'shipment' in topics:
            domains.add('shipment')
        if domains == {'policy'}:
            domains |= {str(item.get('domain')) for item in evidence}
    selected = [
        item
        for item in _domain_items(evidence, domains)
        if _supports_issue(item, issue)
    ]
    return _unique_evidence(selected)[:30]


def _supports_issue(item: Mapping[str, Any], issue: str) -> bool:
    if item.get('domain') == 'policy' or issue in {'unsupported_claim', 'insufficient_evidence'}:
        return True
    keys = {key for key, _ in _walk(item.get('data'))}
    return bool(keys & ISSUE_SUPPORT_KEYS.get(issue, set()))


def _ids(case: Mapping[str, Any], evidence: Iterable[dict[str, Any]], *keys: str) -> list[str]:
    values: list[str] = []
    sources = [case, *(item.get('data') for item in evidence)]
    for source in sources:
        for key, value in _walk(source):
            if key not in keys:
                continue
            candidates = value if isinstance(value, list) else [value]
            values.extend(str(item) for item in candidates if isinstance(item, (str, int)))
    return list(dict.fromkeys(values))[:20]


def _strings(evidence: Iterable[dict[str, Any]], *keys: str) -> list[str]:
    found: list[str] = []
    for value in _values(evidence, keys):
        candidates = value if isinstance(value, list) else [value]
        found.extend(
            candidate
            for candidate in candidates
            if isinstance(candidate, str) and 1 <= len(candidate) <= 80
        )
    return list(dict.fromkeys(found))[:8]


def _data_conflicts(evidence: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    items = list(evidence)
    conflicts: list[dict[str, Any]] = []
    for item in items:
        for value in _values([item], ('data_conflicts', 'conflicts')):
            candidates = value if isinstance(value, list) else [value]
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue
                raw_sources = candidate.get('sources', [])
                if not isinstance(raw_sources, list):
                    continue
                sources = list(dict.fromkeys(str(source)[:80] for source in raw_sources))[:5]
                if len(sources) < 2:
                    continue
                selected = candidate.get('selected_source')
                selected = str(selected)[:80] if selected is not None else None
                if selected not in sources:
                    selected = None
                conflicts.append({
                    'field': str(candidate.get('field') or 'unknown')[:100],
                    'sources': sources,
                    'selected_source': selected,
                    'resolution_code': str(
                        candidate.get('resolution_code') or 'UNRESOLVED'
                    )[:80],
                })
    conflict_keys = {
        'order': {
            'status': ('order_status', 'status'),
            'total_brl': ('order_total_brl', 'order_total', 'order_value', 'total_brl'),
            'delivered_at': ('delivered_at', 'order_delivered_customer_date'),
            'estimated_at': ('estimated_delivery_date', 'order_estimated_delivery_date'),
        },
        'payment': {
            'status': ('payment_status', 'status'),
            'captured_brl': ('captured_total_brl', 'paid_total', 'payment_total'),
            'refunded_brl': ('refunded_total_brl',),
        },
        'refund': {
            'status': ('refund_status', 'refund_state', 'status'),
            'requested_brl': ('requested_refund_brl', 'refund_amount_brl', 'refund_total_brl'),
            'refunded_brl': ('refunded_total_brl',),
        },
        'shipment': {
            'status': ('shipment_status', 'delivery_status', 'status'),
            'delivered_at': ('delivered_at', 'delivery_date'),
            'estimated_at': ('estimated_delivery_date', 'promised_at'),
        },
        'policy': {
            'refund_brl': (
                'recommended_refund_brl', 'refundable_total_brl', 'refund_amount', 'refund_brl'
            ),
        },
    }

    def conflict_values(item: dict[str, Any], keys: tuple[str, ...]) -> set[str]:
        normalized: set[str] = set()
        for value in _values([item], keys, {str(item.get('domain'))}):
            if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                continue
            try:
                number = Decimal(str(value))
            except InvalidOperation:
                normalized.add(str(value).strip().lower().replace(' ', '_'))
            else:
                if number.is_finite():
                    normalized.add(str(number.normalize()))
        return normalized

    observations: dict[tuple[str, str, str], list[tuple[str, str]]] = {}
    for item in items:
        domain = str(item.get('domain'))
        if domain not in conflict_keys:
            continue
        if domain == 'policy':
            markers = _normalized_values(
                [item], ('issue', 'primary_issue', 'policy_code', 'policy_name'), {'policy'}
            )
            identity = next(iter(markers), 'scoped_policy') if len(markers) <= 1 else None
        else:
            identity = next((
                values[0]
                for key in IDENTITY_KEYS[domain]
                if len(values := _ids({}, [item], key)) == 1
            ), None)
        if identity is None:
            continue
        evidence_ref = item['evidence_ref']
        source = f'{domain}:{evidence_ref}'[:80]
        for field, keys in conflict_keys[domain].items():
            values = conflict_values(item, keys)
            if len(values) == 1:
                observations.setdefault((domain, identity, field), []).append(
                    (source, next(iter(values)))
                )
    for (domain, entity_id, field), values in observations.items():
        sources = list(dict.fromkeys(source for source, _ in values))[:5]
        if len(sources) >= 2 and len({value for _, value in values}) > 1:
            conflicts.append({
                'field': f'{domain}_{field}:{entity_id}'[:100],
                'sources': sources,
                'selected_source': None,
                'resolution_code': 'UNRESOLVED_AUTHORITATIVE_CONFLICT',
            })
    unique = {
        (item['field'], tuple(item['sources'])): item
        for item in conflicts
    }
    return sorted(
        unique.values(),
        key=lambda conflict: conflict['selected_source'] is not None,
    )[:5]


def _policy_refund_lines(evidence: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    for value in _values(_domain_items(evidence, {'policy'}), ('refund_lines',)):
        if not isinstance(value, list):
            continue
        lines: list[dict[str, Any]] = []
        for raw in value[:10]:
            if not isinstance(raw, dict):
                continue
            reason = raw.get('reason_code')
            entity_id = raw.get('entity_id')
            try:
                amount = Decimal(str(raw.get('amount_brl'))).quantize(Decimal('0.01'))
            except (InvalidOperation, TypeError):
                continue
            if (
                not isinstance(reason, str)
                or not 1 <= len(reason) <= 80
                or not amount.is_finite()
                or amount < 0
                or entity_id is not None
                and (not isinstance(entity_id, str) or len(entity_id) > 128)
            ):
                continue
            lines.append({
                'reason_code': reason,
                'amount_brl': float(amount),
                'entity_id': entity_id,
            })
        if lines:
            return lines
    return []


def _matching_policy(evidence: Iterable[dict[str, Any]], issue: str) -> list[dict[str, Any]]:
    matching: list[dict[str, Any]] = []
    for item in _domain_items(evidence, {'policy'}):
        data = item.get('data')
        rules = data.get('rules') if isinstance(data, dict) else None
        rule = rules.get(issue) if isinstance(rules, dict) else None
        if isinstance(rule, dict):
            matching.append({
                **item,
                'data': {
                    'policy_version': data.get('policy_version'),
                    'currency': data.get('currency'),
                    'issue': issue,
                    **rule,
                },
            })
            continue
        markers = _normalized_values(
            [item], ('issue', 'primary_issue', 'policy_code', 'policy_name'), {'policy'}
        )
        if not markers or issue in markers:
            matching.append(item)
    return matching


def _policy_actions(evidence: Iterable[dict[str, Any]], issue: str) -> list[str]:
    actions = _strings(
        _matching_policy(evidence, issue),
        'resolution_actions', 'recommended_actions', 'recommended_action',
        'action_code', 'action',
    )
    owners: dict[str, set[str]] = {}
    for owner, issue_actions in FALLBACK_ACTIONS.items():
        for action in issue_actions:
            owners.setdefault(action, set()).add(owner)
    return [action for action in actions if action not in owners or issue in owners[action]]


def _responsible_parties(
    issue: str, case: Mapping[str, Any], evidence: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    party_type = PARTY_BY_ISSUE.get(issue, 'unknown')
    policy_parties: list[dict[str, Any]] = []
    for value in _values(_matching_policy(evidence, issue), ('responsible_parties',)):
        candidates = value if isinstance(value, list) else [value]
        for candidate in candidates:
            if not isinstance(candidate, dict) or candidate.get('party_type') != party_type:
                continue
            party_id = candidate.get('party_id')
            if party_id is None or isinstance(party_id, str) and len(party_id) <= 128:
                policy_parties.append({'party_type': party_type, 'party_id': party_id})
    if policy_parties:
        unique = {(party['party_type'], party['party_id']): party for party in policy_parties}
        return list(unique.values())[:5]
    id_keys = {
        'seller': ('seller_id', 'seller_ids'),
        'platform': ('platform_id', 'platform_ids'),
        'logistics_provider': ('logistics_provider_id', 'carrier_id'),
        'payment_provider': ('payment_provider_id', 'acquirer_id'),
        'customer': ('customer_id', 'customer_ids'),
    }.get(party_type, ())
    party_ids = _ids(case, evidence, *id_keys)[:5] if id_keys else []
    return [
        {'party_type': party_type, 'party_id': party_id}
        for party_id in party_ids
    ] or [{'party_type': party_type, 'party_id': None}]


def _financially_complete(issue: str, evidence: list[dict[str, Any]]) -> bool:
    policy_evidence = _matching_policy(evidence, issue)
    policy_refund = _money_total(
        policy_evidence,
        ('recommended_refund_brl', 'refundable_total_brl', 'refund_amount', 'refund_brl'),
        (),
        {'policy'},
    )
    if (
        policy_refund is not None and policy_refund >= 0
        or _policy_refund_lines(policy_evidence)
    ):
        return True
    captured = _money_total(
        evidence,
        ('captured_total_brl', 'paid_total', 'payment_total', 'total_paid_brl'),
        ('captured_amount', 'payment_value', 'amount_brl'),
        {'payment'},
    )
    order_total = _money_total(
        evidence,
        ('order_total_brl', 'order_total', 'order_value', 'total_amount', 'total_brl'),
        ('price', 'freight_value'),
        {'order', 'item'},
    )
    requested = _money_total(
        evidence,
        ('requested_refund_brl', 'refund_amount_brl', 'refund_total_brl'),
        ('refund_amount', 'amount_brl'),
        {'refund'},
    )
    duplicate_amount = _money_total(
        evidence,
        ('duplicate_amount_brl', 'duplicate_charge_amount_brl'),
        (),
        {'payment'},
    )
    if issue in {'canceled_order_paid', 'unavailable_order_paid'}:
        return captured is not None and captured > 0
    if issue == 'duplicate_charge':
        return duplicate_amount is not None and duplicate_amount >= 0 or (
            captured is not None and order_total is not None
        )
    if issue in {'refund_pending', 'refund_failed'}:
        return requested is not None and requested >= 0 or captured is not None
    if issue == 'payment_mismatch':
        return captured is not None and order_total is not None
    return True


def _build_output(
    case: dict[str, Any],
    evidence: list[dict[str, Any]],
    issue: str,
    status: str,
    confidence: float,
    conflicts: list[dict[str, Any]],
) -> dict[str, Any]:
    refs = _refs(evidence)
    seller_ids = _ids(case, evidence, 'seller_id', 'seller_ids')
    policy_evidence = _matching_policy(evidence, issue)
    policy_refund = _money_total(
        policy_evidence,
        ('recommended_refund_brl', 'refundable_total_brl', 'refund_amount', 'refund_brl'),
        (),
        {'policy'},
    )
    policy_lines = _policy_refund_lines(policy_evidence)
    policy_line_total = sum(
        (Decimal(str(line['amount_brl'])) for line in policy_lines), Decimal(0)
    )
    captured = _money_total(
        evidence,
        ('captured_total_brl', 'paid_total', 'payment_total', 'total_paid_brl'),
        ('captured_amount', 'payment_value', 'amount_brl'),
        {'payment'},
    ) or Decimal(0)
    refunded = _money_total(
        evidence, ('refunded_total_brl',), ('refunded_amount',), {'payment', 'refund'}
    ) or Decimal(0)
    requested_refund = _money_total(
        evidence,
        ('requested_refund_brl', 'refund_amount_brl', 'refund_total_brl'),
        ('refund_amount', 'amount_brl'),
        {'refund'},
    )
    order_total = _money_total(
        evidence,
        ('order_total_brl', 'order_total', 'order_value', 'total_amount', 'total_brl'),
        ('price', 'freight_value'),
        {'order', 'item'},
    ) or Decimal(0)
    duplicate_amount = _money_total(
        evidence,
        ('duplicate_amount_brl', 'duplicate_charge_amount_brl'),
        (),
        {'payment'},
    )
    if status != 'action_required':
        refund = Decimal(0)
    elif policy_refund is not None and policy_refund >= 0:
        refund = policy_refund
    elif policy_lines:
        refund = policy_line_total
    elif issue == 'duplicate_charge':
        refund = (
            max(duplicate_amount, Decimal(0))
            if duplicate_amount is not None
            else max(captured - order_total, Decimal(0))
        )
    elif (
        issue in {'refund_pending', 'refund_failed'}
        and requested_refund is not None
        and requested_refund >= 0
    ):
        refund = requested_refund
    elif issue in {'canceled_order_paid', 'unavailable_order_paid', 'refund_pending',
                   'refund_failed'}:
        refund = max(captured - refunded, Decimal(0))
    elif issue == 'payment_mismatch':
        refund = max(captured - order_total, Decimal(0))
    else:
        refund = Decimal(0)
    calculated_lines: list[dict[str, Any]] = []
    if status == 'action_required' and policy_refund is None and not policy_lines:
        fact_domains = set().union(*ISSUE_DOMAINS.values()) - {'policy'}
        scopes = _evidence_scopes(case, _domain_items(evidence, fact_domains))
        for scope in scopes:
            scope_captured = _money_total(
                scope,
                ('captured_total_brl', 'paid_total', 'payment_total', 'total_paid_brl'),
                ('captured_amount', 'payment_value', 'amount_brl'),
                {'payment'},
            ) or Decimal(0)
            scope_refunded = _money_total(
                scope, ('refunded_total_brl',), ('refunded_amount',), {'payment', 'refund'}
            ) or Decimal(0)
            scope_order_total = _money_total(
                scope,
                ('order_total_brl', 'order_total', 'order_value', 'total_amount', 'total_brl'),
                ('price', 'freight_value'),
                {'order', 'item'},
            ) or Decimal(0)
            scope_requested = _money_total(
                scope,
                ('requested_refund_brl', 'refund_amount_brl', 'refund_total_brl'),
                ('refund_amount', 'amount_brl'),
                {'refund'},
            )
            scope_duplicate = _money_total(
                scope,
                ('duplicate_amount_brl', 'duplicate_charge_amount_brl'),
                (),
                {'payment'},
            )
            scope_amount = (
                max(scope_duplicate, Decimal(0))
                if issue == 'duplicate_charge' and scope_duplicate is not None
                else max(scope_captured - scope_order_total, Decimal(0))
                if issue in {'duplicate_charge', 'payment_mismatch'}
                else max(scope_captured - scope_refunded, Decimal(0))
                if issue in {'canceled_order_paid', 'unavailable_order_paid'}
                else max(scope_requested, Decimal(0))
                if issue in {'refund_pending', 'refund_failed'}
                and scope_requested is not None
                else max(scope_captured - scope_refunded, Decimal(0))
                if issue in {'refund_pending', 'refund_failed'}
                else Decimal(0)
            )
            scope_order_ids = _ids({}, scope, 'order_id', 'order_ids')
            scope_refund_ids = _ids({}, scope, 'refund_id', 'refund_ids')
            if scope_amount > 0:
                calculated_lines.append({
                    'reason_code': issue.upper(),
                    'amount_brl': float(scope_amount.quantize(Decimal('0.01'))),
                    'entity_id': (
                        scope_refund_ids[0]
                        if len(scope_refund_ids) == 1
                        else scope_order_ids[0]
                        if len(scope_order_ids) == 1
                        else None
                    ),
                })
        if len(calculated_lines) > 1:
            refund = sum(
                (Decimal(str(line['amount_brl'])) for line in calculated_lines), Decimal(0)
            )
    order_ids = _ids(case, evidence, 'order_id', 'order_ids')
    amount = float(refund.quantize(Decimal('0.01')))
    refund_lines = (
        policy_lines
        if policy_lines and policy_line_total == Decimal(str(amount))
        else calculated_lines
        if len(calculated_lines) > 1
        else ([{
            'reason_code': issue.upper(),
            'amount_brl': amount,
            'entity_id': (
                _ids(case, evidence, 'refund_id', 'refund_ids') or order_ids or [None]
            )[0],
        }] if amount else [])
    )
    policy_actions = _policy_actions(evidence, issue)
    responsible_parties = _responsible_parties(issue, case, evidence)
    if PARTY_BY_ISSUE.get(issue) == 'seller' and seller_ids:
        responsible_parties = [
            {'party_type': 'seller', 'party_id': seller_id}
            for seller_id in seller_ids[:5]
        ]
    output: dict[str, Any] = {
        'schema_version': 'day09-l3a-output-v2',
        'case_id': case['case_id'],
        'assessment': {
            'primary_issue': issue,
            'case_status': status,
            'confidence': confidence,
        },
        'affected_entities': {
            'order_ids': order_ids,
            'item_ids': _ids(case, evidence, 'item_id', 'item_ids', 'order_item_id'),
            'seller_ids': seller_ids,
            'payment_references': _ids(
                case, evidence, 'payment_reference', 'payment_references', 'payment_id'
            ),
            'shipment_ids': _ids(case, evidence, 'shipment_id', 'shipment_ids'),
        },
        'root_cause_analysis': {
            'ranked_causes': [{'cause_code': issue.upper(), 'rank': 1}],
            'responsible_parties': responsible_parties,
        },
        'evidence_refs': refs,
        'data_conflicts': conflicts,
        'financial_resolution': {
            'currency': 'BRL',
            'recommended_refund_brl': amount,
            'refund_lines': refund_lines,
        },
        'resolution_actions': (
            FALLBACK_ACTIONS['insufficient_evidence']
            if issue == 'insufficient_evidence'
            else policy_actions or FALLBACK_ACTIONS.get(issue, [])
        ),
    }
    return output


def _verify_output(
    case: Mapping[str, Any],
    evidence: list[dict[str, Any]],
    output: dict[str, Any],
    trace: TraceWriter,
    gateway: EvidenceGateway | None = None,
) -> None:
    label = 'solver output for ' + str(case.get('case_id'))
    trace.contracts.validate_output(output, label)
    if output['case_id'] != case.get('case_id'):
        raise ValueError('verifier: case_id mismatch')
    available_refs = set(_refs(evidence))
    output_refs = output['evidence_refs']
    submitted_refs = set(output_refs)
    if not submitted_refs <= available_refs:
        raise ValueError('verifier: output contains unknown evidence refs')
    scope_checker = getattr(gateway, 'assert_evidence_scope', None) if gateway else None
    if scope_checker:
        scope_checker(str(case['case_id']), submitted_refs)
    if output['assessment']['primary_issue'] != 'insufficient_evidence' and not output_refs:
        raise ValueError('verifier: conclusive output requires evidence')
    cited_evidence = [item for item in evidence if item['evidence_ref'] in set(output_refs)]

    entity_keys = {
        'order_ids': ('order_id', 'order_ids'),
        'item_ids': ('item_id', 'item_ids', 'order_item_id'),
        'seller_ids': ('seller_id', 'seller_ids'),
        'payment_references': ('payment_reference', 'payment_references', 'payment_id'),
        'shipment_ids': ('shipment_id', 'shipment_ids'),
    }
    for output_key, source_keys in entity_keys.items():
        allowed = set(_ids(case, evidence, *source_keys))
        if not set(output['affected_entities'][output_key]) <= allowed:
            raise ValueError(f'verifier: {output_key} contains an out-of-scope entity')

    financial = output['financial_resolution']
    line_total = sum(
        (Decimal(str(line['amount_brl'])) for line in financial['refund_lines']), Decimal(0)
    )
    recommended = Decimal(str(financial['recommended_refund_brl']))
    if line_total != recommended:
        raise ValueError('verifier: refund lines do not equal recommended refund')
    entity_ids = set(_ids(
        case,
        evidence,
        'order_id', 'order_ids', 'item_id', 'item_ids', 'order_item_id',
        'seller_id', 'seller_ids', 'payment_reference', 'payment_references',
        'payment_id', 'shipment_id', 'shipment_ids', 'refund_id', 'refund_ids',
        'entity_id',
    ))
    if any(
        line['entity_id'] is not None and line['entity_id'] not in entity_ids
        for line in financial['refund_lines']
    ):
        raise ValueError('verifier: refund line contains an out-of-scope entity')
    issue = output['assessment']['primary_issue']
    status = output['assessment']['case_status']
    policy_status = _strings(_matching_policy(cited_evidence, issue), 'case_status')
    expected_status = policy_status[0] if policy_status else (
        'action_required' if issue in ACTION_ISSUES
        else 'needs_investigation' if issue == 'insufficient_evidence'
        else 'no_action'
    )
    if status != expected_status:
        raise ValueError('verifier: issue and case status are inconsistent')
    if status == 'no_action' and recommended != 0:
        raise ValueError('verifier: no-action case cannot recommend a refund')
    if status == 'action_required' and not output['resolution_actions']:
        raise ValueError('verifier: action-required case has no resolution action')
    if issue != 'insufficient_evidence' and not _financially_complete(issue, cited_evidence):
        raise ValueError('verifier: conclusion lacks complete financial evidence')
    expected = _build_output(
        dict(case),
        cited_evidence,
        issue,
        status,
        output['assessment']['confidence'],
        output['data_conflicts'],
    )
    if financial != expected['financial_resolution']:
        raise ValueError('verifier: financial resolution is inconsistent with evidence')
    if output['resolution_actions'] != expected['resolution_actions']:
        raise ValueError('verifier: resolution actions are inconsistent with policy')
    if output['root_cause_analysis']['responsible_parties'] != (
        expected['root_cause_analysis']['responsible_parties']
    ):
        raise ValueError('verifier: responsible parties are inconsistent with policy')
    cited_domains = {
        item['domain'] for item in cited_evidence
    }
    if any(
        not cited_domains.intersection(group)
        for group in REQUIRED_DOMAIN_GROUPS.get(issue, ())
    ):
        raise ValueError('verifier: conclusion is missing a required evidence domain')
    expected_party = PARTY_BY_ISSUE.get(issue, 'unknown')
    parties = output['root_cause_analysis']['responsible_parties']
    if not parties or {party['party_type'] for party in parties} != {expected_party}:
        raise ValueError('verifier: issue and responsible party are inconsistent')
    party_pairs = {(party['party_type'], party['party_id']) for party in parties}
    if len(party_pairs) != len(parties):
        raise ValueError('verifier: duplicate responsible party')
    if expected_party == 'seller' and any(
        party['party_id'] is not None
        and party['party_id'] not in output['affected_entities']['seller_ids']
        for party in parties
    ):
        raise ValueError('verifier: seller responsibility is outside affected entities')
    causes = output['root_cause_analysis']['ranked_causes']
    if not causes or causes[0]['cause_code'] != issue.upper():
        raise ValueError('verifier: primary issue and root cause are inconsistent')
    for conflict in output['data_conflicts']:
        selected = conflict['selected_source']
        if selected is not None and selected not in conflict['sources']:
            raise ValueError('verifier: conflict selected_source is not a source')
        if selected is None and issue != 'insufficient_evidence':
            raise ValueError('verifier: unresolved conflict requires investigation')
    confidence_ceiling = _calibrate_confidence(issue, 1.0, evidence, output['data_conflicts'])
    if output['assessment']['confidence'] > confidence_ceiling:
        raise ValueError('verifier: confidence exceeds evidence quality ceiling')
    _verify_trace_prefix(trace, str(case['case_id']), set(output_refs))


def _verify_trace_prefix(trace: TraceWriter, case_id: str, output_refs: set[str]) -> None:
    events: list[dict[str, Any]] = []
    if trace.path.exists():
        for line in trace.path.read_text(encoding='utf-8').splitlines():
            if not line.strip():
                continue
            event = json.loads(line)
            if event.get('case_id') == case_id:
                events.append(event)
    specialists = {'order-item-agent', 'payment-agent', 'shipment-agent'}
    for event in events:
        event_type = event['event_type']
        if event_type in {'case_received', 'task_assigned'} and event.get('actor') != 'coordinator':
            raise ValueError(f'verifier: invalid actor for {event_type}')
        if event_type == 'task_assigned' and event.get('target') not in (
            specialists | {'policy-agent'}
        ):
            raise ValueError('verifier: invalid task assignment target')
        if event_type == 'tool_result_consumed' and (
            event.get('actor') not in specialists | {'policy-agent'}
            or not event.get('tool_name')
            or not event.get('evidence_refs')
        ):
            raise ValueError('verifier: invalid tool consumption event')
        if event_type == 'handoff':
            actor, target = event.get('actor'), event.get('target')
            if not (
                actor in specialists and target == 'policy-agent'
                or actor == 'policy-agent' and target == 'verifier-agent'
            ):
                raise ValueError('verifier: invalid handoff')
        if event_type == 'policy_decided' and event.get('actor') != 'policy-agent':
            raise ValueError('verifier: invalid policy actor')
    event_types = [event['event_type'] for event in events]
    required = ('case_received', 'task_assigned', 'handoff', 'policy_decided')
    if any(event_type not in event_types for event_type in required):
        raise ValueError('verifier: trace lifecycle is incomplete')
    positions = [event_types.index(event_type) for event_type in required]
    if positions != sorted(positions):
        raise ValueError('verifier: trace lifecycle is out of order')
    assigned = {
        event.get('target') for event in events if event['event_type'] == 'task_assigned'
    }
    if not (specialists | {'policy-agent'}) <= assigned:
        raise ValueError('verifier: specialist/policy assignments are incomplete')
    handoffs = {
        (event.get('actor'), event.get('target'))
        for event in events
        if event['event_type'] == 'handoff'
    }
    required_handoffs = {
        *((actor, 'policy-agent') for actor in specialists),
        ('policy-agent', 'verifier-agent'),
    }
    if not required_handoffs <= handoffs or event_types.count('policy_decided') != 1:
        raise ValueError('verifier: collaboration lifecycle is incomplete')
    consumed = {
        ref
        for event in events
        if event['event_type'] == 'tool_result_consumed'
        for ref in event.get('evidence_refs', [])
    }
    if not output_refs <= consumed:
        raise ValueError('verifier: output evidence was not consumed in trace')
