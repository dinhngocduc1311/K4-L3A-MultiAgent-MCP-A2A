from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from student_agent.cases import CaseSet
from student_agent.contracts import Contracts
from student_agent.mcp_gateway import EvidenceGateway, is_transient_mcp_error
from student_agent.submission import _validate_case_lifecycle, validate_artifacts
from student_agent.trace import TraceWriter
from student_agent.workflow import (
    _assessment,
    _assessment_scope,
    _build_output,
    _calibrate_confidence,
    _call_with_retry,
    _data_conflicts,
    _evidence_scopes,
    _financially_complete,
    _tool_argument_sets,
    _tool_arguments,
    _verify_output,
    solve_case,
)


class FakeGateway:
    def __init__(self) -> None:
        self.refs: set[str] = set()
        self.scope_checks = 0

    async def list_tools(self) -> list[str]:
        return ['get_order', 'get_payment', 'get_shipment', 'get_policy']

    async def tool_specs(self) -> dict[str, dict[str, Any]]:
        by_order = {
            'type': 'object',
            'properties': {'case_id': {'type': 'string'}, 'order_id': {'type': 'string'}},
            'required': ['case_id', 'order_id'],
        }
        schemas = {
            'get_order': by_order,
            'get_payment': by_order,
            'get_shipment': by_order,
            'get_policy': {
                'type': 'object',
                'properties': {'case_id': {'type': 'string'}, 'issue': {'type': 'string'}},
                'required': ['case_id', 'issue'],
            },
        }
        return {
            name: {'description': name, 'input_schema': schema, 'output_schema': {}}
            for name, schema in schemas.items()
        }

    async def call(self, tool_name: str, *, case_id: str, **arguments: Any) -> dict[str, Any]:
        assert case_id == 'CASE_001'
        data = {
            'get_order': {'order_id': arguments.get('order_id'), 'status': 'canceled'},
            'get_payment': {
                'payment_reference': 'pay_1',
                'status': 'approved',
                'captured_total_brl': 100,
            },
            'get_shipment': {'shipment_id': 'ship_1', 'status': 'not_dispatched'},
            'get_policy': {'issue': arguments.get('issue'), 'refund_allowed': True},
        }[tool_name]
        domains = {
            'get_order': 'order',
            'get_payment': 'payment',
            'get_shipment': 'shipment',
            'get_policy': 'policy',
        }
        suffix = tool_name.removeprefix('get_').ljust(20, 'x')
        evidence = {'evidence_ref': f'ev_{suffix}', 'domain': domains[tool_name], 'data': data}
        self.refs.add(evidence['evidence_ref'])
        return evidence

    def assert_evidence_scope(self, case_id: str, evidence_refs: list[str]) -> None:
        assert case_id == 'CASE_001'
        assert set(evidence_refs) <= self.refs
        self.scope_checks += 1


def test_workflow_routes_evidence_and_returns_contract_output(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    contracts = Contracts(root / 'contracts' / 'schemas')
    trace_path = tmp_path / 'traces' / 'trace.jsonl'
    trace = TraceWriter(trace_path, contracts)
    case = {
        'case_id': 'CASE_001',
        'order_id': 'order_1',
        'customer_request': {
            'claims': [{'claim_id': 'claim-001-a', 'topic': 'canceled_order_paid'}]
        },
    }
    trace.emit(case_id='CASE_001', event_type='case_received', actor='coordinator')

    gateway = FakeGateway()
    output = asyncio.run(solve_case(case, gateway, trace))
    trace.emit(case_id='CASE_001', event_type='case_finalized', actor='coordinator')
    output_path = tmp_path / 'outputs' / 'CASE_001.json'
    output_path.parent.mkdir()
    output_path.write_text(json.dumps(output), encoding='utf-8')

    contracts.validate_output(output, 'test output')
    assert output['assessment']['primary_issue'] == 'canceled_order_paid'
    assert output['financial_resolution']['recommended_refund_brl'] == 100
    assert gateway.scope_checks == 1
    trace_events = [json.loads(line) for line in trace_path.read_text().splitlines()]
    events = [event['event_type'] for event in trace_events]
    consumed = [event for event in trace_events if event['event_type'] == 'tool_result_consumed']
    assert len(consumed) == 3
    assert len(output['evidence_refs']) == 3
    assert all('shipment' not in evidence_ref for evidence_ref in output['evidence_refs'])
    assert {'task_assigned', 'handoff', 'tool_result_consumed', 'policy_decided',
            'verification_completed', 'case_finalized'} <= set(events)
    artifacts, normalized_trace = validate_artifacts(
        tmp_path,
        CaseSet('test-v1', 'l3a', ('CASE_001',), {'CASE_001': case}),
        contracts,
    )
    assert artifacts['CASE_001'] == output
    assert len(normalized_trace) == len(trace_events)


def test_wrong_specialist_domain_is_not_consumed(tmp_path: Path) -> None:
    class WrongDomainGateway(FakeGateway):
        async def call(
            self, tool_name: str, *, case_id: str, **arguments: Any
        ) -> dict[str, Any]:
            evidence = await super().call(tool_name, case_id=case_id, **arguments)
            if tool_name == 'get_order':
                evidence['domain'] = 'payment'
            return evidence

    root = Path(__file__).resolve().parents[1]
    trace_path = tmp_path / 'trace.jsonl'
    trace = TraceWriter(trace_path, Contracts(root / 'contracts' / 'schemas'))
    trace.emit(case_id='CASE_001', event_type='case_received', actor='coordinator')
    with pytest.raises(ValueError, match='forbidden evidence domain'):
        asyncio.run(
            solve_case(
                {'case_id': 'CASE_001', 'order_id': 'order_1'},
                WrongDomainGateway(),
                trace,
            )
        )
    events = [json.loads(line)['event_type'] for line in trace_path.read_text().splitlines()]
    assert 'tool_result_consumed' not in events


def test_missing_refund_amount_downgrades_to_investigation(tmp_path: Path) -> None:
    class MissingAmountGateway(FakeGateway):
        async def call(
            self, tool_name: str, *, case_id: str, **arguments: Any
        ) -> dict[str, Any]:
            evidence = await super().call(tool_name, case_id=case_id, **arguments)
            if tool_name == 'get_payment':
                evidence['data'].pop('captured_total_brl')
            return evidence

    root = Path(__file__).resolve().parents[1]
    trace = TraceWriter(
        tmp_path / 'trace.jsonl', Contracts(root / 'contracts' / 'schemas')
    )
    trace.emit(case_id='CASE_001', event_type='case_received', actor='coordinator')
    output = asyncio.run(
        solve_case(
            {'case_id': 'CASE_001', 'order_id': 'order_1'},
            MissingAmountGateway(),
            trace,
        )
    )
    assert output['assessment'] == {
        'primary_issue': 'insufficient_evidence',
        'case_status': 'needs_investigation',
        'confidence': 0.3,
    }
    assert output['financial_resolution']['recommended_refund_brl'] == 0
    assert output['resolution_actions'] == ['MANUAL_INVESTIGATION']


def test_gateway_caches_discovery_and_rejects_cross_case_ref() -> None:
    class FakeSession:
        list_calls = 0
        call_calls = 0

        async def list_tools(self) -> Any:
            self.list_calls += 1
            tool = SimpleNamespace(
                name='get_order',
                description='authoritative order evidence',
                inputSchema={
                    'type': 'object',
                    'properties': {
                        'case_id': {'type': 'string'},
                        'order_id': {'type': 'string'},
                    },
                    'required': ['case_id', 'order_id'],
                    'additionalProperties': False,
                },
                outputSchema={},
            )
            return SimpleNamespace(tools=[tool])

        async def call_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
            self.call_calls += 1
            assert tool_name == 'get_order'
            assert arguments['case_id'] in {'CASE_001', 'CASE_002'}
            evidence = {
                'schema_version': 'day09-mcp-evidence-v1',
                'evidence_ref': 'ev_' + 'authoritative_order'.ljust(20, 'x'),
                'result_hash': 'sha256:' + 'a' * 64,
                'domain': 'order',
                'data': {'order_id': arguments['order_id']},
            }
            return SimpleNamespace(is_error=False, structuredContent=evidence, content=[])

    async def exercise() -> None:
        root = Path(__file__).resolve().parents[1]
        session = FakeSession()
        gateway = EvidenceGateway(session, Contracts(root / 'contracts' / 'schemas'))
        assert await gateway.list_tools() == ['get_order']
        assert await gateway.list_tools() == ['get_order']
        assert session.list_calls == 1
        with pytest.raises(ValueError, match='required'):
            await gateway.call('get_order', case_id='CASE_001')
        assert session.call_calls == 0
        evidence = await gateway.call('get_order', case_id='CASE_001', order_id='order_1')
        gateway.assert_evidence_scope('CASE_001', [evidence['evidence_ref']])
        with pytest.raises(ValueError, match='reused across case'):
            await gateway.call('get_order', case_id='CASE_002', order_id='order_2')

    asyncio.run(exercise())


def test_nested_mcp_connection_failure_is_retryable() -> None:
    failure = ExceptionGroup('transport failed', [RuntimeError('Connection closed')])
    assert is_transient_mcp_error(failure)
    assert not is_transient_mcp_error(ValueError('invalid evidence schema'))


def _evidence(domain: str, data: dict[str, Any], suffix: str) -> dict[str, Any]:
    return {
        'evidence_ref': 'ev_' + suffix.ljust(20, 'x'),
        'domain': domain,
        'data': data,
    }


def test_assessment_uses_structured_status_and_completeness() -> None:
    claim = {'case_id': 'CASE_001', 'message': 'paid canceled order delivered late'}
    not_paid = [
        _evidence('order', {'order_status': 'canceled'}, 'order'),
        _evidence('payment', {'payment_status': 'not_paid'}, 'payment'),
        _evidence('shipment', {'delivery_status': 'not_delayed'}, 'shipment'),
    ]
    assert _assessment(claim, not_paid)[0] == 'unsupported_claim'
    assert _assessment(claim, not_paid[:1])[0] == 'insufficient_evidence'

    split = [
        _evidence('order', {'order_total_brl': 100}, 'order'),
        _evidence(
            'payment',
            {'payments': [
                {'payment_sequence': 1, 'payment_value': 40},
                {'payment_sequence': 2, 'payment_value': 60},
            ]},
            'payment',
        ),
    ]
    assert _assessment({'case_id': 'CASE_001', 'message': 'payment issue'}, split)[0] == (
        'valid_split_payment'
    )


def test_single_payment_identifiers_are_not_mistaken_for_split_payment() -> None:
    evidence = [
        _evidence('order', {'order_total_brl': 10}, 'order'),
        _evidence('payment', {
            'payment_id': 'PAY_1',
            'payment_sequence': 1,
            'captured_total_brl': 10,
        }, 'payment'),
    ]
    assert _assessment({'case_id': 'CASE_001', 'message': 'payment issue'}, evidence)[0] == (
        'unsupported_claim'
    )


def test_policy_financial_actions_and_confidence_are_calibrated() -> None:
    no_action_evidence = [
        _evidence('order', {'order_id': 'ORDER_1', 'order_total_brl': 10}, 'order'),
        _evidence('payment', {'captured_total_brl': 10}, 'payment'),
        _evidence('policy', {
            'recommended_refund_brl': 99,
            'action': 'ISSUE_REFUND',
        }, 'policy'),
    ]
    no_action = _build_output(
        {'case_id': 'CASE_001'}, no_action_evidence,
        'valid_split_payment', 'no_action', 0.93, [],
    )
    assert no_action['financial_resolution']['recommended_refund_brl'] == 0
    assert no_action['resolution_actions'] == ['DOCUMENT_NO_ACTION']

    pending_evidence = [
        _evidence('refund', {
            'refund_id': 'REFUND_1',
            'refund_status': 'pending',
            'requested_refund_brl': 40,
        }, 'refund'),
        _evidence('policy', {'action': 'ESCALATE_REFUND_STATUS'}, 'policy'),
    ]
    pending = _build_output(
        {'case_id': 'CASE_001'}, pending_evidence,
        'refund_pending', 'action_required', 0.92, [],
    )
    assert pending['financial_resolution']['recommended_refund_brl'] == 40
    assert pending['financial_resolution']['refund_lines'][0]['entity_id'] == 'REFUND_1'

    clean = _calibrate_confidence('refund_pending', 0.92, pending_evidence, [])
    warned = [*pending_evidence, {
        **_evidence('payment', {'payment_id': 'PAY_1'}, 'warning'),
        'warnings': ['stale snapshot'],
    }]
    conflict = [{
        'field': 'refund_status',
        'sources': ['refund', 'payment'],
        'selected_source': 'refund',
        'resolution_code': 'REFUND_AUTHORITY',
    }]
    assert _calibrate_confidence('refund_pending', 0.92, warned, conflict) < clean < 1


def test_conflicts_are_cross_checked_only_for_the_same_entity() -> None:
    conflict = _data_conflicts([
        _evidence('order', {'order_id': 'ORDER_1', 'order_status': 'shipped'}, 'order_a'),
        _evidence('order', {'order_id': 'ORDER_1', 'order_status': 'canceled'}, 'order_b'),
        _evidence('order', {'order_id': 'ORDER_2', 'order_status': 'delivered'}, 'order_c'),
    ])
    assert len(conflict) == 1
    assert conflict[0]['field'] == 'order_status:ORDER_1'
    assert conflict[0]['selected_source'] is None
    assert conflict[0]['resolution_code'] == 'UNRESOLVED_AUTHORITATIVE_CONFLICT'


def test_phase4_counterexamples_are_closed() -> None:
    cross_entity = [
        _evidence(
            'order',
            {'order_id': 'ORDER_CANCEL', 'order_status': 'canceled'},
            'order_cancel',
        ),
        _evidence(
            'payment',
            {
                'order_id': 'ORDER_OTHER',
                'payment_id': 'PAY_2',
                'payment_status': 'captured',
                'captured_total_brl': 100,
            },
            'payment_other',
        ),
    ]
    assert _assessment(
        {'case_id': 'CASE_001', 'order_ids': ['ORDER_CANCEL', 'ORDER_OTHER']},
        cross_entity,
    )[0] == 'insufficient_evidence'

    split = [
        _evidence('order', {'order_id': 'ORDER_1', 'order_total_brl': 100}, 'order'),
        _evidence(
            'payment', {'payment_id': 'PAY_1', 'captured_total_brl': 40}, 'payment_a'
        ),
        _evidence(
            'payment', {'payment_id': 'PAY_2', 'captured_total_brl': 60}, 'payment_b'
        ),
    ]
    assert _assessment(
        {'case_id': 'CASE_001', 'order_id': 'ORDER_1', 'message': 'split payment'}, split
    )[0] == 'valid_split_payment'

    missing_amount = [
        _evidence('order', {'order_id': 'ORDER_1', 'order_status': 'canceled'}, 'order_m'),
        _evidence(
            'payment', {'payment_id': 'PAY_1', 'payment_status': 'captured'}, 'payment_m'
        ),
        _evidence(
            'policy', {'issue': 'canceled_order_paid', 'action': 'ISSUE_REFUND'}, 'policy_m'
        ),
    ]
    assert not _financially_complete('canceled_order_paid', missing_amount)

    amount_conflict = _data_conflicts([
        _evidence(
            'payment',
            {'payment_id': 'PAY_1', 'captured_total_brl': 100},
            'payment_c1',
        ),
        _evidence(
            'payment',
            {'payment_id': 'PAY_1', 'captured_total_brl': 120},
            'payment_c2',
        ),
    ])
    assert amount_conflict[0]['field'] == 'payment_captured_brl:PAY_1'
    resolved = [{
        'field': f'resolved_{index}',
        'sources': ['source_a', 'source_b'],
        'selected_source': 'source_a',
        'resolution_code': 'RESOLVED',
    } for index in range(5)]
    prioritized = _data_conflicts([
        _evidence('policy', {'conflicts': resolved}, 'policy_conflicts'),
        _evidence(
            'payment', {'payment_id': 'PAY_1', 'captured_total_brl': 100}, 'payment_c1'
        ),
        _evidence(
            'payment', {'payment_id': 'PAY_1', 'captured_total_brl': 120}, 'payment_c2'
        ),
    ])
    assert prioritized[0]['field'] == 'payment_captured_brl:PAY_1'
    policy_conflict = _data_conflicts([
        _evidence(
            'policy',
            {'issue': 'canceled_order_paid', 'recommended_refund_brl': 100},
            'policy_c1',
        ),
        _evidence(
            'policy',
            {'issue': 'canceled_order_paid', 'recommended_refund_brl': 120},
            'policy_c2',
        ),
    ])
    assert policy_conflict[0]['field'] == 'policy_refund_brl:canceled_order_paid'

    policy_evidence = [
        _evidence('order', {'order_id': 'ORDER_1'}, 'order_p'),
        _evidence(
            'policy',
            {
                'issue': 'canceled_order_paid',
                'action': 'CONTACT_LOGISTICS_PROVIDER',
            },
            'policy_p',
        ),
    ]
    output = _build_output(
        {'case_id': 'CASE_001'},
        policy_evidence,
        'canceled_order_paid',
        'action_required',
        0.5,
        [],
    )
    assert output['resolution_actions'] == ['ISSUE_REFUND']

    sellers = [
        _evidence('seller', {'seller_id': 'SELLER_A'}, 'seller_a'),
        _evidence('seller', {'seller_id': 'SELLER_B'}, 'seller_b'),
    ]
    output = _build_output(
        {'case_id': 'CASE_001'}, sellers,
        'late_delivery_seller', 'action_required', 0.5, [],
    )
    assert output['root_cause_analysis']['responsible_parties'] == [
        {'party_type': 'seller', 'party_id': 'SELLER_A'},
        {'party_type': 'seller', 'party_id': 'SELLER_B'},
    ]

    same_issue = [
        _evidence(
            'order', {'order_id': 'ORDER_A', 'order_status': 'canceled'}, 'order_same_a'
        ),
        _evidence(
            'payment',
            {
                'order_id': 'ORDER_A',
                'payment_id': 'PAY_A',
                'payment_status': 'captured',
                'captured_total_brl': 50,
            },
            'payment_same_a',
        ),
        _evidence(
            'order', {'order_id': 'ORDER_B', 'order_status': 'canceled'}, 'order_same_b'
        ),
        _evidence(
            'payment',
            {
                'order_id': 'ORDER_B',
                'payment_id': 'PAY_B',
                'payment_status': 'captured',
                'captured_total_brl': 60,
            },
            'payment_same_b',
        ),
    ]
    issue, status, confidence, selected, scopes = _assessment_scope(
        {'case_id': 'CASE_001', 'order_ids': ['ORDER_A', 'ORDER_B']}, same_issue
    )
    assert (issue, status, len(selected), len(scopes)) == (
        'canceled_order_paid', 'action_required', 4, 2,
    )
    output = _build_output(
        {'case_id': 'CASE_001'}, selected,
        issue, status, confidence, [],
    )
    assert output['affected_entities']['order_ids'] == ['ORDER_A', 'ORDER_B']
    assert output['financial_resolution']['recommended_refund_brl'] == 110
    assert output['financial_resolution']['refund_lines'] == [
        {
            'reason_code': 'CANCELED_ORDER_PAID',
            'amount_brl': 50.0,
            'entity_id': 'ORDER_A',
        },
        {
            'reason_code': 'CANCELED_ORDER_PAID',
            'amount_brl': 60.0,
            'entity_id': 'ORDER_B',
        },
    ]

    multiple_refunds = [
        _evidence(
            'refund',
            {
                'refund_id': 'REFUND_A',
                'refund_status': 'pending',
                'requested_refund_brl': 40,
            },
            'refund_a',
        ),
        _evidence(
            'refund',
            {
                'refund_id': 'REFUND_B',
                'refund_status': 'pending',
                'requested_refund_brl': 60,
            },
            'refund_b',
        ),
    ]
    issue, status, confidence, selected, scopes = _assessment_scope(
        {'case_id': 'CASE_001', 'refund_ids': ['REFUND_A', 'REFUND_B']}, multiple_refunds
    )
    output = _build_output(
        {'case_id': 'CASE_001'}, selected, issue, status, confidence, [],
    )
    assert len(scopes) == 2
    assert output['financial_resolution']['recommended_refund_brl'] == 100
    assert [line['entity_id'] for line in output['financial_resolution']['refund_lines']] == [
        'REFUND_A', 'REFUND_B'
    ]

    shared_seller = [
        _evidence(
            'order',
            {'order_id': 'ORDER_A', 'seller_id': 'SELLER_1'},
            'seller_order_a',
        ),
        _evidence(
            'order',
            {'order_id': 'ORDER_B', 'seller_id': 'SELLER_1'},
            'seller_order_b',
        ),
        _evidence(
            'seller',
            {'seller_id': 'SELLER_1', 'seller_delay': True},
            'seller_context',
        ),
    ]
    seller_scopes = _evidence_scopes(
        {'case_id': 'CASE_001', 'order_ids': ['ORDER_A', 'ORDER_B']}, shared_seller
    )
    assert len(seller_scopes) == 2
    assert all(
        any(item['domain'] == 'seller' for item in scope) for scope in seller_scopes
    )


def test_full_lifecycle_and_evidence_linkage_are_required() -> None:
    ref = 'ev_' + 'payment'.ljust(20, 'x')
    events = [
        {'event_type': 'case_received', 'actor': 'coordinator'},
        {'event_type': 'task_assigned', 'actor': 'coordinator', 'target': 'payment-agent'},
        {
            'event_type': 'tool_result_consumed',
            'actor': 'payment-agent',
            'tool_name': 'get_payment',
            'evidence_refs': [ref],
        },
        {'event_type': 'handoff', 'actor': 'payment-agent', 'target': 'policy-agent'},
        {'event_type': 'task_assigned', 'actor': 'coordinator', 'target': 'order-item-agent'},
        {'event_type': 'handoff', 'actor': 'order-item-agent', 'target': 'policy-agent'},
        {'event_type': 'task_assigned', 'actor': 'coordinator', 'target': 'shipment-agent'},
        {'event_type': 'handoff', 'actor': 'shipment-agent', 'target': 'policy-agent'},
        {'event_type': 'task_assigned', 'actor': 'coordinator', 'target': 'policy-agent'},
        {'event_type': 'policy_decided', 'actor': 'policy-agent'},
        {'event_type': 'handoff', 'actor': 'policy-agent', 'target': 'verifier-agent'},
        {
            'event_type': 'verification_completed',
            'actor': 'verifier-agent',
            'target': 'coordinator',
        },
        {'event_type': 'case_finalized', 'actor': 'coordinator'},
    ]
    _validate_case_lifecycle('CASE_001', events, [ref])
    with pytest.raises(ValueError, match='verification_completed'):
        _validate_case_lifecycle('CASE_001', events[:-2] + events[-1:], [ref])
    with pytest.raises(ValueError, match='not linked'):
        _validate_case_lifecycle('CASE_001', events, ['ev_' + 'other'.ljust(20, 'x')])
    with pytest.raises(ValueError, match='not linked'):
        _validate_case_lifecycle(
            'CASE_001',
            events,
            [ref],
            [{'evidence_refs': ['ev_' + 'claim'.ljust(20, 'x')]}],
        )
    wrong_actor = [dict(event) for event in events]
    next(
        event for event in wrong_actor if event['event_type'] == 'policy_decided'
    )['actor'] = 'coordinator'
    with pytest.raises(ValueError, match='invalid actor'):
        _validate_case_lifecycle('CASE_001', wrong_actor, [ref])


def test_artifact_validation_closes_lifecycle_end_to_end(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    contracts = Contracts(root / 'contracts' / 'schemas')
    evidence = [
        _evidence('order', {'order_id': 'ORDER_1'}, 'order'),
        _evidence('policy', {'action': 'NO_ACTION'}, 'policy'),
    ]
    output = _build_output(
        {'case_id': 'CASE_001'}, evidence, 'unsupported_claim', 'no_action', 0.8, [],
    )
    output_path = tmp_path / 'outputs' / 'CASE_001.json'
    output_path.parent.mkdir()
    output_path.write_text(json.dumps(output), encoding='utf-8')
    trace = TraceWriter(tmp_path / 'traces' / 'trace.jsonl', contracts)
    refs = output['evidence_refs']
    for event_type, actor, target, tool_name, event_refs in (
        ('case_received', 'coordinator', None, None, None),
        ('task_assigned', 'coordinator', 'order-item-agent', None, None),
        ('tool_result_consumed', 'order-item-agent', None, 'get_order', [refs[0]]),
        ('handoff', 'order-item-agent', 'policy-agent', None, None),
        ('task_assigned', 'coordinator', 'payment-agent', None, None),
        ('handoff', 'payment-agent', 'policy-agent', None, None),
        ('task_assigned', 'coordinator', 'shipment-agent', None, None),
        ('handoff', 'shipment-agent', 'policy-agent', None, None),
        ('task_assigned', 'coordinator', 'policy-agent', None, None),
        ('tool_result_consumed', 'policy-agent', None, 'get_policy', [refs[1]]),
        ('policy_decided', 'policy-agent', None, None, None),
        ('handoff', 'policy-agent', 'verifier-agent', None, None),
        ('verification_completed', 'verifier-agent', 'coordinator', None, None),
        ('case_finalized', 'coordinator', None, None, None),
    ):
        trace.emit(
            case_id='CASE_001',
            event_type=event_type,
            actor=actor,
            target=target,
            tool_name=tool_name,
            evidence_refs=event_refs,
        )
    case_set = CaseSet('test-v1', 'l3a', ('CASE_001',), {})
    outputs, lines = validate_artifacts(tmp_path, case_set, contracts)
    assert outputs['CASE_001'] == output
    assert len(lines) == 14


def test_all_primary_issue_branches() -> None:
    scenarios = [
        ('canceled_order_paid', 'canceled paid order', [
            _evidence('order', {'order_status': 'canceled'}, 'order'),
            _evidence('payment', {'payment_status': 'captured'}, 'payment'),
        ]),
        ('unavailable_order_paid', 'unavailable paid order', [
            _evidence('item', {'item_status': 'unavailable'}, 'item'),
            _evidence('payment', {'payment_status': 'captured'}, 'payment'),
        ]),
        ('refund_pending', 'refund', [_evidence('refund', {'refund_status': 'pending'}, 'refund')]),
        ('refund_failed', 'refund', [_evidence('refund', {'refund_status': 'failed'}, 'refund')]),
        ('duplicate_charge', 'charge', [
            _evidence('payment', {'duplicate_charge': True}, 'payment')
        ]),
        ('payment_mismatch', 'payment', [
            _evidence('order', {'order_total_brl': 10}, 'order'),
            _evidence('payment', {'captured_total_brl': 12}, 'payment'),
        ]),
        ('late_delivery_seller', 'late delivery', [
            _evidence('shipment', {'delivery_status': 'delayed', 'seller_delay': True}, 'shipment')
        ]),
        ('late_delivery_logistics', 'late delivery', [
            _evidence('shipment', {'delivery_status': 'delayed'}, 'shipment')
        ]),
        ('valid_split_payment', 'payment', [
            _evidence('order', {'order_total_brl': 10}, 'order'),
            _evidence(
                'payment',
                {'split_payment': True, 'captured_total_brl': 10},
                'payment',
            ),
        ]),
        ('unsupported_claim', 'late delivery', [
            _evidence('shipment', {'delivery_status': 'on_time'}, 'shipment')
        ]),
        ('insufficient_evidence', 'refund missing', [
            _evidence('order', {'order_status': 'delivered'}, 'order')
        ]),
    ]
    for expected, message, evidence in scenarios:
        assert _assessment({'case_id': 'CASE_001', 'message': message}, evidence)[0] == expected


def test_all_primary_issues_build_consistent_contract_outputs() -> None:
    root = Path(__file__).resolve().parents[1]
    contracts = Contracts(root / 'contracts' / 'schemas')
    expected = {
        'canceled_order_paid': ('action_required', 'platform'),
        'unavailable_order_paid': ('action_required', 'seller'),
        'late_delivery_seller': ('action_required', 'seller'),
        'late_delivery_logistics': ('action_required', 'logistics_provider'),
        'valid_split_payment': ('no_action', 'customer'),
        'payment_mismatch': ('action_required', 'payment_provider'),
        'duplicate_charge': ('action_required', 'payment_provider'),
        'refund_pending': ('action_required', 'payment_provider'),
        'refund_failed': ('action_required', 'payment_provider'),
        'unsupported_claim': ('no_action', 'customer'),
        'insufficient_evidence': ('needs_investigation', 'unknown'),
    }
    evidence = [
        _evidence('order', {'order_id': 'ORDER_1'}, 'order'),
        _evidence('seller', {'seller_id': 'SELLER_1'}, 'seller'),
        _evidence('policy', {'action': 'POLICY_ACTION'}, 'policy'),
    ]
    for issue, (status, party) in expected.items():
        output = _build_output(
            {'case_id': 'CASE_001'}, evidence, issue, status, 0.5, [],
        )
        contracts.validate_output(output, issue)
        assert output['assessment']['case_status'] == status
        assert output['root_cause_analysis']['responsible_parties'][0]['party_type'] == party
        assert bool(output['resolution_actions'])
        assert sum(
            line['amount_brl'] for line in output['financial_resolution']['refund_lines']
        ) == output['financial_resolution']['recommended_refund_brl']


def test_olist_raw_fields_drive_timeline_and_payment_reconciliation() -> None:
    late = [
        _evidence('order', {
            'order_delivered_carrier_date': '2024-01-03T00:00:00',
            'order_delivered_customer_date': '2024-01-12T00:00:00',
            'order_estimated_delivery_date': '2024-01-10T00:00:00',
        }, 'order'),
        _evidence('item', {'shipping_limit_date': '2024-01-02T00:00:00'}, 'item'),
    ]
    assert _assessment({'case_id': 'CASE_001', 'message': 'late delivery'}, late)[0] == (
        'late_delivery_seller'
    )

    payment = [
        _evidence('item', {'items': [
            {'price': 80, 'freight_value': 10},
            {'price': 20, 'freight_value': 0},
        ]}, 'item'),
        _evidence('payment', {'payments': [
            {'payment_sequential': 1, 'payment_value': 50},
            {'payment_sequential': 2, 'payment_value': 60},
        ]}, 'payment'),
    ]
    assert _assessment({'case_id': 'CASE_001', 'message': 'split payment'}, payment)[0] == (
        'valid_split_payment'
    )

    equal_lines = [
        _evidence('item', {'items': [
            {'order_item_id': 'ITEM_1', 'price': 50},
            {'order_item_id': 'ITEM_2', 'price': 50},
        ]}, 'equal_items'),
        _evidence('payment', {'payments': [
            {'payment_sequential': 1, 'payment_value': 50},
            {'payment_sequential': 2, 'payment_value': 50},
        ]}, 'equal_payments'),
    ]
    assert _assessment(
        {'case_id': 'CASE_001', 'message': 'split payment'}, equal_lines,
    )[0] == 'valid_split_payment'


def test_tool_arguments_skip_unscoped_optional_queries() -> None:
    schema = {
        'properties': {'case_id': {'type': 'string'}, 'order_id': {'type': 'string'}},
        'required': ['case_id'],
    }
    assert _tool_arguments({'case_id': 'CASE_001'}, schema) is None


def test_tool_arguments_fan_out_all_entities() -> None:
    schema = {
        'properties': {'case_id': {'type': 'string'}, 'order_id': {'type': 'string'}},
        'required': ['case_id', 'order_id'],
    }
    assert _tool_argument_sets(
        {'case_id': 'CASE_MULTI', 'order_ids': ['ORDER_A', 'ORDER_B']}, schema
    ) == [{'order_id': 'ORDER_A'}, {'order_id': 'ORDER_B'}]
    assert _tool_argument_sets(
        {'case_id': 'CASE_001', 'customer_request': {'claimed_order_id': 'ORDER_A'}},
        schema,
    ) == [{'order_id': 'ORDER_A'}]
    assert len(
        _tool_argument_sets(
            {'case_id': 'CASE_MULTI', 'order_ids': [f'ORDER_{index}' for index in range(100)]},
            schema,
        )
    ) == 20


def test_specialist_budgets_reserve_payment_and_policy(tmp_path: Path) -> None:
    class CrowdedGateway:
        def __init__(self) -> None:
            self.called: list[str] = []

        async def tool_specs(self) -> dict[str, dict[str, Any]]:
            entity_schema = {
                'properties': {'case_id': {}, 'order_id': {}},
                'required': ['case_id', 'order_id'],
            }
            specs = {
                f'get_order_{index:02}': {
                    'description': 'order',
                    'input_schema': entity_schema,
                    'output_schema': {},
                }
                for index in range(13)
            }
            for name in ('get_payment', 'get_shipment'):
                specs[name] = {
                    'description': name,
                    'input_schema': entity_schema,
                    'output_schema': {},
                }
            specs['get_policy'] = {
                'description': 'policy',
                'input_schema': {
                    'properties': {'case_id': {}, 'issue': {}},
                    'required': ['case_id', 'issue'],
                },
                'output_schema': {},
            }
            return specs

        async def call(
            self, tool_name: str, *, case_id: str, **arguments: Any
        ) -> dict[str, Any]:
            self.called.append(tool_name)
            domain = (
                'order' if 'order' in tool_name
                else 'payment' if 'payment' in tool_name
                else 'shipment' if 'shipment' in tool_name
                else 'policy'
            )
            data = {
                'order': {'order_status': 'canceled', 'order_id': arguments.get('order_id')},
                'payment': {'payment_status': 'captured', 'captured_total_brl': 100},
                'shipment': {'delivery_status': 'on_time'},
                'policy': {'action': 'ISSUE_REFUND'},
            }[domain]
            suffix = tool_name.removeprefix('get_').ljust(20, 'x')
            return {'evidence_ref': f'ev_{suffix}', 'domain': domain, 'data': data}

    root = Path(__file__).resolve().parents[1]
    trace = TraceWriter(tmp_path / 'trace.jsonl', Contracts(root / 'contracts' / 'schemas'))
    trace.emit(case_id='CASE_001', event_type='case_received', actor='coordinator')
    gateway = CrowdedGateway()
    output = asyncio.run(
        solve_case(
            {'case_id': 'CASE_001', 'order_id': 'ORDER_A', 'message': 'canceled paid'},
            gateway,
            trace,
        )
    )
    assert 'get_payment' in gateway.called
    assert 'get_policy' in gateway.called
    assert 'get_order_12' not in gateway.called
    assert output['assessment']['primary_issue'] == 'canceled_order_paid'


def test_retry_is_bounded_to_one_transient_failure() -> None:
    class FlakyGateway:
        calls = 0

        async def call(self, tool_name: str, **arguments: Any) -> dict[str, Any]:
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError
            return _evidence('order', {'status': 'delivered'}, 'order')

    gateway = FlakyGateway()
    result = asyncio.run(
        _call_with_retry(gateway, 'get_order', case_id='CASE_001', arguments={})
    )
    assert result['domain'] == 'order'
    assert gateway.calls == 2


def test_verifier_rejects_inconsistent_refund_total(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    trace = TraceWriter(tmp_path / 'trace.jsonl', Contracts(root / 'contracts' / 'schemas'))
    ref = 'ev_' + 'payment'.ljust(20, 'x')
    for event_type, actor in (
        ('case_received', 'coordinator'),
        ('task_assigned', 'coordinator'),
        ('tool_result_consumed', 'payment-agent'),
        ('handoff', 'payment-agent'),
        ('policy_decided', 'policy-agent'),
    ):
        trace.emit(
            case_id='CASE_001',
            event_type=event_type,
            actor=actor,
            evidence_refs=[ref] if event_type == 'tool_result_consumed' else None,
        )
    output = {
        'schema_version': 'day09-l3a-output-v2',
        'case_id': 'CASE_001',
        'assessment': {
            'primary_issue': 'refund_pending',
            'case_status': 'action_required',
            'confidence': 0.9,
        },
        'affected_entities': {
            'order_ids': [], 'item_ids': [], 'seller_ids': [],
            'payment_references': [], 'shipment_ids': [],
        },
        'root_cause_analysis': {
            'ranked_causes': [{'cause_code': 'REFUND_PENDING', 'rank': 1}],
            'responsible_parties': [{'party_type': 'payment_provider', 'party_id': None}],
        },
        'evidence_refs': [ref],
        'data_conflicts': [],
        'financial_resolution': {
            'currency': 'BRL',
            'recommended_refund_brl': 100,
            'refund_lines': [{'reason_code': 'REFUND_PENDING', 'amount_brl': 1, 'entity_id': None}],
        },
        'resolution_actions': ['ESCALATE_REFUND_STATUS'],
    }
    with pytest.raises(ValueError, match='refund lines'):
        _verify_output(
            {'case_id': 'CASE_001'},
            [_evidence('payment', {'refund_status': 'pending'}, 'payment')],
            output,
            trace,
        )


def test_verifier_recomputes_policy_actions_and_financial_completeness(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    trace = TraceWriter(tmp_path / 'trace.jsonl', Contracts(root / 'contracts' / 'schemas'))
    evidence = [
        _evidence(
            'order', {'order_id': 'ORDER_1', 'order_status': 'canceled'}, 'order_v'
        ),
        _evidence(
            'payment',
            {
                'payment_id': 'PAY_1',
                'payment_status': 'captured',
                'captured_total_brl': 100,
            },
            'payment_v',
        ),
        _evidence(
            'policy',
            {'issue': 'canceled_order_paid', 'action': 'ISSUE_REFUND'},
            'policy_v',
        ),
    ]
    output = _build_output(
        {'case_id': 'CASE_001'}, evidence,
        'canceled_order_paid', 'action_required', 0.9, [],
    )
    output['resolution_actions'] = ['CONTACT_LOGISTICS_PROVIDER']
    with pytest.raises(ValueError, match='resolution actions'):
        _verify_output({'case_id': 'CASE_001'}, evidence, output, trace)

    incomplete = [evidence[0], {
        **evidence[1],
        'data': {'payment_id': 'PAY_1', 'payment_status': 'captured'},
    }, evidence[2]]
    output = _build_output(
        {'case_id': 'CASE_001'}, incomplete,
        'canceled_order_paid', 'action_required', 0.9, [],
    )
    with pytest.raises(ValueError, match='financial evidence'):
        _verify_output({'case_id': 'CASE_001'}, incomplete, output, trace)
