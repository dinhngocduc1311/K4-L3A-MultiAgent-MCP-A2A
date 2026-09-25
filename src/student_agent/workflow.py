from __future__ import annotations

import asyncio
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from .mcp_gateway import EvidenceGateway
from .trace import TraceWriter

ACTOR_COORDINATOR = "coordinator"
ACTOR_ORDER_ITEM = "order-item-agent"
ACTOR_PAYMENT = "payment-agent"
ACTOR_SHIPMENT = "shipment-agent"
ACTOR_POLICY = "policy-agent"
ACTOR_VERIFIER = "verifier-agent"

MAX_MCP_ATTEMPTS = 3
MAX_CANDIDATES_PER_ENTITY = 5
MONEY_TOLERANCE_BRL = 0.01

TOOL_DOMAIN_ALLOWLIST: dict[str, tuple[str, ...]] = {
    ACTOR_ORDER_ITEM: ("order", "item", "seller", "product"),
    ACTOR_PAYMENT: ("payment", "refund"),
    ACTOR_SHIPMENT: ("shipment", "logistics"),
    ACTOR_POLICY: ("policy",),
}

OUTPUT_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "case_id",
        "assessment",
        "affected_entities",
        "claim_assessments",
        "root_cause_analysis",
        "evidence_refs",
        "data_conflicts",
        "financial_resolution",
        "resolution_actions",
    }
)

ID_PATTERNS: dict[str, tuple[str, ...]] = {
    "order_ids": ("claimed_order_id", "order_id", "order_ids"),
    "item_ids": ("item_id", "item_ids", "order_item_id", "order_item_ids"),
    "seller_ids": ("seller_id", "seller_ids"),
    "payment_references": (
        "payment_reference",
        "payment_references",
        "payment_id",
        "payment_ids",
        "transaction_id",
        "transaction_ids",
    ),
    "shipment_ids": ("shipment_id", "shipment_ids", "tracking_id", "tracking_ids"),
}

ENTITY_ARGUMENTS = {
    "order_ids": "order_id",
    "item_ids": "item_id",
    "seller_ids": "seller_id",
    "payment_references": "payment_reference",
    "shipment_ids": "shipment_id",
}

ISSUE_RESPONSIBILITY: dict[str, tuple[str, str | None, str]] = {
    "canceled_order_paid": ("platform", None, "refund_paid_canceled_order"),
    "unavailable_order_paid": ("seller", None, "refund_unavailable_order"),
    "late_delivery_seller": ("seller", None, "seller_delay_resolution"),
    "late_delivery_logistics": ("logistics_provider", None, "logistics_delay_resolution"),
    "valid_split_payment": ("customer", None, "explain_split_payment"),
    "payment_mismatch": ("payment_provider", None, "reconcile_payment_amount"),
    "duplicate_charge": ("payment_provider", None, "refund_duplicate_charge"),
    "refund_pending": ("payment_provider", None, "monitor_refund"),
    "refund_failed": ("payment_provider", None, "retry_failed_refund"),
    "unsupported_claim": ("customer", None, "close_unsupported_claim"),
    "insufficient_evidence": ("unknown", None, "collect_additional_evidence"),
}

REFUND_ISSUES = {
    "canceled_order_paid",
    "unavailable_order_paid",
    "payment_mismatch",
    "duplicate_charge",
    "refund_pending",
    "refund_failed",
}

STRING_ID_REGEXES: dict[str, tuple[re.Pattern[str], ...]] = {
    entity: tuple(
        re.compile(rf"\b{re.escape(key)}\b\s*[:=#-]\s*([A-Za-z0-9][A-Za-z0-9_-]{{2,127}})", re.I)
        for key in keys
    )
    for entity, keys in ID_PATTERNS.items()
}


@dataclass(frozen=True)
class AgentTask:
    case_id: str
    sender: str
    recipient: str
    task: str
    attempt: int = 1
    entity_scope: dict[str, list[str]] = field(default_factory=dict)
    evidence_refs: tuple[str, ...] = ()

    def trace_attributes(self) -> dict[str, str | int | float | bool | None]:
        return {
            "task": self.task,
            "attempt": self.attempt,
            "entity_scope_keys": ",".join(sorted(self.entity_scope)),
        }


@dataclass
class EvidenceIndex:
    case_id: str
    by_ref: dict[str, dict[str, Any]] = field(default_factory=dict)

    def add(self, evidence: dict[str, Any]) -> str:
        evidence_ref = evidence["evidence_ref"]
        self.by_ref[evidence_ref] = evidence
        return evidence_ref

    def require_known_refs(self, refs: Iterable[str]) -> None:
        missing = sorted(set(refs) - set(self.by_ref))
        if missing:
            raise ValueError(f"unknown evidence refs for {self.case_id}: {missing}")

    def refs(self) -> list[str]:
        return sorted(self.by_ref)


@dataclass
class SpecialistResult:
    actor: str
    entities: dict[str, list[str]] = field(default_factory=dict)
    evidence_refs: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def add_entity(self, entity: str, value: str) -> None:
        values = self.entities.setdefault(entity, [])
        if value not in values:
            values.append(value)

    def merge_entities(self, values: dict[str, list[str]]) -> None:
        for entity, entity_values in values.items():
            for value in entity_values:
                self.add_entity(entity, value)

    def add_evidence(self, evidence: dict[str, Any]) -> None:
        evidence_ref = evidence["evidence_ref"]
        if evidence_ref not in self.evidence_refs:
            self.evidence_refs.append(evidence_ref)
        self.merge_entities(_extract_entity_ids(evidence.get("data", {})))


@dataclass(frozen=True)
class PolicySignal:
    issue: str
    confidence: float
    evidence_refs: tuple[str, ...]
    reason_code: str
    refund_brl: float = 0.0
    entity_id: str | None = None
    responsible_party: str | None = None


@dataclass(frozen=True)
class CaseFacts:
    order_status: str | None
    purchase_at: datetime | None
    delivered_at: datetime | None
    estimated_at: datetime | None
    current_item_total: float
    current_freight_total: float
    captured_total: float
    captured_amounts: tuple[float, ...]
    has_split_payment: bool
    has_duplicate_charge: bool
    has_payment_mismatch: bool
    refund_pending_amount: float
    refund_failed_amount: float
    late_actor: str | None


def _case_id(case: dict[str, Any]) -> str:
    value = case.get("case_id")
    if not isinstance(value, str) or not value:
        raise ValueError("case is missing a valid case_id")
    return value


def _tool_allowed(actor: str, tool_name: str) -> bool:
    domains = TOOL_DOMAIN_ALLOWLIST.get(actor, ())
    normalized = tool_name.lower()
    return any(domain in normalized for domain in domains)


def _limit(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        if value and value not in result:
            result.append(value)
        if len(result) >= MAX_CANDIDATES_PER_ENTITY:
            break
    return result


def _normalize_entity_scope(scope: dict[str, Iterable[str]]) -> dict[str, list[str]]:
    return {entity: _limit(str(value) for value in values) for entity, values in scope.items()}


def _walk_json(value: Any) -> Iterable[tuple[str | None, Any]]:
    if isinstance(value, dict):
        for key, item in value.items():
            yield str(key), item
            yield from _walk_json(item)
    elif isinstance(value, list):
        for item in value:
            yield None, item
            yield from _walk_json(item)


def _entity_for_key(key: str) -> str | None:
    normalized = key.lower()
    for entity, patterns in ID_PATTERNS.items():
        if any(pattern == normalized for pattern in patterns):
            return entity
    return None


def _extract_entity_ids(value: Any) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {entity: [] for entity in ID_PATTERNS}
    for key, item in _walk_json(value):
        if key is not None:
            entity = _entity_for_key(key)
            if entity is not None:
                if isinstance(item, str):
                    found[entity].append(item)
                elif isinstance(item, int):
                    found[entity].append(str(item))
                elif isinstance(item, list):
                    found[entity].extend(
                        str(child) for child in item if isinstance(child, str | int)
                    )
        if isinstance(item, str):
            for entity, regexes in STRING_ID_REGEXES.items():
                for regex in regexes:
                    found[entity].extend(match.group(1) for match in regex.finditer(item))
    return _normalize_entity_scope(found)


def _merge_entity_scopes(*scopes: dict[str, list[str]]) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {entity: [] for entity in ID_PATTERNS}
    for scope in scopes:
        for entity, values in scope.items():
            if entity not in merged:
                continue
            for value in values:
                if value not in merged[entity]:
                    merged[entity].append(value)
    return _normalize_entity_scope(merged)


def _find_tool(discovered_tools: set[str], *preferred_names: str) -> str | None:
    for name in preferred_names:
        if name in discovered_tools:
            return name
    for name in preferred_names:
        normalized = name.lower()
        matches = sorted(tool for tool in discovered_tools if normalized in tool.lower())
        if matches:
            return matches[0]
    return None


def _collect_output_evidence_refs(output: dict[str, Any]) -> set[str]:
    refs = set(output.get("evidence_refs", []))
    for claim in output.get("claim_assessments", []):
        refs.update(claim.get("evidence_refs", []))
    return refs


def _claim_topics(case: dict[str, Any]) -> list[str]:
    request = case.get("customer_request", {})
    if not isinstance(request, dict):
        return []
    claims = request.get("claims", [])
    if not isinstance(claims, list):
        return []
    topics: list[str] = []
    for claim in claims:
        if not isinstance(claim, dict):
            continue
        topic = claim.get("topic")
        if isinstance(topic, str) and topic in ISSUE_RESPONSIBILITY and topic not in topics:
            topics.append(topic)
    return topics


def _flatten_text(value: Any) -> str:
    parts: list[str] = []
    for key, item in _walk_json(value):
        if key is not None:
            parts.append(key)
        if isinstance(item, str | int | float | bool):
            parts.append(str(item))
    return " ".join(parts).lower()


def _numbers_for_keys(value: Any, key_fragments: tuple[str, ...]) -> list[float]:
    numbers: list[float] = []
    for key, item in _walk_json(value):
        if key is None or not any(fragment in key.lower() for fragment in key_fragments):
            continue
        if isinstance(item, int | float):
            numbers.append(float(item))
        elif isinstance(item, str):
            normalized = item.replace(",", ".")
            try:
                numbers.append(float(normalized))
            except ValueError:
                continue
    return numbers


def _number_from_value(value: Any) -> float | None:
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", "."))
        except ValueError:
            return None
    return None


def _money_values_for_key(value: Any, target_key: str) -> list[float]:
    result: list[float] = []
    for key, item in _walk_json(value):
        if key != target_key:
            continue
        amount = _number_from_value(item)
        if amount is not None:
            result.append(amount)
    return result


def _max_money(value: Any) -> float:
    candidates = _numbers_for_keys(
        value,
        (
            "amount",
            "total",
            "value",
            "price",
            "paid",
            "payment",
            "refund",
            "freight",
        ),
    )
    return round(max(candidates, default=0.0), 2)


def _safe_parse_date(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    for candidate in (text, text.replace("Z", "+00:00")):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            continue
    return None


def _dates_for_keys(value: Any, key_fragments: tuple[str, ...]) -> list[datetime]:
    dates: list[datetime] = []
    for key, item in _walk_json(value):
        if key is None or not any(fragment in key.lower() for fragment in key_fragments):
            continue
        parsed = _safe_parse_date(item)
        if parsed is not None:
            dates.append(parsed)
    return dates


def _evidence_by_domain(evidence_index: EvidenceIndex, *domains: str) -> list[dict[str, Any]]:
    wanted = set(domains)
    return [
        evidence
        for evidence in evidence_index.by_ref.values()
        if evidence.get("domain") in wanted
    ]


def _refs_for(evidence_items: Iterable[dict[str, Any]]) -> tuple[str, ...]:
    return tuple(sorted({evidence["evidence_ref"] for evidence in evidence_items}))


def _has_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


def _first_order_data(evidence_index: EvidenceIndex) -> dict[str, Any]:
    for evidence in _evidence_by_domain(evidence_index, "order"):
        data = evidence.get("data")
        if isinstance(data, dict):
            return data
    return {}


def _in_case_window(
    value: Any,
    anchor: datetime | None,
    *,
    days_before: int = 1,
    days_after: int = 45,
) -> bool:
    if anchor is None:
        return True
    parsed = _safe_parse_date(value)
    if parsed is None:
        return False
    return anchor - timedelta(days=days_before) <= parsed <= anchor + timedelta(days=days_after)


def _current_item_rows(
    item_evidence: list[dict[str, Any]], purchase_at: datetime | None
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    fallback: list[dict[str, Any]] = []
    for evidence in item_evidence:
        data = evidence.get("data", {})
        if not isinstance(data, list):
            continue
        for item in data:
            if not isinstance(item, dict):
                continue
            fallback.append(item)
            if _in_case_window(item.get("shipping_limit_date"), purchase_at):
                rows.append(item)
    return rows or fallback


def _current_payment_events(
    payment_evidence: list[dict[str, Any]], purchase_at: datetime | None
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    fallback: list[dict[str, Any]] = []
    for evidence in payment_evidence:
        data = evidence.get("data", {})
        if not isinstance(data, dict):
            continue
        raw_events = data.get("events")
        if not isinstance(raw_events, list):
            continue
        for event in raw_events:
            if not isinstance(event, dict):
                continue
            fallback.append(event)
            if _in_case_window(event.get("event_at"), purchase_at):
                events.append(event)
    return events or fallback


def _payment_rows(payment_evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for evidence in payment_evidence:
        if evidence.get("domain") != "payment":
            continue
        data = evidence.get("data", {})
        if isinstance(data, list):
            rows.extend(item for item in data if isinstance(item, dict))
        elif isinstance(data, dict):
            payments = data.get("payments")
            if isinstance(payments, list):
                rows.extend(item for item in payments if isinstance(item, dict))
    return rows


def _evidence_payment_rows(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    if evidence.get("domain") != "payment":
        return []
    data = evidence.get("data", {})
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        payments = data.get("payments")
        if isinstance(payments, list):
            return [item for item in payments if isinstance(item, dict)]
    return []


def _payment_fingerprints(rows: list[dict[str, Any]]) -> list[tuple[str, str, float]]:
    fingerprints: list[tuple[str, str, float]] = []
    for row in rows:
        amount = _number_from_value(row.get("payment_value"))
        if amount is None:
            continue
        fingerprints.append(
            (
                str(row.get("payment_sequential", "")),
                str(row.get("payment_type", "")),
                round(amount, 2),
            )
        )
    return fingerprints


def _has_duplicate_payment_rows(payment_evidence: list[dict[str, Any]]) -> bool:
    for evidence in payment_evidence:
        fingerprints = _payment_fingerprints(_evidence_payment_rows(evidence))
        if fingerprints and len(fingerprints) > len(set(fingerprints)):
            return True
    return False


def _current_refund_events(
    payment_evidence: list[dict[str, Any]], purchase_at: datetime | None
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    fallback: list[dict[str, Any]] = []
    for evidence in payment_evidence:
        if evidence.get("domain") != "refund":
            continue
        data = evidence.get("data", {})
        raw_events = data.get("events") if isinstance(data, dict) else None
        if not isinstance(raw_events, list):
            continue
        for event in raw_events:
            if not isinstance(event, dict):
                continue
            fallback.append(event)
            if _in_case_window(event.get("event_at"), purchase_at, days_after=90):
                events.append(event)
    return events if purchase_at is not None else fallback


def _current_late_actor(
    shipment_evidence: list[dict[str, Any]], purchase_at: datetime | None
) -> str | None:
    for evidence in shipment_evidence:
        data = evidence.get("data", {})
        if not isinstance(data, dict):
            continue
        raw_events = data.get("events")
        if not isinstance(raw_events, list):
            continue
        for event in raw_events:
            if not isinstance(event, dict) or event.get("event_type") != "delivered_late":
                continue
            if not _in_case_window(event.get("event_at"), purchase_at, days_after=60):
                continue
            actor = event.get("actor")
            if isinstance(actor, str) and actor:
                return actor
    return None


def _build_case_facts(evidence_index: EvidenceIndex) -> CaseFacts:
    order_data = _first_order_data(evidence_index)
    purchase_at = _safe_parse_date(order_data.get("order_purchase_timestamp"))
    delivered_at = _safe_parse_date(order_data.get("order_delivered_customer_date"))
    estimated_at = _safe_parse_date(order_data.get("order_estimated_delivery_date"))
    order_status = (
        order_data.get("order_status") if isinstance(order_data.get("order_status"), str) else None
    )

    item_rows = _current_item_rows(
        _evidence_by_domain(evidence_index, "item"), purchase_at
    )
    item_total = round(
        sum((_number_from_value(item.get("price")) or 0.0) for item in item_rows), 2
    )
    freight_total = round(
        sum((_number_from_value(item.get("freight_value")) or 0.0) for item in item_rows), 2
    )

    payment_items = _evidence_by_domain(evidence_index, "payment", "refund")
    current_events = _current_payment_events(payment_items, purchase_at)
    captured_amounts = tuple(
        amount
        for event in current_events
        if event.get("event_type") == "captured"
        for amount in [_number_from_value(event.get("amount_brl"))]
        if amount is not None
    )
    captured_total = round(sum(captured_amounts), 2)
    current_refunds = _current_refund_events(payment_items, purchase_at)
    refund_pending_amount = round(
        sum(
            _number_from_value(event.get("amount_brl")) or 0.0
            for event in current_refunds
            if str(event.get("status", "")).lower() == "pending"
        ),
        2,
    )
    refund_failed_amount = round(
        sum(
            _number_from_value(event.get("amount_brl")) or 0.0
            for event in current_refunds
            if str(event.get("status", "")).lower() == "failed"
        ),
        2,
    )
    has_duplicate_charge = _has_duplicate_payment_rows(payment_items)
    has_split_payment = len(captured_amounts) >= 2 and not has_duplicate_charge
    has_payment_mismatch = any(
        event.get("event_type") == "reconciliation_mismatch" for event in current_events
    )
    late_actor = _current_late_actor(_evidence_by_domain(evidence_index, "shipment"), purchase_at)
    if (
        late_actor is None
        and delivered_at is not None
        and estimated_at is not None
        and delivered_at > estimated_at
    ):
        late_actor = "logistics_provider"

    return CaseFacts(
        order_status=order_status,
        purchase_at=purchase_at,
        delivered_at=delivered_at,
        estimated_at=estimated_at,
        current_item_total=item_total,
        current_freight_total=freight_total,
        captured_total=captured_total,
        captured_amounts=captured_amounts,
        has_split_payment=has_split_payment,
        has_duplicate_charge=has_duplicate_charge,
        has_payment_mismatch=has_payment_mismatch,
        refund_pending_amount=refund_pending_amount,
        refund_failed_amount=refund_failed_amount,
        late_actor=late_actor,
    )


def _policy_evidence(evidence_index: EvidenceIndex) -> dict[str, Any] | None:
    for evidence in _evidence_by_domain(evidence_index, "policy"):
        return evidence
    return None


def _policy_rule(evidence_index: EvidenceIndex, issue: str) -> dict[str, Any]:
    evidence = _policy_evidence(evidence_index)
    data = evidence.get("data", {}) if evidence else {}
    rules = data.get("rules") if isinstance(data, dict) else None
    rule = rules.get(issue) if isinstance(rules, dict) else None
    return rule if isinstance(rule, dict) else {}


def _refs_for_domains(evidence_index: EvidenceIndex, *domains: str) -> tuple[str, ...]:
    return _refs_for(_evidence_by_domain(evidence_index, *domains))


def _issue_refs(evidence_index: EvidenceIndex, issue: str) -> tuple[str, ...]:
    domains_by_issue = {
        "canceled_order_paid": ("order", "payment", "policy"),
        "unavailable_order_paid": ("order", "item", "product", "payment", "policy"),
        "late_delivery_seller": ("order", "item", "seller", "shipment", "policy"),
        "late_delivery_logistics": ("order", "shipment", "policy"),
        "valid_split_payment": ("order", "item", "payment", "policy"),
        "payment_mismatch": ("order", "item", "payment", "policy"),
        "duplicate_charge": ("order", "payment", "policy"),
        "refund_pending": ("payment", "refund", "policy"),
        "refund_failed": ("payment", "refund", "policy"),
        "unsupported_claim": ("order", "item", "payment", "shipment", "policy"),
        "insufficient_evidence": ("order", "item", "payment", "shipment", "policy"),
    }
    return _refs_for_domains(evidence_index, *domains_by_issue.get(issue, ("policy",)))


def _payment_total(payment_evidence: list[dict[str, Any]]) -> float:
    direct_payment_values: list[float] = []
    timeline_payment_values: list[float] = []
    captured_event_values: list[float] = []
    for evidence in payment_evidence:
        data = evidence.get("data", {})
        if isinstance(data, list):
            direct_payment_values.extend(_money_values_for_key(data, "payment_value"))
        elif isinstance(data, dict):
            payments = data.get("payments")
            if payments is not None:
                timeline_payment_values.extend(_money_values_for_key(payments, "payment_value"))
            events = data.get("events")
            if events is not None:
                for event in events:
                    if not isinstance(event, dict) or event.get("event_type") != "captured":
                        continue
                    amount = _number_from_value(event.get("amount_brl"))
                    if amount is not None:
                        captured_event_values.append(amount)

    if direct_payment_values:
        return round(sum(direct_payment_values), 2)
    if timeline_payment_values:
        return round(sum(timeline_payment_values), 2)
    return round(sum(captured_event_values), 2)


def _order_total(order_evidence: list[dict[str, Any]]) -> float:
    item_totals: list[float] = []
    for evidence in order_evidence:
        data = evidence.get("data", {})
        if not isinstance(data, list):
            continue
        for item in data:
            if not isinstance(item, dict):
                continue
            price = _number_from_value(item.get("price")) or 0.0
            freight = _number_from_value(item.get("freight_value")) or 0.0
            item_totals.append(price + freight)
    if item_totals:
        return round(sum(item_totals), 2)

    totals = (_max_money(evidence.get("data", {})) for evidence in order_evidence)
    return round(max(totals, default=0), 2)


def _late_delivery_signal(shipment_evidence: list[dict[str, Any]]) -> bool:
    for evidence in shipment_evidence:
        data = evidence.get("data", {})
        delivered_dates = _dates_for_keys(data, ("delivered", "delivery_date", "actual"))
        estimated_dates = _dates_for_keys(data, ("estimated", "deadline", "promised"))
        if delivered_dates and estimated_dates and max(delivered_dates) > min(estimated_dates):
            return True
        text = _flatten_text(data)
        if _has_any(text, ("late", "delayed", "delay", "atras")):
            return True
    return False


def _data_conflicts(evidence_index: EvidenceIndex) -> list[dict[str, Any]]:
    facts = _build_case_facts(evidence_index)
    conflicts: list[dict[str, Any]] = []
    if facts.has_payment_mismatch:
        conflicts.append(
            {
                "field": "payment_reconciliation",
                "sources": [
                    "payment_timeline:captured",
                    "payment_timeline:reconciliation_mismatch",
                ],
                "selected_source": "payment_timeline",
                "resolution_code": "payment_timeline_reports_mismatch",
            }
        )
    return conflicts[:5]


def _claim_items(case: dict[str, Any]) -> list[dict[str, Any]]:
    request = case.get("customer_request", {})
    claims = request.get("claims", []) if isinstance(request, dict) else []
    return [claim for claim in claims if isinstance(claim, dict)]


def _rule_refund_amount(rule: dict[str, Any], fallback: float) -> float:
    value = _number_from_value(rule.get("refund_brl"))
    if value is None:
        return round(fallback, 2)
    return round(value, 2)


def _rule_action(rule: dict[str, Any], fallback: str) -> str:
    value = rule.get("recommended_action")
    return value if isinstance(value, str) and value else fallback


def _rule_case_status(rule: dict[str, Any], fallback: str) -> str:
    value = rule.get("case_status")
    return value if value in {"action_required", "no_action", "needs_investigation"} else fallback


def _rule_responsible_parties(
    rule: dict[str, Any],
    fallback_party: str,
    entity_scope: dict[str, list[str]],
) -> list[dict[str, str | None]]:
    parties = rule.get("responsible_parties")
    if isinstance(parties, list) and parties:
        normalized: list[dict[str, str | None]] = []
        for party in parties[:5]:
            if not isinstance(party, dict):
                continue
            party_type = party.get("party_type")
            party_id = party.get("party_id")
            if party_type not in {
                "seller",
                "platform",
                "logistics_provider",
                "payment_provider",
                "customer",
                "unknown",
            }:
                continue
            if party_type == "seller":
                party_id = next(iter(entity_scope.get("seller_ids", [])), party_id)
            normalized.append(
                {
                    "party_type": party_type,
                    "party_id": party_id if isinstance(party_id, str) else None,
                }
            )
        if normalized:
            return normalized

    party_id = (
        next(iter(entity_scope.get("seller_ids", [])), None)
        if fallback_party == "seller"
        else None
    )
    return [{"party_type": fallback_party, "party_id": party_id}]


def _detect_policy_signal(
    evidence_index: EvidenceIndex, claimed_topics: Iterable[str]
) -> PolicySignal:
    all_items = list(evidence_index.by_ref.values())
    if not all_items:
        return PolicySignal("insufficient_evidence", 0.05, (), "NO_MCP_EVIDENCE")

    facts = _build_case_facts(evidence_index)
    claimed = set(claimed_topics)
    order_text = _flatten_text(
        [
            item.get("data", {})
            for item in _evidence_by_domain(evidence_index, "order", "item", "product")
        ]
    )
    paid = facts.captured_total > MONEY_TOLERANCE_BRL
    order_total = round(facts.current_item_total + facts.current_freight_total, 2)

    issue = "unsupported_claim"
    confidence = 0.58
    reason = "EVIDENCE_DOES_NOT_SUPPORT_CLAIM"
    fallback_refund = 0.0

    if facts.order_status in {"canceled", "cancelled"} and paid:
        issue = "canceled_order_paid"
        confidence = 0.92
        reason = "CANCELED_AND_PAID"
        fallback_refund = facts.captured_total
    elif (
        facts.order_status == "unavailable"
        or _has_any(order_text, ("unavailable", "indispon", "stockout"))
    ) and paid:
        issue = "unavailable_order_paid"
        confidence = 0.9
        reason = "UNAVAILABLE_AND_PAID"
        fallback_refund = facts.captured_total
    elif facts.refund_failed_amount > MONEY_TOLERANCE_BRL:
        issue = "refund_failed"
        confidence = 0.9
        reason = "REFUND_FAILED_SIGNAL"
        fallback_refund = facts.refund_failed_amount
    elif facts.refund_pending_amount > MONEY_TOLERANCE_BRL:
        issue = "refund_pending"
        confidence = 0.86
        reason = "REFUND_PENDING_SIGNAL"
        fallback_refund = 0.0
    elif facts.has_duplicate_charge:
        issue = "duplicate_charge"
        confidence = 0.88
        reason = "DUPLICATE_PAYMENT_SIGNAL"
        fallback_refund = (
            min(facts.captured_amounts) if facts.captured_amounts else facts.captured_total
        )
    elif facts.has_payment_mismatch:
        issue = "payment_mismatch"
        confidence = 0.86
        reason = "PAYMENT_RECONCILIATION_MISMATCH"
        fallback_refund = abs(facts.captured_total - order_total) if order_total else 0.0
    elif facts.late_actor == "seller":
        issue = "late_delivery_seller"
        confidence = 0.88
        reason = "LATE_DELIVERY_SELLER"
        fallback_refund = facts.current_freight_total
    elif facts.late_actor == "logistics_provider":
        issue = "late_delivery_logistics"
        confidence = 0.88
        reason = "LATE_DELIVERY_LOGISTICS"
        fallback_refund = facts.current_freight_total
    elif (
        facts.has_split_payment
        and order_total
        and abs(facts.captured_total - order_total) <= MONEY_TOLERANCE_BRL
    ):
        issue = "valid_split_payment"
        confidence = 0.87
        reason = "SPLIT_PAYMENT_TOTAL_MATCH"

    if issue not in claimed and issue != "unsupported_claim":
        confidence = min(confidence, 0.78)

    refs = _issue_refs(evidence_index, issue)
    if not refs:
        refs = _refs_for(all_items)
    rule = _policy_rule(evidence_index, issue)
    refund = _rule_refund_amount(rule, round(fallback_refund, 2))
    return PolicySignal(issue, confidence, refs, reason, refund)


def _calibrate_confidence(signal: PolicySignal, conflicts: list[dict[str, Any]]) -> float:
    confidence = signal.confidence
    if not signal.evidence_refs:
        confidence = min(confidence, 0.1)
    elif len(signal.evidence_refs) == 1:
        confidence = min(confidence, 0.65)
    if conflicts:
        confidence = min(confidence, 0.82) - min(0.15, 0.03 * len(conflicts))
    if signal.issue == "unsupported_claim":
        confidence = min(confidence, 0.6)
    if signal.issue == "insufficient_evidence":
        confidence = min(confidence, 0.25)
    return round(max(0.0, min(0.95, confidence)), 2)


def _build_claim_assessments(
    *,
    case: dict[str, Any],
    signal: PolicySignal,
    confidence: float,
    refund_amount: float,
    facts: CaseFacts,
) -> list[dict[str, Any]]:
    assessments: list[dict[str, Any]] = []
    for claim in _claim_items(case):
        claim_id = claim.get("claim_id")
        topic = claim.get("topic")
        if not isinstance(claim_id, str) or not isinstance(topic, str):
            continue
        if topic == "requested_full_refund":
            if refund_amount <= MONEY_TOLERANCE_BRL:
                verdict = (
                    "unsupported"
                    if signal.issue != "insufficient_evidence"
                    else "insufficient_evidence"
                )
            elif (
                facts.captured_total
                and refund_amount + MONEY_TOLERANCE_BRL < facts.captured_total
            ):
                verdict = "partially_supported"
            else:
                verdict = "supported"
        elif topic == signal.issue:
            verdict = "supported"
        elif signal.issue == "insufficient_evidence":
            verdict = "insufficient_evidence"
        else:
            verdict = "unsupported"
        assessments.append(
            {
                "claim_id": claim_id,
                "verdict": verdict,
                "confidence": confidence,
                "evidence_refs": list(signal.evidence_refs),
            }
        )
        if len(assessments) >= 5:
            break
    return assessments


def _build_policy_output(
    *,
    case: dict[str, Any],
    case_id: str,
    specialist_results: Iterable[SpecialistResult],
    evidence_index: EvidenceIndex,
) -> dict[str, Any]:
    entity_scope = _merge_entity_scopes(*(result.entities for result in specialist_results))
    signal = _detect_policy_signal(evidence_index, _claim_topics(case))
    conflicts = _data_conflicts(evidence_index)
    confidence = _calibrate_confidence(signal, conflicts)
    fallback_party, _fallback_party_id, fallback_action = ISSUE_RESPONSIBILITY[signal.issue]
    rule = _policy_rule(evidence_index, signal.issue)
    action = _rule_action(rule, fallback_action)
    refund_amount = round(signal.refund_brl if signal.issue in REFUND_ISSUES else 0.0, 2)
    refund_lines = []
    if refund_amount > 0:
        refund_lines.append(
            {
                "reason_code": signal.reason_code.lower(),
                "amount_brl": refund_amount,
                "entity_id": signal.entity_id,
            }
        )

    default_status = (
        "no_action"
        if signal.issue in {"valid_split_payment", "unsupported_claim"}
        else "action_required"
    )
    if signal.issue == "insufficient_evidence":
        default_status = "needs_investigation"
    case_status = _rule_case_status(rule, default_status)
    facts = _build_case_facts(evidence_index)
    claim_assessments = _build_claim_assessments(
        case=case,
        signal=signal,
        confidence=confidence,
        refund_amount=refund_amount,
        facts=facts,
    )
    responsible_parties = _rule_responsible_parties(rule, fallback_party, entity_scope)

    return {
        "schema_version": "day09-l3a-output-v2",
        "case_id": case_id,
        "assessment": {
            "primary_issue": signal.issue,
            "case_status": case_status,
            "confidence": confidence,
        },
        "affected_entities": entity_scope,
        "claim_assessments": claim_assessments,
        "root_cause_analysis": {
            "ranked_causes": [{"cause_code": signal.reason_code, "rank": 1}],
            "responsible_parties": responsible_parties,
        },
        "evidence_refs": list(signal.evidence_refs),
        "data_conflicts": conflicts,
        "financial_resolution": {
            "currency": "BRL",
            "recommended_refund_brl": refund_amount,
            "refund_lines": refund_lines,
        },
        "resolution_actions": [action],
    }


async def _call_mcp_with_retry(
    *,
    actor: str,
    tool_name: str,
    case_id: str,
    gateway: EvidenceGateway,
    trace: TraceWriter,
    evidence_index: EvidenceIndex,
    arguments: dict[str, str],
) -> dict[str, Any]:
    if not _tool_allowed(actor, tool_name):
        raise ValueError(f"{actor} is not allowed to call MCP tool {tool_name}")

    last_error: Exception | None = None
    for attempt in range(1, MAX_MCP_ATTEMPTS + 1):
        try:
            evidence = await gateway.call(tool_name, case_id=case_id, **arguments)
            evidence_ref = evidence_index.add(evidence)
            trace.emit(
                case_id=case_id,
                event_type="tool_result_consumed",
                actor=actor,
                tool_name=tool_name,
                evidence_refs=[evidence_ref],
                attributes={"attempt": attempt, "domain": evidence["domain"]},
            )
            return evidence
        except RuntimeError:
            raise
        except (OSError, TimeoutError) as exc:
            last_error = exc
            if attempt == MAX_MCP_ATTEMPTS:
                break
            await asyncio.sleep(0.25 * attempt)

    trace.emit(
        case_id=case_id,
        event_type="handoff",
        actor=actor,
        target=ACTOR_COORDINATOR,
        decision_code="MCP_RETRY_EXHAUSTED",
        attributes={"tool_name": tool_name, "attempts": MAX_MCP_ATTEMPTS},
    )
    raise RuntimeError(f"MCP tool {tool_name} failed after retries: {last_error}") from last_error


async def _try_collect_entity(
    *,
    actor: str,
    entity: str,
    entity_id: str,
    tool_name: str | None,
    gateway: EvidenceGateway,
    trace: TraceWriter,
    evidence_index: EvidenceIndex,
    result: SpecialistResult,
) -> None:
    case_id = evidence_index.case_id
    if tool_name is None:
        trace.emit(
            case_id=case_id,
            event_type="handoff",
            actor=actor,
            target=ACTOR_COORDINATOR,
            decision_code="TOOL_UNAVAILABLE",
            attributes={"entity": entity},
        )
        return

    argument_name = ENTITY_ARGUMENTS[entity]
    try:
        evidence = await _call_mcp_with_retry(
            actor=actor,
            tool_name=tool_name,
            case_id=case_id,
            gateway=gateway,
            trace=trace,
            evidence_index=evidence_index,
            arguments={argument_name: entity_id},
        )
    except RuntimeError as exc:
        result.notes.append(f"{tool_name} failed for {entity_id}: {exc}")
        trace.emit(
            case_id=case_id,
            event_type="handoff",
            actor=actor,
            target=ACTOR_COORDINATOR,
            decision_code="TOOL_CALL_FAILED",
            attributes={
                "tool_name": tool_name,
                "entity": entity,
            },
        )
        return

    result.add_entity(entity, entity_id)
    result.add_evidence(evidence)


async def _run_order_item_agent(
    *,
    case: dict[str, Any],
    discovered_tools: set[str],
    gateway: EvidenceGateway,
    trace: TraceWriter,
    evidence_index: EvidenceIndex,
) -> SpecialistResult:
    result = SpecialistResult(actor=ACTOR_ORDER_ITEM)
    result.merge_entities(_extract_entity_ids(case))
    tools = {
        "order_ids": _find_tool(discovered_tools, "get_order"),
        "order_items": _find_tool(discovered_tools, "get_order_items"),
        "product_context": _find_tool(discovered_tools, "get_product_context"),
        "sellers": _find_tool(discovered_tools, "get_sellers"),
    }

    for order_id in result.entities.get("order_ids", []):
        await _try_collect_entity(
            actor=ACTOR_ORDER_ITEM,
            entity="order_ids",
            entity_id=order_id,
            tool_name=tools["order_ids"],
            gateway=gateway,
            trace=trace,
            evidence_index=evidence_index,
            result=result,
        )
        await _try_collect_entity(
            actor=ACTOR_ORDER_ITEM,
            entity="order_ids",
            entity_id=order_id,
            tool_name=tools["order_items"],
            gateway=gateway,
            trace=trace,
            evidence_index=evidence_index,
            result=result,
        )
        await _try_collect_entity(
            actor=ACTOR_ORDER_ITEM,
            entity="order_ids",
            entity_id=order_id,
            tool_name=tools["product_context"],
            gateway=gateway,
            trace=trace,
            evidence_index=evidence_index,
            result=result,
        )
        await _try_collect_entity(
            actor=ACTOR_ORDER_ITEM,
            entity="order_ids",
            entity_id=order_id,
            tool_name=tools["sellers"],
            gateway=gateway,
            trace=trace,
            evidence_index=evidence_index,
            result=result,
        )
    return result


async def _run_payment_agent(
    *,
    case: dict[str, Any],
    initial_scope: dict[str, list[str]],
    discovered_tools: set[str],
    gateway: EvidenceGateway,
    trace: TraceWriter,
    evidence_index: EvidenceIndex,
) -> SpecialistResult:
    result = SpecialistResult(actor=ACTOR_PAYMENT)
    result.merge_entities(initial_scope)
    order_tools: list[str | None] = [
        _find_tool(discovered_tools, "get_order_payments"),
        _find_tool(discovered_tools, "get_payment_timeline"),
    ]
    if any(topic in {"refund_pending", "refund_failed"} for topic in _claim_topics(case)):
        order_tools.append(_find_tool(discovered_tools, "get_refund_timeline"))
    for order_id in result.entities.get("order_ids", []):
        for tool_name in order_tools:
            await _try_collect_entity(
                actor=ACTOR_PAYMENT,
                entity="order_ids",
                entity_id=order_id,
                tool_name=tool_name,
                gateway=gateway,
                trace=trace,
                evidence_index=evidence_index,
                result=result,
            )

    tool_name = _find_tool(discovered_tools, "get_payment")
    for payment_reference in result.entities.get("payment_references", []):
        await _try_collect_entity(
            actor=ACTOR_PAYMENT,
            entity="payment_references",
            entity_id=payment_reference,
            tool_name=tool_name,
            gateway=gateway,
            trace=trace,
            evidence_index=evidence_index,
            result=result,
        )
    return result


async def _run_shipment_agent(
    *,
    initial_scope: dict[str, list[str]],
    discovered_tools: set[str],
    gateway: EvidenceGateway,
    trace: TraceWriter,
    evidence_index: EvidenceIndex,
) -> SpecialistResult:
    result = SpecialistResult(actor=ACTOR_SHIPMENT)
    result.merge_entities(initial_scope)
    order_tool_name = _find_tool(discovered_tools, "get_shipment_summary")
    for order_id in result.entities.get("order_ids", []):
        await _try_collect_entity(
            actor=ACTOR_SHIPMENT,
            entity="order_ids",
            entity_id=order_id,
            tool_name=order_tool_name,
            gateway=gateway,
            trace=trace,
            evidence_index=evidence_index,
            result=result,
        )

    tool_name = _find_tool(discovered_tools, "get_shipment", "get_delivery", "get_tracking")
    for shipment_id in result.entities.get("shipment_ids", []):
        await _try_collect_entity(
            actor=ACTOR_SHIPMENT,
            entity="shipment_ids",
            entity_id=shipment_id,
            tool_name=tool_name,
            gateway=gateway,
            trace=trace,
            evidence_index=evidence_index,
            result=result,
        )
    return result


async def _run_policy_agent(
    *,
    case: dict[str, Any],
    discovered_tools: set[str],
    gateway: EvidenceGateway,
    trace: TraceWriter,
    evidence_index: EvidenceIndex,
) -> SpecialistResult:
    result = SpecialistResult(actor=ACTOR_POLICY)
    tool_name = _find_tool(discovered_tools, "get_policy")
    if tool_name is None:
        trace.emit(
            case_id=evidence_index.case_id,
            event_type="handoff",
            actor=ACTOR_POLICY,
            target=ACTOR_COORDINATOR,
            decision_code="TOOL_UNAVAILABLE",
            attributes={"entity": "policy"},
        )
        return result

    policy_version = case.get("policy_version")
    if not isinstance(policy_version, str) or not policy_version:
        policy_version = "EC_POLICY_V1"
    try:
        evidence = await _call_mcp_with_retry(
            actor=ACTOR_POLICY,
            tool_name=tool_name,
            case_id=evidence_index.case_id,
            gateway=gateway,
            trace=trace,
            evidence_index=evidence_index,
            arguments={"policy_version": policy_version},
        )
    except RuntimeError as exc:
        result.notes.append(f"{tool_name} failed for {policy_version}: {exc}")
        return result
    result.add_evidence(evidence)
    return result


def _draft_insufficient_evidence_output(
    *,
    case_id: str,
    specialist_results: Iterable[SpecialistResult],
    evidence_index: EvidenceIndex,
) -> dict[str, Any]:
    entity_scope = _merge_entity_scopes(*(result.entities for result in specialist_results))
    evidence_refs = evidence_index.refs()
    confidence = 0.25 if evidence_refs else 0.05
    return {
        "schema_version": "day09-l3a-output-v2",
        "case_id": case_id,
        "assessment": {
            "primary_issue": "insufficient_evidence",
            "case_status": "needs_investigation",
            "confidence": confidence,
        },
        "affected_entities": entity_scope,
        "claim_assessments": [],
        "root_cause_analysis": {
            "ranked_causes": [{"cause_code": "INSUFFICIENT_EVIDENCE", "rank": 1}],
            "responsible_parties": [{"party_type": "unknown", "party_id": None}],
        },
        "evidence_refs": evidence_refs,
        "data_conflicts": [],
        "financial_resolution": {
            "currency": "BRL",
            "recommended_refund_brl": 0,
            "refund_lines": [],
        },
        "resolution_actions": ["collect_additional_evidence"],
    }


def _assign_task(trace: TraceWriter, task: AgentTask) -> None:
    trace.emit(
        case_id=task.case_id,
        event_type="task_assigned",
        actor=task.sender,
        target=task.recipient,
        attributes=task.trace_attributes(),
    )


def _verify_output(
    *,
    case_id: str,
    output: dict[str, Any],
    trace: TraceWriter,
    evidence_index: EvidenceIndex,
) -> dict[str, Any]:
    extra_keys = sorted(set(output) - OUTPUT_TOP_LEVEL_KEYS)
    if extra_keys:
        trace.emit(
            case_id=case_id,
            event_type="verification_completed",
            actor=ACTOR_VERIFIER,
            decision_code="SCHEMA_REJECTED",
            attributes={"reason": "extra_top_level_keys", "keys": ",".join(extra_keys)},
        )
        raise ValueError(f"output contains fields outside l3a schema: {extra_keys}")

    if output.get("case_id") != case_id:
        trace.emit(
            case_id=case_id,
            event_type="verification_completed",
            actor=ACTOR_VERIFIER,
            decision_code="CASE_ID_MISMATCH",
        )
        raise ValueError(f"output case_id does not match input case_id {case_id}")

    evidence_index.require_known_refs(_collect_output_evidence_refs(output))
    _verify_policy_consistency(case_id=case_id, output=output, trace=trace)
    trace.contracts.validate_output(output, f"outputs/{case_id}.json")
    trace.emit(
        case_id=case_id,
        event_type="verification_completed",
        actor=ACTOR_VERIFIER,
        decision_code="OUTPUT_ACCEPTED",
    )
    return output


def _verify_policy_consistency(
    *, case_id: str, output: dict[str, Any], trace: TraceWriter
) -> None:
    primary_issue = output["assessment"]["primary_issue"]
    case_status = output["assessment"]["case_status"]
    refund_amount = output["financial_resolution"]["recommended_refund_brl"]
    refund_lines = output["financial_resolution"]["refund_lines"]
    responsible_parties = output["root_cause_analysis"]["responsible_parties"]
    party_types = {party["party_type"] for party in responsible_parties}

    if refund_amount > 0 and primary_issue not in REFUND_ISSUES:
        trace.emit(
            case_id=case_id,
            event_type="verification_completed",
            actor=ACTOR_VERIFIER,
            decision_code="REFUND_ISSUE_MISMATCH",
        )
        raise ValueError(f"{primary_issue} cannot recommend a refund")

    if refund_amount == 0 and refund_lines:
        trace.emit(
            case_id=case_id,
            event_type="verification_completed",
            actor=ACTOR_VERIFIER,
            decision_code="REFUND_TOTAL_MISMATCH",
        )
        raise ValueError("refund lines require a positive recommended_refund_brl")

    line_total = round(sum(line["amount_brl"] for line in refund_lines), 2)
    if abs(line_total - refund_amount) > MONEY_TOLERANCE_BRL:
        trace.emit(
            case_id=case_id,
            event_type="verification_completed",
            actor=ACTOR_VERIFIER,
            decision_code="REFUND_TOTAL_MISMATCH",
        )
        raise ValueError("refund line total does not match recommended_refund_brl")

    expected_party = ISSUE_RESPONSIBILITY[primary_issue][0]
    if expected_party not in party_types:
        trace.emit(
            case_id=case_id,
            event_type="verification_completed",
            actor=ACTOR_VERIFIER,
            decision_code="RESPONSIBLE_PARTY_MISMATCH",
        )
        raise ValueError(f"{primary_issue} must include responsible party {expected_party}")

    if (
        primary_issue in REFUND_ISSUES
        and primary_issue != "refund_pending"
        and case_status != "action_required"
    ):
        trace.emit(
            case_id=case_id,
            event_type="verification_completed",
            actor=ACTOR_VERIFIER,
            decision_code="CASE_STATUS_MISMATCH",
        )
        raise ValueError(f"{primary_issue} requires action_required status")


async def solve_case(
    case: dict[str, Any], gateway: EvidenceGateway, trace: TraceWriter
) -> dict[str, Any]:
    """Run the L3A coordinator and specialist-agent workflow.

    The starter kit intentionally does not generate a fallback answer: submitting an
    invented answer or evidence reference would violate the competition contract.
    """
    case_id = _case_id(case)
    evidence_index = EvidenceIndex(case_id=case_id)
    discovered_tools = set(await gateway.list_tools())
    initial_scope = _extract_entity_ids(case)

    for recipient, task_name in (
        (ACTOR_ORDER_ITEM, "collect_order_item_evidence"),
        (ACTOR_PAYMENT, "collect_payment_evidence"),
        (ACTOR_SHIPMENT, "collect_shipment_evidence"),
        (ACTOR_POLICY, "collect_policy_evidence"),
    ):
        _assign_task(
            trace,
            AgentTask(
                case_id=case_id,
                sender=ACTOR_COORDINATOR,
                recipient=recipient,
                task=task_name,
                entity_scope=initial_scope,
            ),
        )

    order_item_result = await _run_order_item_agent(
        case=case,
        discovered_tools=discovered_tools,
        gateway=gateway,
        trace=trace,
        evidence_index=evidence_index,
    )
    expanded_scope = _merge_entity_scopes(initial_scope, order_item_result.entities)

    payment_result = await _run_payment_agent(
        case=case,
        initial_scope=expanded_scope,
        discovered_tools=discovered_tools,
        gateway=gateway,
        trace=trace,
        evidence_index=evidence_index,
    )
    shipment_result = await _run_shipment_agent(
        initial_scope=_merge_entity_scopes(expanded_scope, payment_result.entities),
        discovered_tools=discovered_tools,
        gateway=gateway,
        trace=trace,
        evidence_index=evidence_index,
    )
    policy_result = await _run_policy_agent(
        case=case,
        discovered_tools=discovered_tools,
        gateway=gateway,
        trace=trace,
        evidence_index=evidence_index,
    )

    trace.emit(
        case_id=case_id,
        event_type="handoff",
        actor=ACTOR_COORDINATOR,
        target=ACTOR_POLICY,
        decision_code="SPECIALIST_EVIDENCE_COLLECTED",
        evidence_refs=evidence_index.refs() or None,
    )

    specialist_results = [order_item_result, payment_result, shipment_result, policy_result]
    draft = _build_policy_output(
        case=case,
        case_id=case_id,
        specialist_results=specialist_results,
        evidence_index=evidence_index,
    )
    trace.emit(
        case_id=case_id,
        event_type="policy_decided",
        actor=ACTOR_POLICY,
        decision_code=draft["assessment"]["primary_issue"].upper(),
        evidence_refs=evidence_index.refs() or None,
        attributes={
            "confidence": draft["assessment"]["confidence"],
            "case_status": draft["assessment"]["case_status"],
        },
    )
    return _verify_output(
        case_id=case_id,
        output=draft,
        trace=trace,
        evidence_index=evidence_index,
    )
