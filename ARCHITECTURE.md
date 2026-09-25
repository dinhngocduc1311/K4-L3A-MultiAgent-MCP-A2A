# L3A Architecture Record

This document records the observable multi-agent design for the Day09 L3A
submission. It must stay aligned with the public JSON Schemas in
`contracts/schemas/`. Do not document private prompts, hidden reasoning, API
keys, or any field that is not accepted by the public contracts.

## 1. System overview

```text
                          +--------------------------+
                          |   Coordinator / Router   |
                          +-------------+------------+
                                        | handoff
         +------------------------------+------------------------------+
         v                              v                              v
+------------------+           +------------------+           +------------------+
| Order/Item Agent |           |  Payment Agent   |           | Shipment Agent  |
+--------+---------+           +--------+---------+           +--------+---------+
         |                              |                              |
         +------------------------------+------------------------------+
                                        | MCP evidence collector
                                        v
                               +------------------+
                               |   Policy Agent   |
                               +--------+---------+
                                        |
                                        v
                               +------------------+
                               |  Verifier Agent  |
                               +--------+---------+
                                        | validated output
                                        v
                                  outputs/<case_id>.json
```

Execution starts from `inputs/<case_id>.json`. The coordinator routes a case to
specialists, each specialist collects only the evidence in its domain through
the MCP gateway, and the verifier returns the final case output only after the
result passes `l3a-output-v2.schema.json`.

The public contracts are the source of truth:

| Contract | Purpose | Enforcement |
| --- | --- | --- |
| `l3a-output-v2.schema.json` | Final output for each L3A case | `Contracts.validate_output` before writing `outputs/<case_id>.json` |
| `trace-event-v1.schema.json` | Observable trace events | `TraceWriter.emit` validates every event before appending |
| `submission-manifest-v2.schema.json` | Submission package manifest | `Contracts.validate_manifest` during packaging |
| `mcp-evidence-response-v1.schema.json` | MCP evidence envelope | `EvidenceGateway.call` validates every tool response |

No additional output, trace, manifest, or evidence fields are allowed. If code,
documentation, or model behavior conflicts with a JSON Schema, the JSON Schema
wins.

## 2. Agent ownership

| Actor | Input | Responsibilities | Tool permissions | Output/handoff |
| --- | --- | --- | --- | --- |
| Coordinator | Raw case JSON and discovered MCP tool names | Normalize case scope, identify requested entities, assign specialist tasks, keep `case_id` correlation, prevent circular handoffs | Tool discovery only; no domain evidence calls unless explicitly acting as evidence collector | Specialist tasks with bounded scope and expected domains |
| Order/Item Agent | Case text, order hints, item hints | Confirm order status, item membership, seller/product links, cancellation or unavailability signals | Order, item, seller, product tools only | Order/item findings, entity IDs, evidence refs |
| Payment Agent | Payment hints and order IDs | Confirm payment events, split payments, duplicate charges, refund state, amount consistency | Payment and refund tools only | Payment findings, refund candidates, evidence refs |
| Shipment Agent | Shipment hints and order IDs | Confirm delivery estimates, actual delivery, logistics status, delay attribution signals | Shipment/logistics tools only | Shipment findings, delay facts, evidence refs |
| Policy Agent | Specialist findings and evidence refs | Apply competition policy to supported facts, classify primary issue, choose responsible parties and actions | Policy tools only | Policy decision and resolution recommendations |
| Verifier Agent | Draft output, evidence index, trace summary | Enforce schema, entity scope, evidence ownership, confidence bounds, and output consistency | No MCP calls by default; may request a bounded specialist retry | Validated output or a rejection reason |

Each actor may consume prior handoff messages for the same `case_id` only. No
agent may reuse evidence from another case, another run, or a manually created
`evidence_ref`.

## 3. A2A protocol

Agents exchange internal Python dictionaries with a fixed envelope:

```text
{
  "case_id": "...",
  "from": "coordinator",
  "to": "payment-agent",
  "task": "collect_payment_evidence",
  "attempt": 1,
  "entity_scope": {"order_ids": [...], "payment_references": [...]},
  "evidence_refs": [...]
}
```

The envelope is an internal protocol, not part of the submitted output. Trace
events record only observable state transitions: `task_assigned`, `handoff`,
`tool_result_consumed`, `policy_decided`, and `verification_completed`.

Handoff rules:

- `case_id` is mandatory on every message and must match the current input case.
- Each specialist receives one initial task and at most one verifier-requested
  retry for missing or inconsistent evidence.
- A specialist may hand off to the policy agent only through the coordinator's
  shared evidence bundle.
- The verifier cannot call arbitrary tools; it either validates the draft or
  requests a bounded retry from the responsible specialist.
- The coordinator stops the workflow on invalid schema, cross-case evidence, or
  exhausted retries.

## 4. Evidence lifecycle

All evidence enters the system through `EvidenceGateway.call`. The gateway
validates the MCP envelope against `mcp-evidence-response-v1.schema.json` before
returning it to workflow code.

Lifecycle:

1. The specialist calls an allowed MCP tool with the current `case_id` and
   explicit entity arguments.
2. The returned envelope is kept as immutable evidence: `schema_version`,
   `evidence_ref`, `result_hash`, `domain`, `data`, and optional `warnings`.
3. The specialist stores `evidence_ref` in a per-case evidence index.
4. The trace writer emits `tool_result_consumed` with the tool name and the
   exact `evidence_ref` values used by the specialist.
5. Policy decisions and final output cite only refs present in the per-case
   index.
6. The verifier rejects any final result containing refs that were not returned
   by MCP for the same `case_id`.

Evidence is never edited, regenerated, shortened, or guessed. Derived claims can
be recomputed, but the original MCP envelope remains the only provenance source.

## 5. Failure policy

| Failure | Retry? | Fallback | Trace event/code |
| --- | --- | --- | --- |
| MCP timeout or transient transport error | Yes, up to 2 retries with bounded backoff for idempotent reads | Mark the affected claim as `insufficient_evidence` if retries fail | `handoff` / `MCP_RETRY_EXHAUSTED` |
| MCP tool not found after discovery | No | Skip that specialist capability and keep only supported evidence | `handoff` / `TOOL_UNAVAILABLE` |
| Entity not found | No automatic retry unless another evidence source names a corrected ID | Keep entity out of `affected_entities` and mark claim unsupported or insufficient | `tool_result_consumed` or `handoff` / `ENTITY_NOT_FOUND` |
| Source conflict | One targeted retry only if conflict may be stale or partial | Add `data_conflicts` and choose `selected_source` only when evidence supports it | `handoff` / `SOURCE_CONFLICT` |
| Invalid specialist result | One verifier-directed retry | Drop unsupported claim fields and finalize only schema-safe facts | `verification_completed` / `SPECIALIST_RESULT_INVALID` |
| Final output schema violation | No | Stop the case; do not write an invalid output | `verification_completed` / `SCHEMA_REJECTED` |

Retries are allowed only for idempotent evidence reads. Missing evidence must not
be converted into guessed business facts.

## 6. Verification invariants

Before `solve_case` returns, the verifier checks:

- The output contains only fields allowed by `l3a-output-v2.schema.json`.
- `schema_version` is `day09-l3a-output-v2` and `case_id` equals the input case.
- Every ID in `affected_entities` is supported by evidence from this case.
- Every `evidence_ref` in the output exists in the per-case evidence index.
- `claim_assessments[*].evidence_refs` are subsets of the top-level evidence
  refs and directly support the related claim.
- Refund line totals equal `financial_resolution.recommended_refund_brl` after
  currency-safe rounding.
- `assessment.primary_issue`, `root_cause_analysis.responsible_parties`, and
  `resolution_actions` are mutually consistent.
- `confidence` values are between 0 and 1 and decrease when evidence is missing
  or conflicting.
- `data_conflicts` are present when accepted evidence disagrees on a material
  field.

## 7. Reproducibility

Runtime command sequence:

```powershell
python -m pip install -e ".[dev]"
day09 validate-inputs
day09 mcp-tools
day09 run
day09 validate
day09 package --output dist/submission.zip
```

Operational settings:

- Python version: 3.11 or newer, pinned by `pyproject.toml`.
- Dependencies: installed from `pyproject.toml`; no undeclared runtime package
  should be required.
- Concurrency: default implementation processes cases sequentially to keep MCP
  audit trails easy to inspect. If concurrency is added, cap it and preserve
  per-case trace ordering.
- Randomness: avoid random decisions. If stochastic ranking is introduced, set
  and log a deterministic seed outside the submitted artifacts.
- Secrets: `.env` may contain `COMPETITION_TEAM_API_KEY`, but `.env` and API
  keys must never be committed or packaged.
