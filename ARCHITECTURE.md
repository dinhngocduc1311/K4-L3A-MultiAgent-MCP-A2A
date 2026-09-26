# L3A Architecture Record

## 1. System overview

```text
Input -> Coordinator
          |-- order/item agent --|
          |-- payment agent -----|-> policy agent -> verifier -> output
          `-- shipment agent ----|         |              |
                    MCP evidence +---------+--------------+-> trace.jsonl
```

`solve_case` là một async state-machine thuần Python. Coordinator khám phá tool từ
MCP Gateway, phân quyền theo domain, phát task và nhận handoff. Không agent nào dùng
customer message làm ground truth. Verifier kiểm tra JSON Schema trước khi trả output;
CLI chỉ ghi file sau khi bước này thành công.

## 2. Agent ownership và tool permissions

| Actor | Input | Trách nhiệm | Tool được phép | Output/handoff |
| --- | --- | --- | --- | --- |
| Coordinator | case, danh sách tool đã discover | định tuyến, giới hạn 30 evidence refs | `list_tools`, không gọi evidence tool | task cho specialist; candidate đã verify |
| Order/item agent | order/item identifiers | trạng thái order, item, seller, product và entity scope | tool có domain order/item/seller/product | evidence envelope sang policy |
| Payment agent | order/payment identifiers | capture, split payment, duplicate charge, refund | tool có domain payment/refund/charge | evidence envelope sang policy |
| Shipment agent | order/shipment identifiers | timeline giao hàng và nguồn gây trễ | tool có domain shipment/delivery/logistics | evidence envelope sang policy |
| Policy agent | specialist evidence, preliminary issue | áp policy cho issue candidate, cung cấp action/refund | tool có domain policy/rule | candidate sang verifier |
| Verifier agent | candidate + evidence refs | schema, invariants và consistency | không gọi MCP | validated output sang coordinator |

Tên tool không được hard-code. `EvidenceGateway.tool_specs()` trả description cùng
input/output schema đã discover; workflow định tuyến từ metadata, chỉ truyền field có
trong input schema và bỏ qua tool nếu thiếu required argument. Tool có optional filter
nhưng không resolve được entity cũng bị bỏ qua để tránh query rộng. Một tool chỉ được
gán cho một actor, ưu tiên policy, payment, shipment rồi order/item. Sau mỗi call,
workflow kiểm tra `domain` thực tế trong evidence envelope vẫn thuộc quyền actor; sai
domain làm fail case trước khi emit `tool_result_consumed`.

Gateway validate toàn bộ payload bằng input JSON Schema đã discover trước network call.
Plural IDs được fan-out thành từng call có scope; array arguments vẫn được gửi nguyên
mảng. Trong mỗi agent, calls round-robin theo tool để một tool nhiều entity không chặn
các tool còn lại.

## 3. A2A protocol

A2A envelope nội bộ gồm `case_id`, actor nguồn/đích, decision code và danh sách
evidence envelope nguyên bản. `case_id` là correlation key duy nhất. Luồng hợp lệ:

1. Coordinator phát `task_assigned` cho ba specialist.
2. Mỗi specialist gọi các tool thuộc quyền, phát `tool_result_consumed`, rồi `handoff`
   sang policy agent.
3. Policy phát `policy_decided` và handoff một lần sang verifier.
4. Verifier phát `verification_completed`; CLI phát `case_finalized` sau khi ghi output.

State-machine không có cạnh quay lại nên không thể lặp vô hạn. Mỗi tool có timeout 45
giây và tối đa hai lần gọi (một lần đầu, một retry transient sau 0.2 giây). Trace chỉ
chứa event/decision quan sát được, không chứa prompt hay chain-of-thought.

## 4. Evidence lifecycle

Gateway validate mọi response bằng `mcp-evidence-response-v1.schema.json` trước khi
workflow nhìn thấy dữ liệu. Workflow giữ nguyên `evidence_ref`, không tự sinh hoặc sửa,
deduplicate theo thứ tự và giới hạn đúng schema. Evidence được consume với cùng
`case_id`, gắn vào trace ngay tại actor gọi tool, rồi mới được dùng để phân loại issue,
entity, trách nhiệm và tài chính. Output chỉ cite domain trực tiếp hỗ trợ issue và policy,
không cite mọi tool result. State chỉ sống trong một lần `solve_case`, vì vậy evidence
không thể rò sang case khác.

Gateway duy trì ledger `{evidence_ref: (case_id, result_hash)}` cho toàn bộ run. Ref lạ,
ref đổi hash hoặc ref được tái sử dụng ở case khác bị verifier từ chối. Ledger client chỉ
là guard sớm; MCP server vẫn là nguồn audit độc lập cho hash, latency và status.

## 5. Failure policy

| Failure | Retry? | Fallback | Trace/result |
| --- | --- | --- | --- |
| timeout/connect/transport | 1 lần, cùng tool và argument | fail case sau lần 2 | timeout 45 giây/call; không tạo evidence giả |
| MCP tool error/not found | không | bỏ qua result lỗi, tiếp tục evidence độc lập | không emit consumed, không tạo evidence giả |
| payload sai input schema | không gửi request | reject sớm tại Gateway | không tạo audit giả |
| thiếu required tool argument | không gọi tool | tiếp tục domain khác | verifier có thể trả `insufficient_evidence` |
| source conflict | không retry | evidence có thẩm quyền và policy quyết định | `policy_decided` |
| invalid evidence envelope | không | Gateway/JSON Schema reject | không emit consumed event |
| evidence ref lạ/cross-case/đổi hash | không | reject case | không finalize output |
| invalid specialist/final candidate | không | verifier reject case | không emit verification/finalized |

Retry chỉ áp dụng read-only evidence calls nên idempotent. Không retry lỗi nghiệp vụ và
không biến missing evidence thành dữ liệu phỏng đoán.

## 6. Verification invariants

Trước finalize, verifier bắt buộc kiểm tra:

- output chỉ có field được `l3a-output-v2.schema.json` cho phép;
- `case_id` và entity IDs thuộc case/evidence hiện tại;
- mọi `evidence_ref` đến từ MCP response đã validate, có trong gateway ledger, đúng
  `case_id` và không trùng;
- kết luận có đủ nhóm domain bắt buộc và chỉ cite evidence hỗ trợ issue/claim;
- issue, case status, root cause, responsible party và resolution action nhất quán;
- refund không âm; tổng refund line bằng recommended refund khi có line;
- confidence nằm trong `[0, 1]`;
- claim chỉ cite ref MCP đã consume đúng case; claim refs độc lập với evidence của primary issue;
- trace prefix có receive, assignment, evidence consumption, handoff và policy đúng thứ
  tự; CLI chỉ phát finalize sau khi verifier thành công.

JSON Schema trong `contracts/schemas/` là nguồn chân lý cao nhất. Các schema public
được giữ nguyên, không thêm field ngoài contract.

## 7. Policy engine, calibration and lifecycle closure

Policy classification uses authoritative structured evidence only. Its deterministic
priority is refund failure/pending, duplicate charge, canceled or unavailable paid order,
payment mismatch, late-delivery ownership, insufficient evidence, valid split payment,
then unsupported claim. A payment is counted once per identifier family, so one payment
carrying both an ID and a sequence cannot be misclassified as split payment.
For multi-order cases, evidence is partitioned into connected components using transaction
identifiers (order/item/payment/refund/shipment); seller identity never joins two orders.
The primary issue is selected from per-component assessments, so a canceled order cannot
borrow a captured payment or delivery timeline from another order.

The issue-to-status and issue-to-responsible-party matrix is closed over all 11 values in
the output contract. Seller-caused delay/unavailability maps to seller; logistics delay to
logistics provider; payment/refund faults to payment provider; canceled-paid handling to
platform; valid/no-action outcomes to the customer; and investigation outcomes to unknown.
The verifier rejects mixed or duplicate parties, out-of-scope seller IDs, inconsistent root
causes and no-action outputs that contain refunds. All relevant seller IDs are retained (up to the schema cap),
and policy-provided parties/actions are accepted only from evidence explicitly scoped to the
same issue. Action codes reserved for a different issue are discarded.

Financial precedence is authoritative policy total, policy refund lines, issue-specific
refund amount, then a deterministic evidence calculation. BRL arithmetic uses Decimal,
non-finite values are ignored, policy lines are capped by schema, and line totals must equal
the recommended refund. Investigation/no-action outputs are forced to zero refund. No-action
outputs retain the authoritative policy action (for example `document_no_action`);
investigation uses the matching authoritative policy action, with manual review only for
insufficient evidence.
Payment totals are deduplicated per authoritative payment identity and summed across split
payments. A monetary issue without enough amount evidence is downgraded to investigation.
The verifier independently rebuilds financial resolution, actions and responsible parties
from cited evidence and requires exact equality with the candidate output.

Confidence is capped at 0.97 and calibrated from required-domain coverage, presence of
policy evidence, MCP warnings, resolved conflicts and unresolved conflicts. Insufficient
evidence is capped at 0.35. An unresolved conflict changes the outcome to investigation;
the verifier recomputes the evidence-quality ceiling and rejects overconfident output.
Besides consuming explicit conflict envelopes, the policy engine compares status, monetary
totals and delivery timestamps only when domain and authoritative entity ID match;
contradictory snapshots for the same entity remain unresolved, while different entities are
never treated as a conflict.

Packaging performs the final lifecycle check after the CLI has emitted `case_finalized`.
For every case it requires the ordered subsequence `case_received -> task_assigned ->
tool_result_consumed -> handoff -> policy_decided -> verification_completed ->
case_finalized`, exactly one receive/finalize boundary, and linkage from every submitted
evidence ref to a consumed MCP result. It also validates actor, target and tool-name
semantics, requires assignments to all specialists plus policy, requires each specialist
handoff to policy and the final policy handoff to verifier, and permits exactly one policy
decision and verification completion.

## 8. Reproducibility

- Python: 3.11+; dependency bounds nằm trong `pyproject.toml`.
- Framework: Python async state-machine, không model và không random seed.
- Session: CLI giữ một MCP session cho trọn batch 100 case như starter; các case chạy
  tuần tự để audit cùng một run. Các read-only calls độc lập trong một specialist vẫn
  chạy đồng thời.
- Resource bounds: tối đa 30 successful evidence results mỗi case, chia quota
  Order/Item=12, Payment=8, Shipment=7, Policy=3; tối đa 20 entity argument sets/tool và
  hai attempts/call (tối đa 60 MCP-audited attempts khi tất cả lần đầu đều transient).
  Tool discovery được cache một lần trên mỗi gateway session.
- Chạy: `day09 validate-inputs`, `day09 run`, `day09 validate`, rồi
  `day09 package --output dist/submission.zip`.
- Runtime config chỉ lấy từ `.env`; API key không ghi vào output, trace hoặc tài liệu.
