# T1178 Live Gibson Ecology Correction

## Miss Cause

T1178 was previously treated as product-ready using scheduled and triggered runtime records whose agent decisions came from a recorded T1137 agent-run artifact. That proved scheduler control flow, lease/backoff handling, bounded packet recording, and mechanical write mediation, but it did not prove Flynn's required ecology invariant: live configured agents must run model inference over soup contents and produce candidate mutations or other soup contents through mediated writes.

The prior proof path exposed the miss in these surfaces:

- `runtime_target: :local_recorded_agent_route`
- YAML runner `recorded-agent-artifact`
- `decision_source: :recorded_agent_transcript`
- producer evidence from `proof-harness/priv/agent_runs/t1137_agentic_meaning_run.json`
- prompt hashes without promoted prompt bodies in the T1178 registry proof

Those artifacts remain useful for T1137 regression coverage, but they cannot be counted as T1178 product-readiness evidence.

## Architecture Fit

The corrected T1178 slice keeps the scheduler, trigger, packet, lease, cascade-budget, and write-mediation substrate, but changes the agent proof route:

- Agent definitions now promote prompt bodies and prompt hashes into registry/YAML records.
- T1178 agent runs call Gibson's live Qwen route at `http://gibson:8080/v1/chat/completions`.
- Gibson currently serves `Qwen3.6-35B-A3B-UD-Q4_K_M.gguf` through `llama-server`.
- Bounded soup packets are serialized as JSON-safe packet payloads before inference.
- Model outputs are parsed as typed decisions, candidate mutations, authoring deltas, abstentions, or no-op reviews.
- Candidate mutations still pass through `Primeradiant.Mediation.WriteGate`; mediation remains schema/evidence/permission/visibility/atomicity containment and does not become truth arbitration.

The implementation still does not deploy services, install autostart wiring, mutate source DBs, or mutate production runtime state. This means local product-valid live-agent proof can be shown, while deployed soak proof remains a separate lifecycle step requiring Flynn authorization.

## Tracker Reconciliation

Tracker currently reports T1178 as `Verified`. Flynn's bounceback means that state cannot honestly stand as product-ready evidence for the corrected invariant.

The supported `tracker-state-core` context command did not find T1178 markdown:

```text
ticket_markdown_not_found
```

The DB-backed Tracker API reported `canonical_state: "Verified"`. No supported transition path was found in the available state-transition guidance for moving `Verified` back to implementation/review. No raw Tracker DB edit was made.

Required reconciliation is therefore blocked on a supported Tracker transition or dashboard/API bounceback path that can move T1178 out of `Verified` with proof of Flynn's rejection.

## Remaining Product-Proof Gap

The corrected proof demonstrates live Gibson/Qwen model inference in the local proof harness without recorded artifacts. It does not prove deployed production runtime behavior, persistent scheduling, source-emitter registration, or production soup mutation. Those remain out of scope unless Flynn explicitly authorizes deployment/runtime mutation in the conversation.
