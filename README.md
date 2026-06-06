# Prime Radiant

Prime Radiant is experimental agentic soup work: a research/prototype system for source-bound evidence admission, story-state evolution, and model-agent meaning work over a shared soup substrate.

## Experimental Status

This repository is public so the work can be inspected, shared, and linked. It is not a generalized product distribution.

The code and documents here are specific to the current experimental OpenClaw/Prime Radiant environment. They assume local host topology, source adapters, runtime conventions, model routes, proof harnesses, and operational constraints that are not portable to other deployments without redesign.

In particular:

- Do not treat this as a drop-in application or production deployment template.
- Do not assume the runtime, adapter, storage, or agent topology applies outside the original environment.
- Do not treat proof harnesses or bounded experimental runs as general product guarantees.
- Expect APIs, schemas, prompts, and architecture documents to move while the product shape is still being discovered.

## Repository Shape

- `architecture/` contains architecture papers and implementation direction.
- `product/` contains product-slice plans and acceptance framing.
- `specs/` contains source-of-truth specifications for current slices.
- `proof-harness/` and `storage-harness/` contain Elixir harnesses used to prove bounded behavior.
- `whitepaper/` contains longer-form explanatory material.

## Current Intent

The active product direction is not a conventional feed processor. Prime Radiant is meant to prove an agentic soup ecology: source ingest admits evidence, while agents perform meaning work over soup contents and contribute candidate mutations or mediated mutations back into the soup.

That distinction matters. Deterministic lifecycle, scheduler, replay, storage, or metadata evidence is substrate proof only; it is not proof of actual agentic meaning work.
