# Prime Radiant

Prime Radiant is an experiment in helping a computer keep up with what is happening.

The idea is simple: news, messages, notes, browser captures, and other source systems keep producing evidence. Prime Radiant should take that evidence in without pretending to understand more than it does, keep a living soup of facts and story state, and let agents work inside that soup.

Those agents should not just sort a feed. They should notice what changed, connect it to what was already known, ask for more evidence when the link is weak, and write back new or changed knowledge. The useful output is not "here are more items." It is "this story changed, here is why, and here is the evidence."

That is the spirit of the project.

## Experimental Status

This repo is public so the work can be inspected, shared, and linked.

It is still experimental. It is not a finished app, a package, or a deployment template.

The code and docs are tied to the current OpenClaw/Prime Radiant setup: the machines, source adapters, local paths, model routes, proof harnesses, and operating rules all come from that environment. Running this somewhere else would take real redesign, not just configuration.

In particular:

- Do not treat this as a drop-in application or production deployment template.
- Do not assume the runtime, adapters, storage, or agent layout fit another deployment.
- Do not treat proof harnesses as proof that the whole product works.
- Expect the APIs, schemas, prompts, and architecture to change while the shape is still being found.

## What It Is Trying To Build

Prime Radiant is trying to become a living story system:

- Source systems provide evidence.
- Prime Radiant admits that evidence into a shared soup.
- Agents read the soup, do meaning work, and write back candidate changes.
- The system keeps track of what changed, what stayed uncertain, and what evidence supports each claim.
- The final output should help a person understand movement in the world, not just consume more raw feed items.

The hard line is important: ingest is not allowed to invent story meaning. It can admit evidence. The agents own the meaning work.

## Repository Shape

- `architecture/` contains architecture notes and implementation direction.
- `product/` contains product slice plans and acceptance framing.
- `specs/` contains source-of-truth specifications for current slices.
- `proof-harness/` and `storage-harness/` contain Elixir harnesses for bounded behavior checks.
- `whitepaper/` contains longer explanations of the system idea.

## Current Reality

This repo contains specs, architecture documents, proof harnesses, storage work, and early runtime pieces. Some parts are real code. Some parts are proof slices. Some parts are still documents that define what the next code should prove.

The project is not done until real agents are doing useful work in the soup and the system can show the resulting story changes with evidence.
