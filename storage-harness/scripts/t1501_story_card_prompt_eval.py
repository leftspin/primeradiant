#!/usr/bin/env python3
import json
import re
import signal
import subprocess
import sys
import time
import urllib.request

ENDPOINT = "http://gibson:8080/v1/chat/completions"
MODEL = "Qwen3.6-35B-A3B-UD-Q4_K_M.gguf"
REQUEST_TIMEOUT_SECONDS = 180

SYSTEM_PROMPT = """You are a Prime Radiant story agent. Use only the bounded packet supplied in the user message.
Source admission is evidence only; you own story/meaning output only when it is packet-grounded.
Return exactly one JSON object matching the requested schema."""

BASELINE_PROMPT = """Maintain the current living Prime Radiant story synopsis artifact from committed story state and linked source evidence.
Return deck, summary, key_claims, source_coverage, contribution explanations, salience hints, and changed fields.
For every article in committed_story_state.linked_sources, return exactly one matching source_coverage row keyed by source_ref.
Do not cover only the newest source_ref. The synopsis source coverage list is incomplete unless every linked source_ref has source_coverage.
Each source_coverage row must include contribution_reason.text: a concise plain-language explanation of why that article matters to this story, suitable for a reader-facing News source popup.
Use contribution_reason unavailable/refused with a specific non-sentinel reason only when the linked article text truly cannot support a reader-facing salience explanation.
Examples: "Adds the funding amount and names the investor" or "Provides the primary quote from the company."
If any required source display or synopsis/evidence artifact field is unavailable, return explicit unavailable/refused/incomplete field state with reason and provenance.
If the packet includes source_coverage_repair_request, repair the prior omission by returning source_coverage for every missing source_ref."""

def production_story_synthesis_contract():
    raw = subprocess.check_output([
        "mix",
        "run",
        "-e",
        "IO.puts(Jason.encode!(Primeradiant.StorageHarness.LiveStoryAgentLoop.story_synthesis_eval_contract()))"
    ], cwd=".", text=True)
    return json.loads(raw)


BASELINE_SCHEMA = {
    "status": "complete | incomplete | refused | unavailable",
    "title": {"text": "string", "state": "complete | unavailable", "provenance_refs": ["string"]},
    "deck": {"text": "string or null", "state": "complete | unavailable", "reason": "string or null", "provenance_refs": ["string"]},
    "summary": {"text": "string or null", "state": "complete | unavailable", "reason": "string or null", "provenance_refs": ["string"]},
    "key_claims": [{
        "claim_ref": "claim ref",
        "text": "claim text",
        "status": "current | disputed | stale | background | unresolved",
        "materiality": "material | background | unresolved",
        "evidence_refs": ["evidence ref"],
        "conflict_refs": ["conflict ref"],
        "uncertainty": {"state": "known | unavailable", "reason": "string or null"},
        "appears_in_current_synopsis": True
    }],
    "source_coverage": [{
        "source_ref": "must exactly match one committed_story_state.linked_sources[].source_ref; include one row for every linked source_ref",
        "contribution_reason": {"text": "concise reader-facing reason this article matters to the story", "state": "complete | unavailable | refused", "reason": "string or null", "provenance_refs": ["string"]},
        "materiality": "material | nonmaterial",
        "source_posture": {"state": "complete | unavailable | refused", "value": "string or null", "reason": "string or null"},
        "source_weight": {"state": "complete | unavailable | refused", "value": "number or null", "reason": "string or null"}
    }],
    "topic_salience": {
        "salience_explanation": {"text": "story-to-topic salience explanation", "state": "complete", "provenance_refs": ["string"]},
        "global_salience": "hint",
        "flynn_priority": "hint"
    },
    "changed_field_keys": ["field_key"],
    "change_summary": {"text": "story-agent-authored change summary", "state": "complete", "provenance_refs": ["string"]},
    "field_completeness": {}
}

VARIANT_SCHEMA = {
    "status": "complete | refused",
    "title": {"text": "string", "state": "complete", "provenance_refs": ["string"]},
    "exact_happening": {"text": "string", "state": "complete", "provenance_refs": ["string"]},
    "deck": {"text": "string", "state": "complete", "provenance_refs": ["string"]},
    "summary": {"text": "string", "state": "complete", "provenance_refs": ["string"]},
    "key_claims": [{
        "claim_ref": "string",
        "text": "string",
        "status": "current | disputed | stale | background | unresolved",
        "materiality": "material | background | unresolved",
        "evidence_refs": ["string"],
        "conflict_refs": ["string"],
        "uncertainty": {"state": "known | unavailable", "reason": "string or null"},
        "appears_in_current_synopsis": True
    }],
    "source_links": [{"source_ref": "string", "evidence_refs": ["string"]}],
    "source_coverage": [{
        "source_ref": "string",
        "contribution_reason": {"text": "string", "state": "complete", "provenance_refs": ["string"]},
        "materiality": "material | nonmaterial",
        "source_posture": {"state": "complete", "value": "string"},
        "source_weight": {"state": "complete", "value": "number"}
    }],
    "topic_salience": {
        "salience_explanation": {"text": "string", "state": "complete", "provenance_refs": ["string"]},
        "global_salience": "string",
        "flynn_priority": "string"
    },
    "changed_field_keys": ["field_key"],
    "change_summary": {"text": "string", "state": "complete", "provenance_refs": ["string"]},
    "field_completeness": {}
}

def item_sources(item):
    sources = item.get("sources")
    if isinstance(sources, list) and sources:
        return sources
    return [{
        "source_ref": item["source_ref"],
        "article_ref": item["external_id"],
        "article_title": item["article_title"],
        "canonical_uri": item["canonical_uri"],
        "source_name": item["source_name"],
        "observed_at": item["observed_at"],
        "body_text": item["body_text"]
    }]

def packet(item):
    sources = item_sources(item)
    return {
        "packet_id": f"t1501:{item['item_id']}",
        "story_key": item["story_key"],
        "evidence_refs": item["evidence_refs"],
        "committed_story_state": {
            "title": item["story_title"],
            "state": "active",
            "version": item.get("version", 1),
            "structural_facts": item.get("structural_facts", {}),
            "background_facts": item.get("background_facts", {}),
            "topic_tokens": item.get("topic_tokens", []),
            "linked_sources": [{
                "source_ref": source["source_ref"],
                "article_ref": source.get("article_ref") or source.get("external_id"),
                "article_title": source.get("article_title"),
                "canonical_uri": source.get("canonical_uri"),
                "source_name": source.get("source_name"),
                "observed_at": source.get("observed_at"),
                "excerpt": (source.get("body_text") or "")[:420],
                "excerpt_state": "bounded"
            } for source in sources],
            "packet_bounds": {
                "source_count": len(sources),
                "source_excerpt_chars": 420,
                "truncated_source_count": sum(1 for source in sources if len(source.get("body_text") or "") > 420)
            }
        },
        "prior_story_card_version": None
    }

class RequestDeadlineExceeded(TimeoutError):
    pass

def _deadline_exceeded(_signum, _frame):
    raise RequestDeadlineExceeded(f"model request exceeded {REQUEST_TIMEOUT_SECONDS}s")

def invoke(prompt, schema, item, max_tokens):
    user = json.dumps({
        "instruction": prompt,
        "output_schema": schema,
        "bounded_soup_packet": packet(item)
    })
    payload = json.dumps({
        "model": MODEL,
        "messages": [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}],
        "temperature": 0.1,
        "max_tokens": max_tokens
    }).encode()
    req = urllib.request.Request(ENDPOINT, data=payload, headers={"Content-Type": "application/json"})
    started = time.time()
    previous_handler = signal.signal(signal.SIGALRM, _deadline_exceeded)
    signal.alarm(REQUEST_TIMEOUT_SECONDS)
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as res:
            body = json.loads(res.read())
    except RequestDeadlineExceeded as exc:
        return None, str(exc), "model_request_timeout"
    except TimeoutError as exc:
        return None, str(exc), "model_request_timeout"
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)
    content = body["choices"][0]["message"]["content"]
    match = re.search(r"\{.*\}", re.sub(r"<think>.*?</think>", "", content, flags=re.S), flags=re.S)
    if not match:
        return None, content, "invalid_model_json"
    try:
        return json.loads(match.group(0)), content, None
    except json.JSONDecodeError:
        return None, content, "invalid_model_json"

def field_complete(value):
    return isinstance(value, dict) and value.get("state") == "complete" and isinstance(value.get("text"), str) and bool(value["text"].strip()) and bool(value.get("provenance_refs"))

def mixed_refusal(output):
    card_fields = [
        "exact_happening",
        "deck",
        "summary",
        "key_claims",
        "source_links",
        "source_coverage",
        "topic_salience",
        "changed_field_keys",
        "change_summary",
        "field_completeness",
    ]
    return any(output.get(key) not in (None, {}, []) for key in card_fields)

def score_output(output, item, parse_error):
    required_refs = item["expected"]["required_source_refs"]
    matrix = {
        "title": False,
        "exact_happening": False,
        "summary": False,
        "deck": False,
        "key_claims": False,
        "topic_salience": False,
        "source_links": False,
        "source_coverage_contribution_reasons": False,
        "valid_refusal_quarantine_provenance": False
    }
    if parse_error:
        return matrix, "invalid_model_json"
    if not isinstance(output, dict):
        return matrix, "invalid_model_schema"
    if output.get("status") == "refused":
        proof = output.get("refusal_provenance")
        matrix["valid_refusal_quarantine_provenance"] = isinstance(proof, dict) and bool(proof.get("reason")) and bool(proof.get("evidence_refs")) and not mixed_refusal(output)
        return matrix, "honest_evidence_limited_refusal" if matrix["valid_refusal_quarantine_provenance"] else "invalid_model_schema"
    if output.get("status") != "complete":
        return matrix, "invalid_model_schema"

    for key in ["title", "exact_happening", "summary", "deck"]:
        matrix[key] = field_complete(output.get(key))
    claims = output.get("key_claims")
    matrix["key_claims"] = isinstance(claims, list) and len(claims) > 0 and all(isinstance(c, dict) and c.get("text") and c.get("evidence_refs") for c in claims)
    salience = output.get("topic_salience")
    matrix["topic_salience"] = isinstance(salience, dict) and field_complete(salience.get("salience_explanation")) and bool(salience.get("global_salience")) and bool(salience.get("flynn_priority"))
    links = output.get("source_links")
    matrix["source_links"] = isinstance(links, list) and sorted([x.get("source_ref") for x in links if isinstance(x, dict)]) == sorted(required_refs)
    coverage = output.get("source_coverage")
    matrix["source_coverage_contribution_reasons"] = isinstance(coverage, list) and sorted([x.get("source_ref") for x in coverage if isinstance(x, dict)]) == sorted(required_refs) and all(field_complete(x.get("contribution_reason")) for x in coverage if isinstance(x, dict))
    complete_required = [key for key in matrix.keys() if key != "valid_refusal_quarantine_provenance"]
    reason = "pass" if all(matrix[key] for key in complete_required) else "invalid_model_schema"
    return matrix, reason

def main():
    corpus_path = sys.argv[1] if len(sys.argv) > 1 else "priv/evals/t1501_story_card_prompt_corpus.json"
    corpus = json.load(open(corpus_path))
    production_contract = production_story_synthesis_contract()
    variants = [
        ("baseline_current_contract", BASELINE_PROMPT, BASELINE_SCHEMA, 4096, "hardcoded_pre_t1501_baseline"),
        ("production_story_synthesis_contract", production_contract["task_prompt"], production_contract["output_schema"], production_contract["max_tokens"], production_contract["config_version"])
    ]
    report = {"corpus_id": corpus["corpus_id"], "item_ids": [i["item_id"] for i in corpus["items"]], "runs": []}
    for name, prompt, schema, max_tokens, contract_version in variants:
        rows = []
        for item in corpus["items"]:
            print(f"running {name} {item['item_id']}", file=sys.stderr, flush=True)
            output, raw, parse_error = invoke(prompt, schema, item, max_tokens)
            matrix, reason = score_output(output, item, parse_error)
            rows.append({"item_id": item["item_id"], "failure_reason": reason, "matrix": matrix, "raw_excerpt": raw[:500]})
        passed = sum(1 for row in rows if row["failure_reason"] == "pass")
        report["runs"].append({"variant": name, "contract_version": contract_version, "max_tokens": max_tokens, "score": f"{passed}/{len(rows)}", "items": rows})
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
