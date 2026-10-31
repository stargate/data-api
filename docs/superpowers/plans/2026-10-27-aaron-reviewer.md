# Aaron Reviewer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and validate a project-local skill that reviews data-api changes with Aaron Morton's judgment, priorities, severity calibration, and direct collaborative style, then save the durable profile to Mem0.

**Architecture:** A concise `SKILL.md` supplies the review workflow and output contract, while one evidence reference holds detailed preferences and public GitHub examples. Fresh review agents establish baseline failures before the skill exists and verify the finished skill against the same scenarios and a held-out historical PR.

**Tech Stack:** Markdown agent skills, GitHub REST through `gh`, Python 3 standard-library evidence processing, fresh coder subagents, Mem0 MCP.

**Spec:** `docs/superpowers/specs/2026-10-27-aaron-reviewer-design.md`

## Global Constraints

- Deliver only `.agents/skills/aaron-reviewer/SKILL.md`, `.agents/skills/aaron-reviewer/references/review-profile.md`, the design/plan documents, and the Mem0 profile; raw research stays in `.bob/` and is not a deliverable.
- Use US English and asynchronous written feedback; no meeting offers, AI attribution, invented findings, or rigid praise template.
- Historical comments guide judgment but never replace reading current source and sibling code.
- Fresh review subagents are authorized for baseline, wording, and regression tests.
- Do not run git commit or push without separate explicit approval.

## Review Focus

- Shared mutable state or builder reuse: identify the correctness risk before any style feedback; covered by the `shared-state-hotfix` scenario in Tasks 1 and 3.
- Public API errors: require stable titles, request-specific values, actionable recovery, correct family/scope, and no driver internals; covered by `api-error-contract` in Tasks 1 and 3.
- Fragile tests: reject order dependence, excessive mocking, weak assertions, and missing boundary cases without demanding redundant coverage; covered by `fragile-test-suite` in Tasks 1 and 3.
- Scope and AI filler: remove unrelated docs, private links, speculative abstractions, and comments the team cannot explain; covered by `scoped-ai-change` in Tasks 1 and 3.
- Severity and uncertainty: distinguish blockers, nits, questions, and follow-ups and state unreviewed areas; checked by every scenario and the held-out historical review in Task 3.

---

### Task 1: Establish baseline failures and wording controls

**Files:**
- Create: `.bob/aaron-reviewer-research/scenarios/shared-state-hotfix.md`
- Create: `.bob/aaron-reviewer-research/scenarios/api-error-contract.md`
- Create: `.bob/aaron-reviewer-research/scenarios/fragile-test-suite.md`
- Create: `.bob/aaron-reviewer-research/scenarios/scoped-ai-change.md`
- Create: `.bob/aaron-reviewer-research/scenarios/rubric.md`
- Create: `.bob/aaron-reviewer-research/baseline-results.md`

**Interfaces:**
- Consumes: the evidence profile and public examples in `.bob/aaron-reviewer-research/corpus-*.txt`.
- Produces: verbatim baseline failures and a scenario rubric used unchanged to evaluate `SKILL.md` in Task 3.

- [ ] **Step 1: Write four pressure scenarios and the rubric**

Each scenario supplies a realistic Java/data-api diff or focused excerpt, a deadline plus authority and sunk-cost pressure, and asks for a review rather than a rewrite. The rubric requires the Review Focus behaviors above, `bug` / `-1` only for blockers, `nit` / `consider` for non-blockers, explicit follow-ups, and candid uncertainty.

- [ ] **Step 2: Run all scenarios with fresh agents and no Aaron skill**

Use one fresh coder agent per scenario. Do not provide the learned profile or any draft skill. Record exact findings, severity, omissions, unsupported certainty, and rationalizations in `baseline-results.md`.

- [ ] **Step 3: Run no-guidance wording controls**

For the comment recipe and severity calibration, run at least five fresh single-shot samples per variant with the same realistic system context. Manually read every flagged output for buried verdicts, generic praise, wrong severity, missing fixes, and template echo. If the control does not exhibit a failure, do not author guidance for that non-failure; record the evidence instead.

- [ ] **Step 4: Derive the minimal content requirements**

List only repeated baseline failures that the skill must address, including the exact agent wording that shows each failure. Expected baseline risks include generic correctness-only checklists, style-first feedback, over-blocking, vague fixes, fabricated confidence, and failure to reject AI filler; keep only what the outputs actually show.

### Task 2: Write the minimal Aaron-reviewer skill

**Files:**
- Create: `.agents/skills/aaron-reviewer/SKILL.md`
- Create: `.agents/skills/aaron-reviewer/references/review-profile.md`

**Interfaces:**
- Consumes: Task 1 baseline failures and the full evidence corpus.
- Produces: a loadable `aaron-reviewer` skill and its evidence reference for Task 3 validation.

- [ ] **Step 1: Write valid frontmatter and a concise core workflow**

Use:

```yaml
---
name: aaron-reviewer
description: Use when reviewing or pre-reviewing data-api code, tests, API errors, schemas, comments, or PR scope and the review should match Aaron Morton's correctness-first, direct, pragmatic judgment.
---
```

The body must explain how to inspect current code and call sites, prioritize the Review Focus classes, label severity, write `observable problem -> impact -> concrete fix/test`, state uncertainty, and keep PR scope narrow. Address Task 1 failures with the guidance form that matches each failure; do not add rules for hypothetical failures not observed.

- [ ] **Step 2: Write the evidence reference**

Document durable likes, dislikes, current-era rules, and representative public GitHub links. Include one excellent annotated example showing a blocker, a nit, and a follow-up rather than a rigid full-review template. Keep raw corpus and narrative research history out of the reference.

- [ ] **Step 3: Check structure and discovery**

Run:

```bash
python3 - <<'PY'
from pathlib import Path
p = Path('.agents/skills/aaron-reviewer/SKILL.md')
s = p.read_text()
assert s.startswith('---\nname: aaron-reviewer\ndescription: Use when ')
assert '\n---\n' in s
assert len(s.split('description:', 1)[1].split('\n---', 1)[0]) <= 1024
assert 'references/review-profile.md' in s
assert Path('.agents/skills/aaron-reviewer/references/review-profile.md').is_file()
print('Skill structure and discovery checks passed')
PY
```

Expected: `Skill structure and discovery checks passed`. Also confirm the main skill has a quick reference, common mistakes, no narrative storytelling, and no flowchart unless the decision is genuinely non-obvious.

### Task 3: Verify, refactor, and save the durable profile

**Files:**
- Modify: `.agents/skills/aaron-reviewer/SKILL.md`
- Modify: `.agents/skills/aaron-reviewer/references/review-profile.md`
- Create: `.bob/aaron-reviewer-research/validation-results.md`

**Interfaces:**
- Consumes: Task 1 scenarios/rubric and Task 2 skill files.
- Produces: observed validation evidence and the final profile text to store in Mem0.

- [ ] **Step 1: Run wording micro-tests with the skill**

Repeat the Task 1 variants with the complete skill in the realistic system context. Compare at least five repetitions per variant against the no-guidance control, manually inspect all flagged matches, and record whether outputs converge on the required shape and severity.

- [ ] **Step 2: Run all pressure scenarios with the skill**

Use fresh coder agents and the unchanged Task 1 rubric. Record every remaining miss verbatim. Add explicit counters, rationalization entries, and red flags only for observed misses, then rerun until no new rationalization or severity error appears.

- [ ] **Step 3: Run a held-out historical review**

Select one historical data-api PR not used while drafting the skill. Give a fresh agent the relevant diff and current surrounding code, then compare its findings with Aaron's actual comments for correctness coverage, severity, specificity, tone, and stated review boundaries. Record gaps and refactor once if needed.

- [ ] **Step 4: Save the validated profile to Mem0**

Store a durable `erichare` memory containing Aaron's identity, the evidence scope (217 PRs and 1,835 nonempty bodies), core preferences, the skill path, and the actual validation result. Do not store secrets, raw reviews, or step-by-step task state.

- [ ] **Step 5: Verify final artifacts and report**

Run the Task 2 structure check again and `git status --short`. Report exact scenario counts, observed baseline/regression outcomes, remaining limitations, and the two skill file paths. Leave git commit/push undecided until separately approved by Eric.
