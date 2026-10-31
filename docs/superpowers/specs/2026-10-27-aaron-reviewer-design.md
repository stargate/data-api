# Aaron Reviewer Skill Design

## Intent

Build a reusable data-api review skill that reproduces Aaron Morton's judgment, priorities, severity calibration, and direct but collaborative feedback. Use it to review changes and to pre-review work before requesting Aaron's review.

## Evidence

The profile is grounded in all 217 stargate/data-api PRs where Aaron reviewed or commented: 486 formal reviews, 1,617 inline comments, and 48 discussion comments, totaling 1,835 nonempty text bodies from 2022 through 2026. Feedback on other people's PRs is the primary evidence; Aaron's replies on his own PRs provide supporting rationale rather than review-style examples.

## Goals

- Prioritize correctness, API contracts, failure modes, understandable ownership, and meaningful tests before surface style.
- Produce specific findings with an observable problem, impact, and concrete fix or test.
- Distinguish blockers, questions, nits, and follow-ups without blocking every PR.
- Preserve candid uncertainty, narrow scope, short human comments, and explicit review boundaries.
- Provide a concise skill, a linked evidence reference, and a durable Mem0 profile.
- Verify with fresh review agents that the skill changes behavior in useful and repeatable ways.

## Non-goals and anti-goals

- Do not imitate Aaron's typos, jokes, meeting offers, or historical technology positions when current code says otherwise.
- Do not generate generic style-only reviews, rigid praise templates, invented findings, or performative certainty.
- Do not turn every nit into a blocker or expand a PR with unrelated refactors.
- Do not commit the raw review corpus or large generated research artifacts.
- Do not add AI attribution to repository content or git metadata.

## Constraints

- Keep the skill project-local under `.agents/skills/aaron-reviewer/`, consistent with other agent repositories.
- Use US English and asynchronous written feedback.
- Match current source and sibling conventions when reviewing; historical comments are evidence, not a substitute for reading code.
- Fresh review subagents are authorized for validation.
- Do not run git commit or push without separate explicit approval.

## Approaches

1. **Validated repo skill (chosen):** concise playbook, evidence reference, Mem0 profile, and fresh-agent baseline/regression tests. Best confidence that the skill works rather than merely sounding plausible.
2. **Repo skill without agents:** same artifacts, but only self-validation. Faster and less independent evidence.
3. **Memory-only profile:** no versioned skill. Simplest, but does not meet the goal of a reusable repository capability.

## Parts

- `.agents/skills/aaron-reviewer/SKILL.md`: trigger conditions, review workflow, priority checklist, severity labels, comment recipe, and common mistakes.
- `.agents/skills/aaron-reviewer/references/review-profile.md`: detailed likes, dislikes, current-era rules, representative public GitHub links, and one excellent annotated example.
- Mem0: durable summary of the profile, skill location, evidence scope, and validation outcome.
- `.bob/aaron-reviewer-research/`: temporary raw evidence and test notes only; not a deliverable.

## Validation

1. Create at least three realistic review scenarios covering correctness, API/error design, tests, scope, and tone under time/authority/sunk-cost pressure.
2. Run fresh agents without the skill and record exact generic or incorrect review behavior.
3. Micro-test behavior-shaping wording against a no-guidance control with at least five repetitions per variant and manually inspect flagged matches.
4. Write the minimal skill addressing observed failures, rerun the same scenarios with it, and close new loopholes.
5. Finish with a held-out historical data-api review scenario and compare coverage, severity, specificity, and tone against Aaron's actual feedback.

## Builder discretion

The builder may choose the final scenario fixtures, concise wording, and whether all evidence fits in one reference file. The deliverable remains the two skill files, Mem0 profile, and observed validation results.
