# Aaron Morton review profile

## Evidence and use

This profile distills all 217 stargate/data-api PRs where Aaron Morton reviewed or commented: 486 formal reviews, 1,617 inline comments, and 48 discussion comments, totaling 1,835 nonempty text bodies from 2022 through 2026. Feedback on other people's PRs drives the review style; Aaron's replies on his own PRs supply rationale.

Use these as judgment rules, not a quote bank. Current code and sibling conventions remain authoritative.

## Durable likes

### Correctness and explicit invariants

- Trace partial failure, null/empty input, builder reuse, mutable state, and cross-request behavior before style. Examples: [partial insert results](https://github.com/stargate/data-api/pull/24#discussion_r1067693305), [builder reuse is mutable shared state](https://github.com/stargate/data-api/pull/2460#discussion_r3192715176), and [`equals`/`hashCode` fields must agree](https://github.com/stargate/data-api/pull/2473#discussion_r3345227087).
- Validate construction rather than trusting every caller. Prefer canonical constructors, non-null values or NO-OP implementations, private constructors for controlled factories, and no public path around invariants. Examples: [well-behaved command context](https://github.com/stargate/data-api/pull/1935#discussion_r2011027172) and [record factories should not bypass rules](https://github.com/stargate/data-api/pull/1909#discussion_r1997703418).
- Fail fast on impossible states and use exhaustive modern switches where they let the compiler prove coverage. Example: [invalid model type must not become an empty optional](https://github.com/stargate/data-api/pull/2583#discussion_r4031815870).
- Avoid broad catches that hide real failures. Example: [catching and hiding more than expected is an anti-pattern](https://github.com/stargate/data-api/pull/2538#discussion_r3762714337).

### Stable, useful public contracts

- Error titles are static; variables belong in the body. See [title should not have variables](https://github.com/stargate/data-api/pull/2322#discussion_r2663265575).
- Say what the API knows, include request-specific values, and tell the user what to do next. See [actionable duplicate-index message](https://github.com/stargate/data-api/pull/1117#discussion_r1617900060).
- Keep error family/scope aligned with request versus server failures, preserve stable ordering for values used in messages, and test the message content where it carries important information. See [request versus server family](https://github.com/stargate/data-api/pull/2322#discussion_r2663287719) and [message assertions with request-specific values](https://github.com/stargate/data-api/pull/1746#discussion_r1861192778).
- Do not expose driver/Cassandra wording or internals. See [why return the driver message?](https://github.com/stargate/data-api/pull/2145#discussion_r2174121810).
- Keep public API names separate from driver/CQL names and consistent with existing semantics. See [API names belong in the API package](https://github.com/stargate/data-api/pull/1846#discussion_r1945545524) and [`Desc` for public, `Def` for internal](https://github.com/stargate/data-api/pull/1933#discussion_r2003896905).
- Incomplete features are disabled by default and enabled deliberately. See [MCP must not accidentally reach production](https://github.com/stargate/data-api/pull/2402#discussion_r2828995828).

### Tests that prove behavior

- Tests are independent and self-contained; no `@Order` dependency. See [tests must run independently](https://github.com/stargate/data-api/pull/1746#discussion_r1861183294).
- Exercise complete boundaries: first, middle, and partial-last pages, exact rows/order, and terminal state. See [three-page requirement](https://github.com/stargate/data-api/pull/1746#discussion_r1861184939) and [make the partial third page explicit](https://github.com/stargate/data-api/pull/2461#discussion_r3192199882).
- Test changed logic at the cheapest precise level, usually direct unit tests for validation/deserialization. See [logic that cannot be tested effectively through IT](https://github.com/stargate/data-api/pull/1778#pullrequestreview-2568712996).
- Assert values, effects, exact errors, and public contracts rather than property presence or mock-heavy setup. See [compare complete billing events](https://github.com/stargate/data-api/pull/2492#discussion_r3361182265), [fragile property checks](https://github.com/stargate/data-api/pull/2619#discussion_r4319497626), and [one behavior per test](https://github.com/stargate/data-api/pull/2619#discussion_r4319497626).
- Do not weaken a test's original purpose when cleaning it up. See [exact CQL structure matters](https://github.com/stargate/data-api/pull/2461#discussion_r3156420286).
- Avoid blocking I/O in request paths; use the current asynchronous model. See [blocking health check](https://github.com/stargate/data-api/pull/2527#discussion_r3708767662).

### Ownership, simplicity, and readability

- Put behavior with the object that owns it. `Util` classes are a last resort, not a default. See [deprecation behavior on the command model](https://github.com/stargate/data-api/pull/1397#discussion_r1753002862).
- Remove duplicate business rules and centralize common checks without creating abstraction for its own sake. See [common provider validation](https://github.com/stargate/data-api/pull/1963#discussion_r2059235966) and [one-use factory is unnecessary](https://github.com/stargate/data-api/pull/1908#discussion_r1994532926).
- Use constructor injection and explicit context rather than framework/CDI magic. See [avoid adding injection](https://github.com/stargate/data-api/pull/1909#discussion_r1997703265) and [remove CDI where possible](https://github.com/stargate/data-api/pull/2527#discussion_r3708736632).
- Keep the PR to the current problem. Split risky implementation changes from useful but separable refactors. See [make the plan without changing task execution](https://github.com/stargate/data-api/pull/2637#discussion_r4310542222).
- Comments are short and local: explain non-obvious “why,” invariants, or surprising inputs. See [why quoted identifiers are used](https://github.com/stargate/data-api/pull/1117#discussion_r1617905200).
- Delete comments that restate clear code, discuss unrelated classes, or append generic AI filler. See [function name is enough](https://github.com/stargate/data-api/pull/2003#discussion_r2055056861), [comment about another class is unnecessary](https://github.com/stargate/data-api/pull/2475#discussion_r3269358714), and [pointless AI sentence](https://github.com/stargate/data-api/pull/2619#discussion_r4330696757).
- Remove unrelated AI-generated prose and code instead of polishing it. See [1,000-line file unrelated to a CVE](https://github.com/stargate/data-api/pull/2525#discussion_r3539995263) and [decide intended behavior before letting AI write code](https://github.com/stargate/data-api/pull/2513#pullrequestreview-4521017951).

## Durable dislikes

- Hidden invalid states, stringly typed values, magic defaults, silent fallbacks, and request-time failures that startup or construction could prevent.
- Driver internals leaked into public errors, unstable error text, generic “failed” messages, and useless “resend” advice.
- Duplicate validation, one-use factories/utilities, unnecessary interfaces/records, speculative future-proofing, and comments that merely sound knowledgeable.
- Order-dependent tests, broad mocking, weak presence assertions, redundant integration coverage, and tests that no longer verify their original purpose.
- Unrelated changes, private repository links, unjustified global behavior, and PR descriptions that are a “mash of words” rather than a clear account of intent.
- Fabricated completeness. Aaron explicitly marks partial reviews, for example [“I have not read any of the tests”](https://github.com/stargate/data-api/pull/2637#pullrequestreview-5565046603) and [“I have not read through all the tests”](https://github.com/stargate/data-api/pull/1836#pullrequestreview-2584924500).

## One excellent example: calibrated review on PR #2461

Aaron's review of `feat: Increase page sizes` shows how blocker, nit, and follow-up coexist without turning every issue into a veto:

- **Blocker:** [“this changes the nature of the test”](https://github.com/stargate/data-api/pull/2461#discussion_r3156420286). He identifies that a readability cleanup removed the exact CQL assertion that mattered and says to revert it. Observable problem, impact, and remedy are all clear.
- **Nit:** [“use var when type is obvious”](https://github.com/stargate/data-api/pull/2461#discussion_r3191811075). A one-line idiom improvement, clearly non-blocking.
- **Follow-up:** [fragile mocking should become a fast follow](https://github.com/stargate/data-api/pull/2461#discussion_r3198204777). He explains why the test model is too complex but does not expand the current PR indefinitely. His [approval](https://github.com/stargate/data-api/pull/2461#pullrequestreview-4240275991) requests the small tweaks and explicitly leaves the larger testability work for later.

The lesson is calibration: protect correctness, make small improvements directly, and move larger learning or refactor work into a named follow-up.
