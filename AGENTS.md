# Agent Guidelines

## Code Comment Standards

### Purpose of Comments

Comments should explain **WHY** code exists or **WHY** a particular approach was chosen, NOT simply describe **WHAT** the code does.

### What to Avoid

- Redundant comments that restate obvious code behavior
- Comments that merely describe what a line or block of code does
- Stating the obvious (e.g., `// increment counter` above `counter++`)

### What to Include

- **Intent and reasoning**: Why this approach was chosen over alternatives
- **Business logic context**: Why certain rules or constraints exist
- **Edge cases**: Non-obvious scenarios the code handles
- **Non-obvious behavior**: Subtle interactions or side effects
- **Workarounds**: Why a particular workaround was necessary
- **Performance considerations**: Why certain optimizations were made
- **Security implications**: Why certain checks or patterns are used

## Documentation Hierarchy

### Class-Level Documentation

Class-level Javadoc should provide comprehensive context and explain the design intent. Include:

- **Purpose**: What problem this class/interface solves
- **Design rationale**: Why this approach was chosen
- **Usage context**: How and when this should be used
- **Constraints and rules**: What implementations should NOT do
- **Integration points**: How this fits into the larger system

**Avoid repeating this information at the method level.**

### Method-Level Documentation

Documentation requirements vary based on the method's visibility and purpose:

#### Public API, Interface, and Abstract Methods

These require formal Javadoc with:
- **When the method is called**: Brief statement of why this should be called and what it does
- **Parameter documentation**: Standard `@param` tags for all parameters
- **Return value**: Standard `@return` tag

**Do NOT repeat class-level context or add implementation examples at the method level.**

#### Private Methods

Private implementation methods typically only need a brief comment explaining **WHY** the method exists or **WHY** a particular approach was taken. Formal `@param` and `@return` tags are not required unless:
- The method is complex with non-obvious parameter usage
- The method has subtle behavior that needs explanation

**Example (simple private method):**
```java
// Normalize user input to prevent injection attacks
private String sanitizeInput(String raw) {
    return raw.replaceAll("[^a-zA-Z0-9]", "");
}
```

**Example (complex private method needing params):**
```java
/**
 * Merges overlapping time ranges to optimize query performance.
 * Adjacent ranges within the tolerance window are combined to reduce
 * the number of database queries.
 *
 * @param ranges List of time ranges, may contain overlaps
 * @param toleranceMs Milliseconds of gap allowed between ranges to still merge them
 * @return Consolidated list with overlapping ranges merged
 */
private List<TimeRange> mergeTimeRanges(List<TimeRange> ranges, long toleranceMs) {
```

### Documentation Example: Interface

**Class-level (comprehensive):**
```java
/**
 * Defines the stages in the lifecycle of a test bench run.
 * <p>
 * Designed to be implemented by a Backend so that it can make changes 
 * to the data environment so tests can run in a common environment. 
 * For example, when we use Cassandra as a backend we need to create 
 * a keyspace but for Astra we use the default one.
 * </p>
 * <p>
 * There should not be any test logic within the implementations, 
 * that should all be in the test definitions.
 * </p>
 */
public interface TestPlanLifecycle {
```

**Method-level (minimal):**
```java
  /**
   * Called to optionally add a node to execute before the workflow starts.
   *
   * @param testNodeFactory Factory to use to create test nodes
   * @param uriBuilder Builder to use to create URIs
   * @param workflow The workflow about to execute
   * @return Optional DynamicNode to run before the workflow
   */
  default Optional<DynamicNode> beforeWorkflow(...) {
```

**What to avoid at method level:**
- Repeating "useful for cleanup" or "allows setting up resources" (already covered at class level)
- Adding specific implementation examples (violates the "no test logic" constraint stated at class level)
- Restating the overall purpose of the interface