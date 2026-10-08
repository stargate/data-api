package io.stargate.sgv2.jsonapi.testresource;

import java.util.Map;

/**
 * {@link FakeRerankerTestResource} with four more changes, for the tests of switched-off features:
 *
 * <ul>
 *   <li>The reranking and MCP feature flags are blank, so both features are off unless a request
 *       sends {@code Feature-Flag-reranking: true} or {@code Feature-Flag-mcp: true}.
 *   <li>The nvidia and mistral reranking providers are disabled, so the enabled ones are cohere,
 *       fakeProvider and voyageAI.
 *   <li>The {@code hybridLimits} bounds are 0 to 1500 for {@code $vector} and 0 to 1600 for {@code
 *       $lexical} instead of 1 to 100 for both. The upper bounds differ so that a test can tell
 *       which bound each limit is checked against.
 *   <li>The default findAndRerank limit ({@code default-find-and-rerank-limit}) is 1 instead of the
 *       built-in 10, so a test can tell that a request without a limit uses the configured value.
 * </ul>
 */
public class FeatureOffFakeRerankerTestResource extends FakeRerankerTestResource {

  @Override
  public String getFeatureFlagReranking() {
    // A blank value leaves the feature undefined: off, unless a request header turns it on.
    return " ";
  }

  @Override
  public String getFeatureFlagMcp() {
    return " ";
  }

  @Override
  protected Map<String, String> overrides() {
    return Map.of(
        PROVIDERS + "nvidia.enabled",
        "false",
        PROVIDERS + "mistral.enabled",
        "false",
        VECTOR_LIMIT,
        "0,50,1500",
        LEXICAL_LIMIT,
        "0,50,1600",
        "stargate.jsonapi.operations.default-find-and-rerank-limit",
        "1");
  }
}
