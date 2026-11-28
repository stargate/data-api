package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertIds;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.findAndRerank;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.assertRerankerCalls;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.calls;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankCollectionCases;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankEntryCases;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankExampleCases;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankExecutionCases;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFilterProjectionCases;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankOptionCases;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankOverrideCases;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankPrecedenceCases;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankResponseCases;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRuntimeErrorCases;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankSortCases;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankTestContext;
import io.stargate.sgv2.jsonapi.testresource.FakeRerankerTestResource;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for findAndRerank on collections, run against a fake reranker.
 *
 * <p>{@link FakeRerankerTestResource} starts a fake NVIDIA reranker inside the test JVM and points
 * every configured reranking model at it, so the whole production path runs, including the HTTP
 * client, and the tests can check exactly what the reranker received. Because this class has its
 * own test resource, Quarkus starts a separate application for it.
 *
 * <p>This class only holds the setup and one smoke test. The tests themselves are default methods
 * of the {@code FindAndRerank*Cases} interfaces in package {@code findandrerank}, one interface per
 * topic, and JUnit runs them as methods of this class. Each test has a short comment that says what
 * it sends, what it expects, and whether the documentation specifies that behavior. Every test also
 * asserts what the reranker received (or that it was not called).
 */
@QuarkusIntegrationTest
@QuarkusTestResource(value = FakeRerankerTestResource.class, restrictToAnnotatedClass = true)
public class FindAndRerankFakeRerankerIntegrationTest extends AbstractCollectionIntegrationTestBase
    implements FindAndRerankTestContext,
        FindAndRerankExampleCases,
        FindAndRerankCollectionCases,
        FindAndRerankSortCases,
        FindAndRerankOptionCases,
        FindAndRerankOverrideCases,
        FindAndRerankFilterProjectionCases,
        FindAndRerankExecutionCases,
        FindAndRerankResponseCases,
        FindAndRerankPrecedenceCases,
        FindAndRerankRuntimeErrorCases,
        FindAndRerankEntryCases {

  private final Set<String> createdFixtures = ConcurrentHashMap.newKeySet();

  @BeforeAll
  @Override
  public final void createDefaultCollection() {
    // No default collection: the tests create the fixtures they use on first use.
  }

  @Override
  public String keyspace() {
    return keyspaceName;
  }

  @Override
  public boolean lexicalAvailable() {
    return isLexicalAvailableForDB();
  }

  @Override
  public int port() {
    return getTestPort();
  }

  @Override
  public Set<String> createdFixtures() {
    return createdFixtures;
  }

  // Sends one findAndRerank with a $vectorize sort to a collection with vectorize and rerank and
  // two documents. Expects both documents in rerank-score order, and exactly one reranker request
  // on the default model with the sort text as query and both $vectorize texts as passages.
  // The docs do not specify the test setup; the test proves that the model URL from the test
  // resource is used and that the application starts with the extra models it adds.
  @Test
  public void smokeFakeRerankerReceivesExactlyOneRequest() {
    var response =
        postToFixture(
            FindAndRerankFixtures.NOLEX,
            findAndRerank().sort("{\"$hybrid\": {\"$vectorize\": \"ChatGPT upgraded\"}}").json());

    assertIds(response, "n2", "n1");
    assertRerankerCalls(
        calls(1)
            .query("ChatGPT upgraded")
            .passages("ChatGPT integrated sneakers that talk to you", "New data updated"));
  }
}
