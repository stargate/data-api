package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.testresource.FakeRerankerTestResource.Model;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Assertions on what the fake reranker received. Every integration test method calls at least one
 * of {@link #assertRerankerNotCalled()}, {@code assertRerankerCallCount(...)} or {@link
 * #assertRerankerCalls(ExpectedRerankCalls...)}; the error assertions in {@link
 * FindAndRerankAssertions} call them too. Batches are sent at the same time and arrive in no fixed
 * order, so passages and batch sizes are compared as multisets, never by arrival order.
 */
public final class RerankerAssertions {

  /** The {@code tenant-id} header the application sends in the integration test setup. */
  private static final String TENANT_ID = "SINGLE-TENANT";

  /** The {@code max-batch-size} of {@link Model#DEFAULT} in the test reranking configuration. */
  private static final int DEFAULT_MODEL_BATCH_SIZE = 10;

  private RerankerAssertions() {}

  /** Starts the expectation for {@code count} requests on {@link Model#DEFAULT}. */
  public static ExpectedRerankCalls calls(int count) {
    return new ExpectedRerankCalls(count);
  }

  /**
   * The requests on {@link Model#DEFAULT} for these passages: one per batch of up to 10, so 12
   * passages give two requests with 10 and 2 passages.
   */
  public static ExpectedRerankCalls defaultModelCalls(String query, String... passages) {
    List<Integer> batchSizes = new ArrayList<>();
    for (int left = passages.length; left > 0; left -= DEFAULT_MODEL_BATCH_SIZE) {
      batchSizes.add(Math.min(left, DEFAULT_MODEL_BATCH_SIZE));
    }
    return calls(batchSizes.size())
        .query(query)
        .passages(passages)
        .batchSizes(batchSizes.toArray(Integer[]::new));
  }

  /** The fake received no request at all. */
  public static void assertRerankerNotCalled() {
    assertRerankerCallCount(0);
  }

  /** The fake received exactly {@code count} requests, on any endpoint. */
  public static void assertRerankerCallCount(int count) {
    List<RecordedRerankRequest> all = FakeRerankerClient.requests();
    assertThat(all).as("requests received by the fake reranker:%n%s", describe(all)).hasSize(count);
  }

  /** The fake received exactly {@code count} requests on this model's endpoint. */
  public static void assertRerankerCallCount(Model model, int count) {
    List<RecordedRerankRequest> all = FakeRerankerClient.requests();
    assertThat(all.stream().filter(r -> Objects.equals(r.path(), model.path())).toList())
        .as("requests on %s; all requests:%n%s", model.path(), describe(all))
        .hasSize(count);
  }

  /**
   * Checks every request the fake received against the expectations, one expectation per model: the
   * total count and the count per endpoint, and for each request the method, body {@code model},
   * {@code truncate: NONE}, {@code Authorization}, {@code tenant-id}, and that the fake recognized
   * it. Then, per expectation: the query text, all passages as a multiset, and the batch sizes when
   * set. An expectation with requests must set the query and the passages.
   */
  public static void assertRerankerCalls(ExpectedRerankCalls... expected) {
    List<RecordedRerankRequest> all = FakeRerankerClient.requests();
    String context = "requests received by the fake reranker:%n%s".formatted(describe(all));
    assertThat(all)
        .as("total request count; %s", context)
        .hasSize(Arrays.stream(expected).mapToInt(e -> e.count).sum())
        .allSatisfy(r -> assertThat(r.unrecognized()).as("unrecognized reason").isNull());
    for (ExpectedRerankCalls e : expected) {
      List<RecordedRerankRequest> calls =
          all.stream().filter(r -> Objects.equals(r.path(), e.model.path())).toList();
      assertThat(calls).as("requests on %s; %s", e.model.path(), context).hasSize(e.count);
      if (e.count == 0) {
        continue;
      }
      assertThat(e.query != null && e.passages != null)
          .as("set query(...) and passages(...) on the expectation")
          .isTrue();
      for (RecordedRerankRequest call : calls) {
        assertThat(call.method()).as("method; %s", context).isEqualTo("POST");
        assertThat(call.model()).as("model; %s", context).isEqualTo(e.model.modelName());
        assertThat(call.query()).as("query; %s", context).isEqualTo(e.query);
        assertThat(call.truncate()).as("truncate; %s", context).isEqualTo("NONE");
        assertThat(call.header("Authorization"))
            .as("Authorization; %s", context)
            .isEqualTo(e.authorization);
        assertThat(call.header("tenant-id")).as("tenant-id; %s", context).isEqualTo(TENANT_ID);
      }
      assertThat(calls.stream().flatMap(c -> c.passages().stream()).toList())
          .as("all passages; %s", context)
          .containsExactlyInAnyOrderElementsOf(e.passages);
      if (e.batchSizes != null) {
        assertThat(calls.stream().map(c -> c.passages().size()).toList())
            .as("batch sizes; %s", context)
            .containsExactlyInAnyOrderElementsOf(e.batchSizes);
      }
    }
  }

  /** No request was unrecognized by the fake. Runs after every test. */
  public static void assertNoUnrecognizedRequests() {
    List<RecordedRerankRequest> all = FakeRerankerClient.requests();
    assertThat(all.stream().filter(r -> r.unrecognized() != null).toList())
        .as("requests the fake reranker did not recognize:%n%s", describe(all))
        .isEmpty();
  }

  private static String describe(List<RecordedRerankRequest> requests) {
    return requests.stream().map(r -> "  " + r).collect(Collectors.joining("\n"));
  }

  /**
   * Expected requests on one model endpoint. Defaults: {@link Model#DEFAULT}, {@code Authorization:
   * Bearer <Data API token>}, {@code tenant-id: SINGLE-TENANT}.
   */
  public static final class ExpectedRerankCalls {
    private final int count;
    private Model model = Model.DEFAULT;
    private String query;
    private List<String> passages;
    private List<Integer> batchSizes;
    private String authorization = "Bearer " + FindAndRerankRequests.dataApiToken();

    private ExpectedRerankCalls(int count) {
      this.count = count;
    }

    /** The requests go to this model's endpoint and carry its name. */
    public ExpectedRerankCalls on(Model model) {
      this.model = model;
      return this;
    }

    /** The exact {@code query.text} of every request. */
    public ExpectedRerankCalls query(String query) {
      this.query = query;
      return this;
    }

    /** All passages of all requests together, as a multiset. */
    public ExpectedRerankCalls passages(String... passages) {
      this.passages = List.of(passages);
      return this;
    }

    /** The number of passages in each request, as a multiset. */
    public ExpectedRerankCalls batchSizes(Integer... sizes) {
      this.batchSizes = List.of(sizes);
      return this;
    }

    /** The request sent {@code reranking-api-key: key}, so the reranker gets {@code Bearer key}. */
    public ExpectedRerankCalls rerankingApiKey(String key) {
      this.authorization = "Bearer " + key;
      return this;
    }
  }
}
