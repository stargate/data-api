package io.stargate.sgv2.jsonapi.service.reranking.gateway;

import static io.grpc.Status.DEADLINE_EXCEEDED;
import static io.grpc.Status.UNAVAILABLE;
import static io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport.SupportStatus.SUPPORTED;
import static io.stargate.sgv2.jsonapi.service.provider.ModelProvider.NVIDIA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest;
import io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse;
import io.stargate.embedding.gateway.RerankingService;
import io.stargate.sgv2.jsonapi.api.model.command.CommandErrorFactory;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.exception.RerankingProviderException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider.Rank;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Regression tests for the reranker client used when the embedding gateway is enabled: reranking
 * then goes to the gateway over gRPC instead of to the model URL. The integration tests run without
 * a gateway, so these behaviors are checked here against an in-memory gateway. For review: the
 * tests call the same {@code rerank(query, passages, credentials)} as the command, so batching is
 * included, and map failures with {@link CommandErrorFactory} to get the error the API returns.
 */
class RerankingGatewayClientRegressionTest {

  private static final Tenant TENANT = Tenant.create(DatabaseType.ASTRA, "tenant-a", "us-east-1");
  private static final String QUERY = "which fruit is yellow?";
  // At most 3 retries, back-off 10 to 100 ms, read timeout 5000 ms, batches of 2 passages.
  private static final ModelConfigImpl MODEL =
      new ModelConfigImpl(
          "nvidia/gateway-test-model",
          new ApiModelSupport.ApiModelSupportImpl(SUPPORTED, Optional.empty()),
          false,
          "http://model-url-not-used.invalid",
          new ModelConfigImpl.RequestPropertiesImpl(3, 10, 5000, 100, 0.5, 2));

  // Each batch becomes one gRPC request with the model, query, command, provider, tenant and the
  // Data API token, plus RERANKING_API_KEY only for a non-empty key (an empty key is not rejected).
  // The client's authentication map is never sent; ranks and usage come from the gateway. The docs
  // do not specify this; the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource("rerankingKeyCases")
  void gatewayRequestAndResponse(String rerankingKey, Map<String, String> expectedTokens) {
    var gateway = new FakeGateway(RerankingGatewayClientRegressionTest::scoreByLength);
    var response = rerank(gateway, rerankingKey, List.of("ccc", "a", "bb"));

    var passageBatches =
        gateway.requests.stream().map(r -> List.copyOf(r.getRerankingRequest().getPassagesList()));
    assertThat(passageBatches).containsExactlyInAnyOrder(List.of("ccc", "a"), List.of("bb"));
    for (var request : gateway.requests) {
      var reranking = request.getRerankingRequest();
      var context = request.getProviderContext();
      assertThat(reranking.getModelName()).isEqualTo("nvidia/gateway-test-model");
      assertThat(reranking.getQuery()).isEqualTo(QUERY);
      assertThat(reranking.getCommandName()).isEqualTo("findAndRerank");
      assertThat(context.getProviderName()).isEqualTo("nvidia");
      assertThat(context.getTenantId()).isEqualTo("tenant-a");
      assertThat(context.getAuthTokensMap()).isEqualTo(expectedTokens);
      assertThat(request.toString()).doesNotContain("shared-secret-name");
    }
    assertThat(response.ranks()).containsExactly(new Rank(0, 3f), new Rank(1, 1f), new Rank(2, 2f));
    var usage = response.modelUsage();
    assertThat(usage.modelName()).isEqualTo("gateway-reported-model");
    assertThat(usage.promptTokens()).isEqualTo(3);
    assertThat(usage.totalTokens()).isEqualTo(6);
    assertThat(usage.batchCount()).isEqualTo(2);
  }

  static Stream<Arguments> rerankingKeyCases() {
    return Stream.of(
        Arguments.of(
            Named.of("reranking key given -> Data API token and reranking key are sent", "key-1"),
            Map.of("DATA_API_TOKEN", "data-api-token", "RERANKING_API_KEY", "key-1")),
        Arguments.of(
            Named.of("empty reranking key -> only the Data API token is sent, no error", ""),
            Map.of("DATA_API_TOKEN", "data-api-token")));
  }

  // Gateway error codes other than UNEXPECTED_SERVER_ERROR become RERANKING_PROVIDER_SERVER_ERROR
  // with the gateway code, title and body in the message; DEADLINE_EXCEEDED thrown by the gRPC call
  // becomes RERANKING_PROVIDER_TIMEOUT, whose message is the gRPC status message. The docs do not
  // specify this; the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource("rerankingErrorCases")
  void gatewayFailureBecomesRerankingError(
      FakeGateway gateway, Class<?> type, String errorCode, List<String> snippets) {
    assertFailure(gateway, type, errorCode, snippets);
  }

  static Stream<Arguments> rerankingErrorCases() {
    return Stream.of(
        gatewayError(
            "gateway error RERANKING_PROVIDER_TIMEOUT -> RERANKING_PROVIDER_SERVER_ERROR",
            "RERANKING_PROVIDER_TIMEOUT"),
        gatewayError(
            "gateway error RERANKING_PROVIDER_RATE_LIMITED -> RERANKING_PROVIDER_SERVER_ERROR",
            "RERANKING_PROVIDER_RATE_LIMITED"),
        gatewayError(
            "gateway error RERANKING_PROVIDER_CLIENT_ERROR -> RERANKING_PROVIDER_SERVER_ERROR",
            "RERANKING_PROVIDER_CLIENT_ERROR"),
        failure(
            "gRPC call throws DEADLINE_EXCEEDED -> RERANKING_PROVIDER_TIMEOUT",
            grpcFailure(DEADLINE_EXCEEDED, true),
            RerankingProviderException.class,
            "RERANKING_PROVIDER_TIMEOUT",
            "The reranking provider was: nvidia.",
            "status code was: DEADLINE_EXCEEDED.",
            "The error message was: DEADLINE_EXCEEDED: gateway failure."));
  }

  // No docs cover gateway failures, and a server error is treated as a possible bug. Main keeps the
  // gateway's UNEXPECTED_SERVER_ERROR, with the gateway body as the message, and passes any other
  // gRPC failure, thrown or in the Uni (DEADLINE_EXCEEDED included), through as an unhandled
  // exception. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource("serverErrorCases")
  void gatewayFailureEndsAsServerError(
      FakeGateway gateway, Class<?> type, String errorCode, List<String> snippets) {
    assertFailure(gateway, type, errorCode, snippets);
  }

  static Stream<Arguments> serverErrorCases() {
    return Stream.of(
        failure(
            "gateway error UNEXPECTED_SERVER_ERROR -> same code, gateway body as message",
            errorAnswer("UNEXPECTED_SERVER_ERROR"),
            ServerException.class,
            "UNEXPECTED_SERVER_ERROR",
            "gateway body"),
        unhandled("gRPC call throws UNAVAILABLE -> rethrown as is", UNAVAILABLE, true),
        unhandled("gRPC Uni fails with UNAVAILABLE -> passed through as is", UNAVAILABLE, false),
        unhandled(
            "gRPC Uni fails with DEADLINE_EXCEEDED -> passed through, not a timeout",
            DEADLINE_EXCEEDED,
            false));
  }

  /** Reranks one batch, then asserts the exception, the API error, and one call with no retry. */
  private static void assertFailure(
      FakeGateway gateway, Class<?> type, String errorCode, List<String> snippets) {
    var failure = catchThrowable(() -> rerank(gateway, "key-1", List.of("a", "bb")));
    assertThat(failure).isExactlyInstanceOf(type);
    var error = CommandErrorFactory.create(failure);
    assertThat(error.errorCode()).isEqualTo(errorCode);
    assertThat(error.httpStatus().getStatusCode()).isEqualTo(200);
    assertThat(error.message()).contains(snippets.toArray(String[]::new));
    // The model allows 3 retries, but the gateway is called once and its answer subscribed once.
    assertThat(gateway.requests).hasSize(1);
    assertThat(gateway.subscriptions.get()).isLessThanOrEqualTo(1);
  }

  /** Calls the client like the findAndRerank command, with an authentication map on the client. */
  private static RerankingProvider.RerankingResponse rerank(
      FakeGateway gateway, String rerankingKey, List<String> passages) {
    var authentication = Map.of("providerKey", "shared-secret-name");
    var client =
        new RerankingEGWClient(
            NVIDIA, MODEL, TENANT, "data-api-token", gateway, authentication, "findAndRerank");
    var credentials = new RerankingCredentials(TENANT, rerankingKey);
    return client.rerank(QUERY, passages, credentials).await().atMost(Duration.ofSeconds(10));
  }

  private static Arguments gatewayError(String description, String code) {
    return failure(
        description,
        errorAnswer(code),
        SchemaException.class,
        "RERANKING_PROVIDER_SERVER_ERROR",
        "Gateway Error Code: " + code,
        "Gateway Error Title: gateway title",
        "Gateway Error Body: gateway body");
  }

  private static Arguments unhandled(String description, Status status, boolean thrownByCall) {
    return failure(
        description,
        grpcFailure(status, thrownByCall),
        StatusRuntimeException.class,
        "UNEXPECTED_SERVER_ERROR",
        "Error Class: StatusRuntimeException",
        "Error Message: " + status.getCode() + ": gateway failure");
  }

  private static Arguments failure(
      String description, FakeGateway gateway, Class<?> type, String code, String... texts) {
    return Arguments.of(Named.of(description, gateway), type, code, List.of(texts));
  }

  /** Scores each passage by its length, listed in reverse passage order, with a usage record. */
  private static Uni<RerankingResponse> scoreByLength(ProviderRerankingRequest request) {
    var passages = request.getRerankingRequest().getPassagesList();
    var response = RerankingResponse.newBuilder();
    for (int i = passages.size() - 1; i >= 0; i--) {
      response.addRanksBuilder().setIndex(i).setScore(passages.get(i).length());
    }
    response
        .getModelUsageBuilder()
        .setModelType(EmbeddingGateway.ModelUsage.ModelType.RERANKING)
        .setModelProvider("nvidia")
        .setTenantId("tenant-a")
        .setModelName("gateway-reported-model")
        .setPromptTokens(passages.size())
        .setTotalTokens(2 * passages.size());
    return Uni.createFrom().item(response.build());
  }

  private static FakeGateway errorAnswer(String code) {
    var response = RerankingResponse.newBuilder();
    response.getErrorBuilder().setErrorCode(code).setErrorTitle("gateway title");
    response.getErrorBuilder().setErrorBody("gateway body");
    return new FakeGateway(request -> Uni.createFrom().item(response.build()));
  }

  private static FakeGateway grpcFailure(Status status, boolean thrownByCall) {
    var exception = status.withDescription("gateway failure").asRuntimeException();
    return new FakeGateway(
        request -> {
          if (thrownByCall) {
            throw exception;
          }
          return Uni.createFrom().failure(exception);
        });
  }

  /** In-memory gRPC gateway: records requests and counts subscriptions to its answers. */
  private static class FakeGateway implements RerankingService {
    final List<ProviderRerankingRequest> requests = new CopyOnWriteArrayList<>();
    final AtomicInteger subscriptions = new AtomicInteger();
    final Function<ProviderRerankingRequest, Uni<RerankingResponse>> answer;

    FakeGateway(Function<ProviderRerankingRequest, Uni<RerankingResponse>> answer) {
      this.answer = answer;
    }

    @Override
    public Uni<RerankingResponse> rerank(ProviderRerankingRequest request) {
      requests.add(request);
      return answer.apply(request).onSubscription().invoke(subscriptions::incrementAndGet);
    }

    @Override
    public Uni<EmbeddingGateway.GetSupportedRerankingProvidersResponse>
        getSupportedRerankingProviders(
            EmbeddingGateway.GetSupportedRerankingProvidersRequest request) {
      throw new UnsupportedOperationException("not used by the reranking client");
    }
  }
}
