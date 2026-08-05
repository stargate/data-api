package io.stargate.sgv2.jsonapi.service.reranking.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RerankingProviderRetryTest {

  @Test
  void retriesServerErrorOnceThenReturnsSuccess() {
    RetryingTestProvider provider = new RetryingTestProvider();
    AtomicInteger calls = new AtomicInteger();
    Response successfulResponse = response(Response.Status.OK);

    Response result =
        provider
            .execute(
                Uni.createFrom()
                    .deferred(
                        () ->
                            Uni.createFrom()
                                .item(
                                    calls.incrementAndGet() == 1
                                        ? response(Response.Status.INTERNAL_SERVER_ERROR)
                                        : successfulResponse)))
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    assertThat(result).isSameAs(successfulResponse);
    assertThat(calls).hasValue(2);
  }

  @Test
  void stopsAfterOneServerErrorRetry() {
    RetryingTestProvider provider = new RetryingTestProvider();
    AtomicInteger calls = new AtomicInteger();

    Throwable failure =
        provider
            .execute(
                Uni.createFrom()
                    .deferred(
                        () -> {
                          calls.incrementAndGet();
                          return Uni.createFrom()
                              .item(response(Response.Status.INTERNAL_SERVER_ERROR));
                        }))
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitFailure()
            .getFailure();

    assertThat(calls).hasValue(2);
    assertThat(failure)
        .isInstanceOf(SchemaException.class)
        .satisfies(
            throwable ->
                assertThat(((SchemaException) throwable).code)
                    .isEqualTo(SchemaException.Code.RERANKING_PROVIDER_SERVER_ERROR.name()));
  }

  @Test
  void doesNotRetryClientError() {
    RetryingTestProvider provider = new RetryingTestProvider();
    AtomicInteger calls = new AtomicInteger();

    Throwable failure =
        provider
            .execute(
                Uni.createFrom()
                    .deferred(
                        () -> {
                          calls.incrementAndGet();
                          return Uni.createFrom().item(response(Response.Status.BAD_REQUEST));
                        }))
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitFailure()
            .getFailure();

    assertThat(calls).hasValue(1);
    assertThat(failure)
        .isInstanceOf(SchemaException.class)
        .satisfies(
            throwable ->
                assertThat(((SchemaException) throwable).code)
                    .isEqualTo(SchemaException.Code.RERANKING_PROVIDER_CLIENT_ERROR.name()));
  }

  private static Response response(Response.Status status) {
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(status.getStatusCode());
    when(response.getStatusInfo()).thenReturn(status);
    when(response.getMediaType()).thenReturn(MediaType.TEXT_PLAIN_TYPE);
    when(response.readEntity(String.class)).thenReturn("provider response");
    return response;
  }

  private static final class RetryingTestProvider extends RerankingProvider {

    private RetryingTestProvider() {
      super(ModelProvider.NVIDIA, modelConfig());
    }

    private Uni<Response> execute(Uni<Response> request) {
      return retryHTTPCall(request);
    }

    @Override
    protected String errorMessageJsonPtr() {
      return "/message";
    }

    @Override
    public Uni<BatchedRerankingResponse> rerank(
        int batchId,
        String query,
        List<String> passages,
        RerankingCredentials rerankingCredentials) {
      throw new UnsupportedOperationException("Not used by retry tests");
    }

    private static RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig() {
      return new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
          "test-model",
          new ApiModelSupport.ApiModelSupportImpl(
              ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
          false,
          "http://testing.com",
          new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
              .RequestPropertiesImpl(1, 1, 100, 1, 0.0, 10));
    }
  }
}
