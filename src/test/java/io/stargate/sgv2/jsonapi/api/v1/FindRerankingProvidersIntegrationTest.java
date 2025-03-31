package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindRerankingProvidersIntegrationTest extends AbstractKeyspaceIntegrationTestBase {
  @Nested
  @Order(1)
  class FindEmbeddingProviders {

    @Test
    public final void happyPath() {
      String json =
          """
                    {
                      "findRerankingProviders": {
                      }
                    }
                    """;

      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsStatusOnly())
          .body("status.rerankingProviders", notNullValue())
          .body(
              "status.rerankingProviders.nvidia.models[0].name",
              equalTo("nvidia/llama-3.2-nv-rerankqa-1b-v2"))
          .body(
              "status.rerankingProviders.nvidia.models[0].modelSupport.status",
              equalTo(
                  RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.ModelSupport
                      .SupportStatus.SUPPORTING
                      .status))
          .body(
              "status.rerankingProviders.nvidia.models[1].name",
              equalTo("nvidia/a-random-deprecated-model"))
          .body(
              "status.rerankingProviders.nvidia.models[1].modelSupport.status",
              equalTo(
                  RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.ModelSupport
                      .SupportStatus.DEPRECATED
                      .status))
          .body(
              "status.rerankingProviders.nvidia.models[2].name",
              equalTo("nvidia/a-random-EOL-model"))
          .body(
              "status.rerankingProviders.nvidia.models[2].modelSupport.status",
              equalTo(
                  RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.ModelSupport
                      .SupportStatus.END_OF_LIFE
                      .status));
    }
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindRerankingProvidersIntegrationTest.super.checkMetrics("FindRerankingProvidersCommand");
      FindRerankingProvidersIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
