package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@QuarkusTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindEmbeddingProvidersIntegrationTest extends AbstractKeyspaceIntegrationTestBase {
  @Nested
  @Order(1)
  class FindEmbeddingProviders {

    @Test
    public final void happyPath() {
      // without option specified, only return supported models
      givenHeadersAndJson(
              """
                    {
                      "findEmbeddingProviders": {
                      }
                    }
                    """)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.embeddingProviders", notNullValue())
          .body("status.embeddingProviders.nvidia.url", notNullValue())
          .body("status.embeddingProviders.nvidia.models[0].vectorDimension", equalTo(1024))
          .body("status.embeddingProviders.nvidia.models[0].name", equalTo("NV-Embed-QA"))
          .body(
              "status.embeddingProviders.nvidia.models[0].apiModelSupport.status",
              equalTo(ApiModelSupport.SupportStatus.SUPPORTED.name()));
    }

    private static Stream<Arguments> returnedAllStatus() {
      return Stream.of(
          // emtpy string
          Arguments.of("\"\""),
          // null
          Arguments.of("null"));
    }

    @ParameterizedTest()
    @MethodSource("returnedAllStatus")
    public final void returnModelsWithAllStatus(String filterModelStatus) {
      givenHeadersAndJson(
                  """
                            {
                              "findEmbeddingProviders": {
                                "options": {
                                  "filterModelStatus": %s
                                }
                              }
                            }
                            """
                  .formatted(filterModelStatus))
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.embeddingProviders", notNullValue())
          .body("status.embeddingProviders.nvidia.models", hasSize(3))
          .body("status.embeddingProviders.nvidia.models[0].name", equalTo("NV-Embed-QA"))
          .body(
              "status.embeddingProviders.nvidia.models[0].apiModelSupport.status",
              equalTo(ApiModelSupport.SupportStatus.SUPPORTED.name()))
          .body(
              "status.embeddingProviders.nvidia.models[1].name",
              equalTo("a-EOL-nvidia-embedding-model"))
          .body(
              "status.embeddingProviders.nvidia.models[1].apiModelSupport.status",
              equalTo(ApiModelSupport.SupportStatus.END_OF_LIFE.name()))
          .body(
              "status.embeddingProviders.nvidia.models[2].name",
              equalTo("a-deprecated-nvidia-embedding-model"))
          .body(
              "status.embeddingProviders.nvidia.models[2].apiModelSupport.status",
              equalTo(ApiModelSupport.SupportStatus.DEPRECATED.name()));
    }

    @Test
    public final void returnModelsWithSpecifiedStatus() {
      givenHeadersAndJson(
              """
                                {
                                  "findEmbeddingProviders": {
                                    "options": {
                                      "filterModelStatus": "deprecated"
                                    }
                                  }
                                }
                                """)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.embeddingProviders", notNullValue())
          .body("status.embeddingProviders.nvidia.models", hasSize(1))
          .body(
              "status.embeddingProviders.nvidia.models[0].name",
              equalTo("a-deprecated-nvidia-embedding-model"))
          .body(
              "status.embeddingProviders.nvidia.models[0].apiModelSupport.status",
              equalTo(ApiModelSupport.SupportStatus.DEPRECATED.name()));
    }

    @Test
    public final void failedWithRandomStatus() {
      givenHeadersAndJson(
              """
                          {
                            "findEmbeddingProviders": {
                              "options": {
                                "filterModelStatus": "random"
                              }
                            }
                          }
                          """)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body(
              "errors[0].message",
              containsString(
                  "field 'command.options.filterModelStatus' value \"random\" not valid"));
    }
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindEmbeddingProvidersIntegrationTest.super.checkMetrics("FindEmbeddingProvidersCommand");
      FindEmbeddingProvidersIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
