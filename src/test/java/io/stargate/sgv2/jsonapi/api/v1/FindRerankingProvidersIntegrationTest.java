package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindRerankingProvidersIntegrationTest extends AbstractKeyspaceIntegrationTestBase {
  @Nested
  @Order(1)
  class FindRerankingProviders {

    @Test
    public final void defaultSupportModels() {
      // without option specified, only return supported models
      String json =
          """
                    {
                      "findRerankingProviders": {
                      }
                    }
                    """;

      given()
          .port(getTestPort())
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.rerankingProviders", notNullValue())
          .body("status.rerankingProviders.nvidia.models", hasSize(1))
          .body(
              "status.rerankingProviders.nvidia.models[0].name",
              equalTo("nvidia/llama-3.2-nv-rerankqa-1b-v2"))
          .body(
              "status.rerankingProviders.nvidia.models[0].apiModelSupport.status",
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
      String json =
              """
                            {
                              "findRerankingProviders": {
                                "options": {
                                  "filterModelStatus": %s
                                }
                              }
                            }
                            """
              .formatted(filterModelStatus);

      given()
          .port(getTestPort())
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.rerankingProviders", notNullValue())
          .body("status.rerankingProviders.nvidia.models", hasSize(3))
          .body(
              "status.rerankingProviders.nvidia.models[0].name",
              equalTo("nvidia/a-random-EOL-model"))
          .body(
              "status.rerankingProviders.nvidia.models[0].apiModelSupport.status",
              equalTo(ApiModelSupport.SupportStatus.END_OF_LIFE.name()))
          .body(
              "status.rerankingProviders.nvidia.models[1].name",
              equalTo("nvidia/a-random-deprecated-model"))
          .body(
              "status.rerankingProviders.nvidia.models[1].apiModelSupport.status",
              equalTo(ApiModelSupport.SupportStatus.DEPRECATED.name()))
          .body(
              "status.rerankingProviders.nvidia.models[2].name",
              equalTo("nvidia/llama-3.2-nv-rerankqa-1b-v2"))
          .body(
              "status.rerankingProviders.nvidia.models[2].apiModelSupport.status",
              equalTo(ApiModelSupport.SupportStatus.SUPPORTED.name()));
    }

    @Test
    public final void returnModelsWithSpecifiedStatus() {
      String json =
          """
                                    {
                                      "findRerankingProviders": {
                                        "options": {
                                          "filterModelStatus": "deprecated"
                                        }
                                      }
                                    }
                                    """;

      given()
          .port(getTestPort())
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.rerankingProviders", notNullValue())
          .body("status.rerankingProviders.nvidia.models", hasSize(1))
          .body(
              "status.rerankingProviders.nvidia.models[0].name",
              equalTo("nvidia/a-random-deprecated-model"))
          .body(
              "status.rerankingProviders.nvidia.models[0].apiModelSupport.status",
              equalTo(ApiModelSupport.SupportStatus.DEPRECATED.name()));
    }

    @Test
    public final void failedWithRandomStatus() {
      String json =
          """
                      {
                        "findRerankingProviders": {
                          "options": {
                              "filterModelStatus": "random"
                          }
                        }
                      }
                      """;

      given()
          .port(getTestPort())
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
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
      FindRerankingProvidersIntegrationTest.super.checkMetrics("FindRerankingProvidersCommand");
      FindRerankingProvidersIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
