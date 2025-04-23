package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.service.provider.ModelSupport;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindEmbeddingProvidersIntegrationTest extends AbstractKeyspaceIntegrationTestBase {
  @Nested
  @Order(1)
  class FindEmbeddingProviders {

    @Test
    public final void happyPath() {
      // without option specified, only return supported models
      String json =
          """
                    {
                      "findEmbeddingProviders": {
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
          .body("status.embeddingProviders", notNullValue())
          .body("status.embeddingProviders.nvidia.url", notNullValue())
          .body("status.embeddingProviders.nvidia.models[0].vectorDimension", equalTo(1024))
          .body("status.embeddingProviders.nvidia.models[0].name", equalTo("NV-Embed-QA"))
          .body(
              "status.embeddingProviders.nvidia.models[0].modelSupport.status",
              equalTo(ModelSupport.SupportStatus.SUPPORTED.name()));
    }

    @Test
    public final void returnModelsWithAllStatus() {
      String json =
          """
                            {
                              "findEmbeddingProviders": {
                                "options": {
                                  "includeModelStatus": ""
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
          .body("status.embeddingProviders", notNullValue())
          .body("status.embeddingProviders.nvidia.models", hasSize(3))
          .body("status.embeddingProviders.nvidia.models[0].name", equalTo("NV-Embed-QA"))
          .body(
              "status.embeddingProviders.nvidia.models[0].modelSupport.status",
              equalTo(ModelSupport.SupportStatus.SUPPORTED.name()))
          .body(
              "status.embeddingProviders.nvidia.models[1].name",
              equalTo("a-EOL-nvidia-embedding-model"))
          .body(
              "status.embeddingProviders.nvidia.models[1].modelSupport.status",
              equalTo(ModelSupport.SupportStatus.END_OF_LIFE.name()))
          .body(
              "status.embeddingProviders.nvidia.models[2].name",
              equalTo("a-deprecated-nvidia-embedding-model"))
          .body(
              "status.embeddingProviders.nvidia.models[2].modelSupport.status",
              equalTo(ModelSupport.SupportStatus.DEPRECATED.name()));
    }

    @Test
    public final void returnModelsWithSpecifiedStatus() {
      String json =
          """
                                {
                                  "findEmbeddingProviders": {
                                    "options": {
                                      "includeModelStatus": "deprecated"
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
          .body("status.embeddingProviders", notNullValue())
          .body("status.embeddingProviders.nvidia.models", hasSize(1))
          .body(
              "status.embeddingProviders.nvidia.models[0].name",
              equalTo("a-deprecated-nvidia-embedding-model"))
          .body(
              "status.embeddingProviders.nvidia.models[0].modelSupport.status",
              equalTo(ModelSupport.SupportStatus.DEPRECATED.name()));
    }

    @Test
    public final void failedWithRandomStatus() {
      String json =
          """
                          {
                            "findEmbeddingProviders": {
                              "options": {
                                "includeModelStatus": "random"
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
                  "field 'command.options.includeModelStatus' value \"random\" not valid"));
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
