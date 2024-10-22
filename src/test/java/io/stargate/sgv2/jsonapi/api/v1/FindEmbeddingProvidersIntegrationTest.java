package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
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
          .body("status.embeddingProviders.nvidia.models[0].vectorDimension", equalTo(1024));
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
