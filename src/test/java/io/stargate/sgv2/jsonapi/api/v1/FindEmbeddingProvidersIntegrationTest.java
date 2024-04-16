package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindEmbeddingProvidersIntegrationTest extends AbstractNamespaceIntegrationTestBase {
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
