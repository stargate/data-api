package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static io.stargate.sgv2.common.IntegrationTestUtils.getTestPort;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindVectorProvidersIntegrationTest extends AbstractNamespaceIntegrationTestBase {
  @Nested
  @Order(1)
  class FindVectorProviders {

    @Test
    public final void happyPath() {
      String json =
          """
                    {
                      "findVectorProviders": {
                      }
                    }
                    """;

      given()
          .port(getTestPort())
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.vectorProviders", notNullValue());
    }
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindVectorProvidersIntegrationTest.super.checkMetrics("FindVectorProvidersCommand");
      FindVectorProvidersIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
