package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class FindKeyspacesIntegrationTest extends AbstractNamespaceIntegrationTestBase {

  @Nested
  @Order(1)
  class FindKeyspaces {

    @Test
    public final void happyPath() {
      String json =
          """
          {
            "findKeyspaces": {
            }
          }
          """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.keyspaces", hasSize(greaterThanOrEqualTo(1)))
          .body("status.keyspaces", hasItem(namespaceName));
    }
  }

  @Nested
  @Order(2)
  class DeprecatedFindNamespaces {

    @Test
    public final void happyPath() {
      String json =
          """
              {
                "findNamespaces": {
                }
              }
              """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("status.namespaces", hasSize(greaterThanOrEqualTo(1)))
          .body("status.namespaces", hasItem(namespaceName))
          .body(
              "status.warnings",
              hasItem(
                  "This findNamespaces has been deprecated and will be removed in future releases, use findKeyspaces instead."));
    }
  }

  @Nested
  @Order(3)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindKeyspacesIntegrationTest.super.checkMetrics("FindKeyspacesCommand");
      // We decided to keep findNamespaces metrics and logs, even it is a deprecated command
      FindKeyspacesIntegrationTest.super.checkMetrics("FindNamespacesCommand");
      FindKeyspacesIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
