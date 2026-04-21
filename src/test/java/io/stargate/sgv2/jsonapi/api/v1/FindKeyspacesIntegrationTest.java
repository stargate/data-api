package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.config.constants.ErrorConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorFamily;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class FindKeyspacesIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  @Nested
  @Order(1)
  class FindKeyspaces {

    @Test
    public final void happyPath() {
      givenHeadersAndJson(
              """
          {
            "findKeyspaces": {
            }
          }
          """)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.keyspaces", hasSize(greaterThanOrEqualTo(1)))
          .body("status.keyspaces", hasItem(keyspaceName));
    }
  }

  @Nested
  @Order(2)
  class DeprecatedFindNamespaces {

    @Test
    public final void happyPath() {
      givenHeadersAndJson(
              """
              {
                "findNamespaces": {
                }
              }
              """)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.namespaces", hasSize(greaterThanOrEqualTo(1)))
          .body("status.namespaces", hasItem(keyspaceName))
          .body("status.warnings", hasSize(1))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorConstants.Fields.FAMILY, ErrorFamily.REQUEST.name()))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorConstants.Fields.SCOPE, RequestException.Scope.WARNING.scope()))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorConstants.Fields.CODE, WarningException.Code.DEPRECATED_COMMAND.name()))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorConstants.Fields.CODE, WarningException.Code.DEPRECATED_COMMAND.name()))
          .body(
              "status.warnings[0].message",
              containsString("The deprecated command is: findNamespaces."))
          .body(
              "status.warnings[0].message",
              containsString("The new command to use is: findKeyspaces."));
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
