package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants;
import io.stargate.sgv2.jsonapi.exception.ErrorFamily;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class DropKeyspaceIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  @Nested
  @Order(1)
  class DropKeyspace {

    @Test
    public final void happyPath() {
      givenHeadersAndJson(
                  """
          {
            "dropKeyspace": {
              "name": "%s"
            }
          }
          """
                  .formatted(keyspaceName))
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.ok", is(1));

      // ensure it's dropped
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
          .body("$", responseIsStatusOnly())
          .body("status.keyspaces", not(hasItem(keyspaceName)));
    }

    @Test
    public final void withExistingCollection() {
      String keyspace = "k%s".formatted(RandomStringUtils.randomAlphanumeric(8)).toLowerCase();
      String collection = "c%s".formatted(RandomStringUtils.randomAlphanumeric(8)).toLowerCase();

      givenHeadersAndJson(
                  """
              {
                "createKeyspace": {
                  "name": "%s"
                }
              }
              """
                  .formatted(keyspace))
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.ok", is(1));
      givenHeadersAndJson(
                  """
              {
                "createCollection": {
                  "name": "%s"
                }
              }
              """
                  .formatted(collection))
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspace)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.ok", is(1));

      givenHeadersAndJson(
                  """
          {
            "dropKeyspace": {
              "name": "%s"
            }
          }
          """
                  .formatted(keyspace))
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.ok", is(1));

      // ensure it's dropped
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
          .body("$", responseIsStatusOnly())
          .body("status.keyspaces", not(hasItem(keyspace)));
    }

    @Test
    public final void notExisting() {
      givenHeadersAndJson(
              """
              {
                "dropKeyspace": {
                  "name": "whatever_not_there"
                }
              }
              """)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.ok", is(1));
    }
  }

  @Nested
  @Order(2)
  class DeprecatedDropNamespace {

    @Test
    public final void happyPath() {
      givenHeadersAndJson(
                  """
          {
            "dropNamespace": {
              "name": "%s"
            }
          }
          """
                  .formatted(keyspaceName))
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.ok", is(1))
          .body("status.warnings", hasSize(1))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorObjectV2Constants.Fields.FAMILY, ErrorFamily.REQUEST.name()))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorObjectV2Constants.Fields.SCOPE, RequestException.Scope.WARNING.scope()))
          .body(
              "status.warnings[0]",
              hasEntry(
                  ErrorObjectV2Constants.Fields.CODE,
                  WarningException.Code.DEPRECATED_COMMAND.name()))
          .body(
              "status.warnings[0]",
              hasEntry(
                  ErrorObjectV2Constants.Fields.CODE,
                  WarningException.Code.DEPRECATED_COMMAND.name()))
          .body(
              "status.warnings[0].message",
              containsString("The deprecated command is: dropNamespace."))
          .body(
              "status.warnings[0].message",
              containsString("The new command to use is: dropKeyspace."));

      // ensure it's dropped
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
          .body("$", responseIsStatusOnly())
          .body("status.keyspaces", not(hasItem(keyspaceName)));
    }

    @Test
    public final void withExistingCollection() {
      String keyspace = "k%s".formatted(RandomStringUtils.randomAlphanumeric(8)).toLowerCase();
      String collection = "c%s".formatted(RandomStringUtils.randomAlphanumeric(8)).toLowerCase();

      givenHeadersAndJson(
                  """
              {
                "createKeyspace": {
                  "name": "%s"
                }
              }
              """
                  .formatted(keyspace))
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.ok", is(1));
      givenHeadersAndJson(
                  """
              {
                "createCollection": {
                  "name": "%s"
                }
              }
              """
                  .formatted(collection))
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspace)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.ok", is(1));

      givenHeadersAndJson(
                  """
          {
            "dropNamespace": {
              "name": "%s"
            }
          }
          """
                  .formatted(keyspace))
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.ok", is(1))
          .body("status.warnings", hasSize(1))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorObjectV2Constants.Fields.FAMILY, ErrorFamily.REQUEST.name()))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorObjectV2Constants.Fields.SCOPE, RequestException.Scope.WARNING.scope()))
          .body(
              "status.warnings[0]",
              hasEntry(
                  ErrorObjectV2Constants.Fields.CODE,
                  WarningException.Code.DEPRECATED_COMMAND.name()))
          .body(
              "status.warnings[0].message",
              containsString("The deprecated command is: dropNamespace."))
          .body(
              "status.warnings[0].message",
              containsString("The new command to use is: dropKeyspace."));
      ;
      // ensure it's dropped
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
          .body("$", responseIsStatusOnly())
          .body("status.keyspaces", not(hasItem(keyspace)));
    }

    @Test
    public final void notExisting() {
      givenHeadersAndJson(
              """
          {
            "dropNamespace": {
              "name": "whatever_not_there"
            }
          }
          """)
          .when()
          .post(GeneralResource.BASE_PATH)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.ok", is(1))
          .body("status.warnings", hasSize(1))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorObjectV2Constants.Fields.FAMILY, ErrorFamily.REQUEST.name()))
          .body(
              "status.warnings[0]",
              hasEntry(ErrorObjectV2Constants.Fields.SCOPE, RequestException.Scope.WARNING.scope()))
          .body(
              "status.warnings[0]",
              hasEntry(
                  ErrorObjectV2Constants.Fields.CODE,
                  WarningException.Code.DEPRECATED_COMMAND.name()))
          .body(
              "status.warnings[0].message",
              containsString("The deprecated command is: dropNamespace."))
          .body(
              "status.warnings[0].message",
              containsString("The new command to use is: dropKeyspace."));
      ;
    }
  }

  @Nested
  @Order(3)
  class Metrics {
    @Test
    public void checkMetrics() {
      DropKeyspaceIntegrationTest.super.checkMetrics("DropKeyspaceCommand");
      // We decided to keep dropNamespace metrics and logs, even it is a deprecated command
      DropKeyspaceIntegrationTest.super.checkMetrics("DropNamespaceCommand");
      DropKeyspaceIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
