package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class DeleteCollectionIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  @Nested
  @Order(1)
  class DeleteCollection {

    @Test
    public void happyPath() {
      String collection = RandomStringUtils.randomAlphabetic(16);

      // first create
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
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // then delete
      givenHeadersAndJson(
                  """
              {
                "deleteCollection": {
                  "name": "%s"
                }
              }
              """
                  .formatted(collection))
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
    }

    @Test
    public void notExisting() {
      String collection = RandomStringUtils.randomAlphabetic(16);

      // delete not existing
      givenHeadersAndJson(
                  """
              {
                "deleteCollection": {
                  "name": "%s"
                }
              }
              """
                  .formatted(collection))
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
    }

    // [data-api#1186]: handling of non-existing keyspace
    @Test
    public void nonExistingKeyspace() {
      givenHeadersAndJson(
              """
              {
                "deleteCollection": {
                  "name": "some-collection"
                }
              }
          """)
          .when()
          .post(KeyspaceResource.BASE_PATH, "no_such_keyspace")
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("KEYSPACE_DOES_NOT_EXIST"))
          .body("errors[0].message", containsString("no_such_keyspace"));
    }

    @Test
    public void invalidCommand() {
      givenHeadersAndJson(
              """
              {
                "deleteCollection": {
                }
              }
          """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body(
              "errors[0].message",
              is(
                  "Request invalid: field 'command.name' value `null` not valid. Problem: must not be empty."));
    }
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      DeleteCollectionIntegrationTest.super.checkMetrics("DeleteCollectionCommand");
      DeleteCollectionIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
