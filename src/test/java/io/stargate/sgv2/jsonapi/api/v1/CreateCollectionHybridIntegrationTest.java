package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
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
class CreateCollectionHybridIntegrationTest extends AbstractKeyspaceIntegrationTestBase {
  @Nested
  @Order(1)
  class CreateLexicalHappyPath {
    @Test
    void createLexicalSimple() {
      final String collectionName = "coll_lexical_" + RandomStringUtils.randomNumeric(16);
      String json =
          createRequestWithLexical(
              collectionName,
              """
                {
                  "enabled": "true",
                  "analyzer": "standard"
                }
          """);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
      deleteCollection(collectionName);
    }
  }

  private String createRequestWithLexical(String collectionName, String lexicalDef) {
    return
        """
                  {
                    "createCollection": {
                      "name": "%s",
                      "options": {
                        "lexical": %s
                      }
                    }
                  }
                  """
        .formatted(collectionName, lexicalDef);
  }

  private void deleteCollection(String collectionName) {
    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(
                """
                        {
                          "deleteCollection": {
                            "name": "%s"
                          }
                        }
                        """
                .formatted(collectionName))
        .when()
        .post(KeyspaceResource.BASE_PATH, keyspaceName)
        .then()
        .statusCode(200)
        .body("$", responseIsDDLSuccess())
        .body("status.ok", is(1));
  }
}
