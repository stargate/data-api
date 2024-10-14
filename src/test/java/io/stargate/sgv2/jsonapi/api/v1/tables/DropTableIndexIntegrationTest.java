package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class DropTableIndexIntegrationTest extends AbstractTableIntegrationTestBase {

  String simpleTableName = "simpleTableForDropIndexTest";

  @BeforeAll
  public final void createSimpleTable() {
    String tableJson =
            """
                              {
                                      "name": "%s",
                                      "definition": {
                                          "columns": {
                                              "id": {
                                                  "type": "text"
                                              },
                                              "age": {
                                                  "type": "int"
                                              },
                                              "name": {
                                                  "type": "text"
                                              }
                                          },
                                          "primaryKey": "id"
                                      }
                            }
                        """
            .formatted(simpleTableName);
    createTable(tableJson);
  }

  @Nested
  @Order(1)
  class DropIndexSuccess {

    @Test
    @Order(1)
    public void dropIndex() {
      String createIndexJson =
          """
          {
              "createIndex": {
                  "name": "age_idx",
                  "definition": {
                     "column": "age"
                  }
              }
          }
          """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(createIndexJson)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, simpleTableName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));

      String dropIndexJson =
          """
                                {
                                    "dropIndex": {
                                        "indexName": "age_idx"
                                    }
                                }
                                """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(dropIndexJson)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, simpleTableName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }
  }
}
