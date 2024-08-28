package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.containsString;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
// TODO, it is currently called AddIndex, do we want to change to createIndex
class AddTableIndexIntegrationTest extends AbstractTableIntegrationTestBase {
  String simpleTableName = "simpleTableForAddIndexTest";

  @BeforeAll
  public final void createSimpleTable() {
    String tableJson =
            """
                          {
                              "createTable": {
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
                          }
                    """
            .formatted(simpleTableName);
    createTable(tableJson);
  }

  @Nested
  @Order(1)
  class AddIndexSuccess {

    @Test
    @Order(1)
    public void addIndex() {
      String json =
          """
                      {
                          "addIndex": {
                              "column": "age",
                              "indexName": "age_index"
                          }
                      }
                      """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, simpleTableName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }
  }

  @Nested
  @Order(2)
  class AddIndexFailure {
    @Test
    public void addIndex() {
      String json =
          """
                      {
                          "addIndex": {
                              "column": "city",
                              "indexName": "city_index"
                          }
                      }
                      """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, simpleTableName)
          .then()
          .statusCode(200)
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_QUERY"))
          .body("errors[0].message", containsString("Undefined column name city"));
    }
  }
}
