package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.api.v1.AbstractNamespaceIntegrationTestBase;
import io.stargate.sgv2.jsonapi.api.v1.NamespaceResource;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.Test;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class CreateTableIntegrationTest extends AbstractNamespaceIntegrationTestBase {

  @Nested
  @Order(1)
  class CreateTable {
    @Test
    public void primaryKeyAsString() {
      String json =
          """
                            {
                                 "createTable": {
                                     "name": "primaryKeyAsStringTable",
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
                    """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      deleteTable("primaryKeyAsStringTable");
    }

    @Test
    public void primaryKeyAsJsonObject() {

      String json =
          """
                        {
                            "createTable": {
                                "name": "primaryKeyAsJsonObjectTable",
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
                                    "primaryKey": {
                                        "partitionBy": [
                                            "id"
                                        ],
                                        "partitionSort" : {
                                            "name" : 1, "age" : -1
                                        }
                                    }
                                }
                            }
                        }
                    """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
      deleteTable("primaryKeyAsJsonObjectTable");
    }
  }

  private void deleteTable(String tableName) {
    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(
                """
                                {
                                  "deleteTable": {
                                    "name": "%s"
                                  }
                                }
                                """
                .formatted(tableName))
        .when()
        .post(NamespaceResource.BASE_PATH, namespaceName)
        .then()
        .statusCode(200)
        .body("status.ok", is(1));
  }
}
