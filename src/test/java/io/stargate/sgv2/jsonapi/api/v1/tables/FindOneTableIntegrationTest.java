package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneTableIntegrationTest extends AbstractTableIntegrationTestBase {
  static final String TABLE_WITH_STRING_ID_AGE_NAME = "findOneSingleStringKeyTable";

  @BeforeAll
  public final void createDefaultTables() {
    createTableWithColumns(
        TABLE_WITH_STRING_ID_AGE_NAME,
        Map.of(
            "id",
            Map.of("type", "text"),
            "age",
            Map.of("type", "int"),
            "name",
            Map.of("type", "text")),
        "id");
  }

  // On-empty tests to be run before ones that populate tables
  @Nested
  @Order(1)
  class FindOneOnEmpty {
    @Test
    public void findOnEmptyNoFilter() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                                  {
                                    "findOne": { }
                                  }
                              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .then()
          .statusCode(200)
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOnEmptyNonMatchingFilter() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                                  {
                                    "findOne": {
                                        "filter": {
                                            "id": "nosuchkey"
                                        }
                                    }
                                  }
                              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()));
    }
  }

  @Nested
  @Order(2)
  class FindOneSuccess {
    @Test
    @Order(1)
    public void findOneSingleStringKey() {
      // First, insert 2 documents:
      insertOneInTable(
          TABLE_WITH_STRING_ID_AGE_NAME,
          """
                      {
                          "id": "a",
                          "age": 20,
                          "name": "John"
                      }
                      """);
      insertOneInTable(
          TABLE_WITH_STRING_ID_AGE_NAME,
          """
                      {
                          "id": "b",
                          "age": 40,
                          "name": "Bob"
                      }
                      """);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                          {
                            "findOne": {
                                "filter": {
                                    "id": "b"
                                }
                            }
                          }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .then()
          .statusCode(200)
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }
  }

  @Nested
  @Order(3)
  class FindOneFail {
    @Test
    @Order(1)
    public void failOnUnknownColumn() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                          {
                            "findOne": {
                              "filter": {
                                "unknown": "a"
                              }
                            }
                          }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, TABLE_WITH_STRING_ID_AGE_NAME)
          .then()
          // Not like it should be, but until fixed let's verify current behavior
          .statusCode(500)
          .body("data", is(nullValue()))
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SERVER_UNHANDLED_ERROR"))
          .body(
              "errors[0].message",
              containsString("FilterClause Clause does not support validating for Tables"));
    }
  }
}