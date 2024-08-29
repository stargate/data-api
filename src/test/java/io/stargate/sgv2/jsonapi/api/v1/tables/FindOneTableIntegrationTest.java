package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.restassured.RestAssured.given;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
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
      givenFindOneArgPost(TABLE_WITH_STRING_ID_AGE_NAME, 200, "{ }")
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOnEmptyNonMatchingFilter() {
      givenFindOneArgPost(
              TABLE_WITH_STRING_ID_AGE_NAME,
              200,
              """
                                  {
                                    "filter": {
                                        "id": "nosuchkey"
                                    }
                                  }
                              """)
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
      final String DOC_B_JSON =
          """
                          {
                              "id": "b",
                              "age": 40,
                              "name": "Bob"
                          }
                          """;
      insertOneInTable(TABLE_WITH_STRING_ID_AGE_NAME, DOC_B_JSON);

      givenFindOneArgPost(
              TABLE_WITH_STRING_ID_AGE_NAME,
              200,
              """
                          {
                              "filter": {
                                  "id": "b"
                              }
                          }
                      """)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.document", jsonEquals(DOC_B_JSON));
    }

    @Test
    @Order(2)
    public void findOneDocUpperCaseKey() {
      final String TABLE_NAME = "findOneDocUpperCaseKey";
      createTableWithColumns(
          TABLE_NAME, Map.of("Id", Map.of("type", "int"), "value", Map.of("type", "text")), "Id");

      // Insert 2 documents:
      insertOneInTable(
          TABLE_NAME,
          """
                          {
                              "Id": 1,
                              "value": "a"
                          }
                          """);
      final String DOC_B_JSON =
          """
                          {
                              "Id": 2,
                              "value": "b"
                          }
                              """;
      insertOneInTable(TABLE_NAME, DOC_B_JSON);

      givenFindOneArgPost(
              TABLE_NAME,
              200,
              """
              {
                    "filter": {
                        "Id": 2
                    }
              }
          """)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.document", jsonEquals(DOC_B_JSON));
    }

    @Test
    @Order(3)
    public void findOneDocIdKey() {
      final String TABLE_NAME = "findOneDocIdKeyTable";
      createTableWithColumns(
          TABLE_NAME,
          Map.of(
              "_id",
              Map.of("type", "int"),
              "desc",
              Map.of("type", "text"),
              "valueLong",
              Map.of("type", "bigint"),
              "valueDouble",
              Map.of("type", "double")),
          "_id");

      // First, insert 2 documents:
      insertOneInTable(
          TABLE_NAME,
          """
                              {
                                  "_id": 1,
                                  "desc": "a",
                                  "valueLong": 1234567890,
                                  "valueDouble": -1.25
                              }
                              """);
      final String DOC_B_JSON =
          """
                              {
                                  "_id": 2,
                                  "desc": "b",
                                  "valueLong": 42,
                                  "valueDouble": 0.5
                              }
                                  """;
      insertOneInTable(TABLE_NAME, DOC_B_JSON);

      givenFindOneArgPost(
              TABLE_NAME,
              200,
              """
                                          {
                                                "filter": {
                                                    "_id": 2
                                                }
                                          }
                                      """)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.document", jsonEquals(DOC_B_JSON));
    }
  }

  @Nested
  @Order(3)
  class FindOneFail {
    @Test
    @Order(1)
    public void failOnUnknownColumn() {
      givenFindOneArgPost(
              TABLE_WITH_STRING_ID_AGE_NAME,
              200,
              """
                          {
                              "filter": {
                                "unknown": "a"
                              }
                          }
                      """)
          .body("data", is(nullValue()))
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("TABLE_COLUMN_UNKNOWN"))
          .body(
              "errors[0].message",
              containsString("Column unknown: No column with name 'unknown' found in table"));
    }

    @Test
    @Order(2)
    public void failOnNonKeyColumn() {
      givenFindOneArgPost(
              TABLE_WITH_STRING_ID_AGE_NAME,
              200,
              """
                                  {
                                      "filter": {
                                        "age": 80
                                    }
                                  }
                              """)
          .body("data", is(nullValue()))
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          // 22-Aug-2024, tatu: Not optimal, leftovers from Collections... but has to do
          //    on short term
          .body("errors[0].errorCode", is("NO_INDEX_ERROR"))
          .body("errors[0].message", containsString("Faulty collection (missing indexes)."));
    }
  }

  private ValidatableResponse givenFindOneArgPost(String table, int expStatus, String findOneArg) {
    return given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body("{ \"findOne\": %s }".formatted(findOneArg))
        .when()
        .post(CollectionResource.BASE_PATH, namespaceName, table)
        .then()
        .statusCode(expStatus);
  }
}
