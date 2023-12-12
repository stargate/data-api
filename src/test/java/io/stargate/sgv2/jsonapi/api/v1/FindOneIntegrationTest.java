package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindOne {
    private static final String DOC1_JSON =
        """
        {
          "_id": "doc1",
          "username": "user1",
          "active_user" : true
        }
        """;
    private static final String DOC2_JSON =
        """
        {
          "_id": "doc2",
          "username": "user2",
          "subdoc" : {
             "id" : "abc"
          },
          "array" : [
              "value1"
          ]
        }
        """;
    private static final String DOC3_JSON =
        """
        {
          "_id": "doc3",
          "username": "user3",
          "tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true],
          "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]
        }
        """;
    private static final String DOC4_JSON =
        """
        {
          "_id": "doc4",
          "indexedObject" : { "0": "value_0", "1": "value_1" }
        }
        """;
    private static final String DOC5_JSON =
        """
        {
          "_id": "doc5",
          "username": "user5",
          "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
        }
        """;

    @Test
    @Order(1)
    public void setUp() {
      insertDoc(DOC1_JSON);
      insertDoc(DOC2_JSON);
      insertDoc(DOC3_JSON);
      insertDoc(DOC4_JSON);
      insertDoc(DOC5_JSON);
    }

    @Test
    @Order(-1) // executed before insert
    public void noFilterNoDocuments() {
      String json =
          """
          {
            "findOne": {
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void noFilter() {
      String json =
          """
          {
            "findOne": {
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void emptyOptionsAllowed() {
      String json =
          """
          {
            "findOne": {
              "options": {}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void noFilterSortAscending() {
      String json =
          """
          {
            "findOne": {
              "sort" : {"username" : 1}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.document", jsonEquals(DOC4_JSON)); // missing value is the lowest precedence
    }

    @Test
    public void noFilterSortDescending() {
      String json =
          """
          {
            "findOne": {
              "sort" : {"username" : -1 }
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.document", jsonEquals(DOC5_JSON)); // missing value is the lowest precedence
    }

    @Test
    public void byId() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
          {
            "findOne": {
              "filter" : {"_id" : "doc1"}
            }
          }
          """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("data.document", jsonEquals(DOC1_JSON))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    // https://github.com/stargate/jsonapi/issues/572 -- is passing empty Object for "sort" ok?
    @Test
    public void byIdEmptySort() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                {
                  "findOne": {
                    "filter": {"_id" : "doc1"},
                    "sort": {}
                  }
                }
                """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("data.document", jsonEquals(DOC1_JSON))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void byIdNotFound() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"_id" : "none"}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void inCondition() {
      String json =
          """
        {
          "findOne": {
            "filter" : {"_id" : {"$in": ["doc5", "doc4"]}}
          }
        }
        """;
      // findOne resolves any one of the resolved documents. So the order of the documents in the
      // $in clause is not guaranteed.
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.document", anyOf(jsonEquals(DOC5_JSON), jsonEquals(DOC4_JSON)));
    }

    @Test
    public void inConditionEmptyArray() {
      String json =
          """
        {
          "findOne": {
            "filter" : {"_id" : {"$in": []}}
          }
        }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void inConditionNonArrayArray() {
      String json =
          """
        {
          "findOne": {
            "filter" : {"_id" : {"$in": true}}
          }
        }
        """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].message", is("$in operator must have `ARRAY`"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_FILTER_EXPRESSION"));
    }

    @Test
    public void ninConditionNonArrayArray() {
      String json =
          """
            {
              "findOne": {
                "filter" : {"_id" : {"$nin": false}}
              }
            }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].message", is("$nin operator must have `ARRAY`"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_FILTER_EXPRESSION"));
    }

    @Test
    public void inConditionNonIdField() {
      String json =
          """
        {
          "findOne": {
            "filter" : {"non_id" : {"$in": ["a", "b", "c"]}}
          }
        }
        """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()));
    }

    @Test
    public void byColumn() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"username" : "user1"}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("data.document", jsonEquals(DOC1_JSON))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void byColumnMissing() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"nickname" : "user1"}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void byColumnNotMatching() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"username" : "batman"}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withExistsOperatorSortAsc() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"username" : {"$exists" : true}},
              "sort" : {"username" : 1 }
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("data.document", jsonEquals(DOC1_JSON))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withExistsOperatorSortDesc() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"username" : {"$exists" : true}},
              "sort" : {"username" : -1}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          // post sorting by sort id , it uses document id by default.
          .body("data.document", jsonEquals(DOC5_JSON))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withExistsOperator() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"active_user" : {"$exists" : true}}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("data.document", jsonEquals(DOC1_JSON))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withExistsOperatorFalse() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"active_user" : {"$exists" : false}}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withExistsNotMatching() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"power_rating" : {"$exists" : true}}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withAllOperatorMissing() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"tags-and-button" : {"$all" : ["tag1", "tag2"]}}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withAllOperatorNotMatching() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"tags" : {"$all" : ["tag1", "tag2", "tag-not-there"]}}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withAllOperatorNotArray() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"tags" : {"$all" : 1}}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].message", is("$all operator must have `ARRAY` value"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_FILTER_EXPRESSION"));
    }

    @Test
    public void withSizeOperator() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"tags" : {"$size" : 6}}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("data.document", jsonEquals(DOC3_JSON))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withSizeOperatorNotMatching() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"tags" : {"$size" : 78}}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withSizeOperatorNotNumber() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"tags" : {"$size" : true}}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].message", is("$size operator must have integer"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_FILTER_EXPRESSION"));
    }
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindOneIntegrationTest.super.checkMetrics("FindOneCommand");
    }
  }
}
