package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindOneWithJSONExtensions {
    private final String OBJECTID_ID1 = "5f3e3b2e4f6e6b6e6f6e6f6e";
    private final String OBJECTID_LEAF = "5f3e3b2e4f6e6b6e6f6eaaaa";
    private final String OBJECTID_X = "5f3e3b2e4f6e6b6e6f6effff";

    private final String UUID_ID1 = "CB34673E-B7CF-4429-AB73-D6306FF427EE";
    private final String UUID_LEAF = "C576C182-4266-423E-A621-32951D160EC8";

    private final String UUID_X = "BB3F3A87-98B7-4B85-B1D1-706A9FBC6807";

    private final String DOC1 =
        """
                    {
                      "_id": {"$objectId": "%s"},
                      "value": 1,
                      "stuff": {
                           "id": "id1"
                      }
                    }
                    """
            .formatted(OBJECTID_ID1);

    private final String DOC2 =
        """
                    {
                      "_id": {"$uuid": "%s"},
                      "value": 2,
                      "stuff": {
                           "id": "id2"
                      }
                    }
                    """
            .formatted(UUID_ID1);
    private final String DOC3 =
        """
                    {
                      "_id": "id3",
                      "value": 3,
                      "stuff": {
                           "id": {"$objectId": "%s"}
                      }
                    }
                    """
            .formatted(OBJECTID_LEAF);
    private final String DOC4 =
        """
                    {
                      "_id": "id4",
                      "value": 4,
                      "stuff": {
                           "id": {"$uuid": "%s"}
                      }
                    }
                    """
            .formatted(UUID_LEAF);

    @Test
    @Order(1)
    public void setUp() {
      insertDoc(DOC1);
      insertDoc(DOC2);
      insertDoc(DOC3);
      insertDoc(DOC4);
    }

    @Test
    @Order(2)
    public void inConditionForObjectIdId() {
      final String request =
          """
            {
              "findOne": {
                "filter" : {"_id" : {"$in": [
                  {"$objectId": "%s"},
                  {"$uuid": "%s"}
                ]}}
              }
            }
            """
              .formatted(OBJECTID_ID1, UUID_X);

      // We should only match one of ids so ordering won't matter
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(request)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.document", jsonEquals(DOC1));
    }

    @Test
    @Order(3)
    public void inConditionForUUIDId() {
      final String request =
          """
            {
              "findOne": {
                "filter" : {"_id" : {"$in": [
                  {"$objectId": "%s"},
                  {"$uuid": "%s"}
                ]}}
              }
            }
            """
              .formatted(OBJECTID_X, UUID_ID1);

      // We should only match one of ids so ordering won't matter
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(request)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.document", jsonEquals(DOC2));
    }

    @Test
    @Order(4)
    public void inConditionForObjectIdField() {
      final String request =
          """
            {
              "findOne": {
                "filter" : {"stuff.id" : {"$in": [
                  {"$objectId": "%s"},
                  {"$objectId": "%s"}
                ]}}
              }
            }
            """
              .formatted(OBJECTID_ID1, OBJECTID_LEAF);

      // We should only match one of ids so ordering won't matter
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(request)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.document", jsonEquals(DOC3));
    }

    @Test
    @Order(5)
    public void inConditionForUUIDField() {
      final String request =
          """
            {
              "findOne": {
                "filter" : {"stuff.id" : {"$in": [
                  {"$uuid": "%s"},
                  {"$uuid": "%s"}
                ]}}
              }
            }
            """
              .formatted(UUID_LEAF, UUID_X);

      // We should only match one of ids so ordering won't matter
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(request)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(not(nullValue())))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.document", jsonEquals(DOC4));
    }
  }

  @Nested
  @Order(3)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindOneFail {
    @Test
    public void failForMissingCollection() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body("{ \"findOne\": { \"filter\" : {\"_id\": \"doc1\"}}}")
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, "no_such_collection")
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("COLLECTION_NOT_EXIST"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              is("Collection does not exist, collection name: no_such_collection"));
    }

    @Test
    public void failForInvalidCollectionName() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body("{ \"findOne\": { \"filter\" : {\"_id\": \"doc1\"}}}")
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, "table,rate=100")
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              startsWith(
                  "Request invalid: field 'collection' value \"table,rate=100\" not valid. Problem:"));
    }

    @Test
    public void failForInvalidJsonExtension() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body("{ \"findOne\": { \"filter\" : {\"_id\": {\"$guid\": \"doc1\"}}}}")
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("UNSUPPORTED_FILTER_OPERATION"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].message", is("Unsupported filter operator: $guid"));
    }

    @Test
    public void failForInvalidUUIDAsId() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body("{ \"findOne\": { \"filter\" : {\"_id\": {\"$uuid\": \"not-an-uuid\"}}}}")
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("SHRED_BAD_DOCID_TYPE"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              containsString(
                  "Bad JSON Extension value: '$uuid' value has to be 36-character UUID String, instead got (\"not-an-uuid\")"));
    }

    @Test
    public void failForInvalidObjectIdAsId() {
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body("{ \"findOne\": { \"filter\" : {\"_id\": {\"$objectId\": \"bogus\"}}}}")
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("SHRED_BAD_DOCID_TYPE"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              containsString(
                  "Bad JSON Extension value: '$objectId' value has to be 24-digit hexadecimal ObjectId, instead got (\"bogus\")"));
    }
  }

  @Nested
  @Order(99)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindOneIntegrationTest.super.checkMetrics("FindOneCommand");
      FindOneIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
