package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class FindOneIntegrationTest extends CollectionResourceBaseIntegrationTest {

  @Nested
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

    private static final String DOC6_JSON =
        """
            {
              "_id": "doc6",
              "username": "user6",
              "active_user" : true
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
      insertDoc(DOC5_JSON);
    }

    @Test
    @Order(-1) // executed before insert
    public void findOneNoFilterNoDocuments() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(0))
          .body("data.docs", is(empty()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneNoFilter() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs", hasSize(1))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneNoFilterSortAscending() {
      String json =
          """
              {
                "findOne": {
                  "sort" : ["username"]
                }
              }
              """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs", hasSize(1))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.docs[0]", jsonEquals(DOC4_JSON)); // missing value is the lowest precedence
    }

    @Test
    public void findOneNoFilterSortDescending() {
      String json =
          """
              {
                "findOne": {
                  "sort" : ["-username"]
                }
              }
              """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs", hasSize(1))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.docs[0]", jsonEquals(DOC5_JSON)); // missing value is the lowest precedence
    }

    @Test
    public void findOneById() {
      String json =
          """
          {
            "findOne": {
              "filter" : {"_id" : "doc1"}
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs", hasSize(1))
          .body("data.docs[0]", jsonEquals(DOC1_JSON))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneByIdNotFound() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(0))
          .body("data.docs", is(empty()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneByColumn() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs", hasSize(1))
          .body("data.docs[0]", jsonEquals(DOC1_JSON))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneByColumnMissing() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(0))
          .body("data.docs", is(empty()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneByColumnNotMatching() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(0))
          .body("data.docs", is(empty()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneWithExistsOperator() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs", hasSize(1))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneWithExistsOperatorSort() {
      String json =
          """
        {
          "findOne": {
            "filter" : {"active_user" : {"$exists" : true}},
            "sort" : ["username"]

          }
        }
        """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs", hasSize(1))
          .body("data.docs[0]", jsonEquals(DOC1_JSON))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneWithExistsOperatorSortDescending() {
      String json =
          """
              {
                "findOne": {
                  "filter" : {"active_user" : {"$exists" : true}},
                  "sort" : ["-username"]

                }
              }
              """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs", hasSize(1))
          .body("data.docs[0]", jsonEquals(DOC6_JSON))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneWithExistsOperatorFalse() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors[0].message", is("$exists is supported only with true option"))
          .body("errors[0].errorCode", is("UNSUPPORTED_FILTER_DATA_TYPE"));
    }

    @Test
    public void findOneWithExistsNotMatching() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(0))
          .body("data.docs", is(empty()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneWithAllOperatorMissing() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(0))
          .body("data.docs", is(empty()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneWithAllOperatorNotMatching() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(0))
          .body("data.docs", is(empty()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneWithAllOperatorNotArray() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body(
              "errors[0].message",
              is("Filter type not supported, unable to resolve to a filtering strategy"))
          .body("errors[0].errorCode", is("FILTER_UNRESOLVABLE"));
    }

    @Test
    public void findOneWithSizeOperator() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs", hasSize(1))
          .body("data.docs[0]", jsonEquals(DOC3_JSON))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneWithSizeOperatorNotMatching() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(0))
          .body("data.docs", is(empty()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void findOneWithSizeOperatorNotNumber() {
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body(
              "errors[0].message",
              is("Filter type not supported, unable to resolve to a filtering strategy"))
          .body("errors[0].errorCode", is("FILTER_UNRESOLVABLE"));
    }
  }
}
