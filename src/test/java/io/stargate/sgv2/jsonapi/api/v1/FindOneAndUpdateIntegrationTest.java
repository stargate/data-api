package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class FindOneAndUpdateIntegrationTest extends CollectionResourceBaseIntegrationTest {

  @Nested
  class FindOneAndUpdate {

    @Test
    public void byIdAndSet() {
      String document =
          """
          {
            "_id": "doc3",
            "username": "user3",
            "active_user" : true
          }
          """;
      insertDoc(document);

      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "doc3"},
              "update" : {"$set" : {"active_user": false}},
              "options": {"upsert": true}
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
          .body("data.docs[0]", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
          {
            "_id": "doc3",
            "username": "user3",
            "active_user": false
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc3"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void byIdAndSetNotFound() {
      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "doc3"},
              "update" : {"$set" : {"active_user": false}}
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
          .body("data.docs", is(empty()))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));
    }

    @Test
    public void emptyOptionsAllowed() {
      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "doc3"},
              "update" : {"$set" : {"active_user": false}},
              "options": {}
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
          .body("data.docs", is(empty()))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));
    }

    @Test
    public void byIdReturnDocumentAfter() {
      insertDoc(
          """
          {
            "_id": "afterDoc3",
            "username": "afterUser3",
            "active_user" : true
          }
          """);
      final String expected =
          """
          {
            "_id":"afterDoc3",
            "username":"afterUser3",
            "active_user":false
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                {
                  "findOneAndUpdate": {
                    "filter" : {"_id" : "afterDoc3"},
                    "update" : {"$set" : {"active_user": false}},
                    "options" : {"returnDocument" : "after"}
                  }
                }
          """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                {
                  "find": {
                    "filter" : {"_id" : "afterDoc3"}
                  }
                }
          """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void byIdReturnDocumentBefore() {
      final String docBefore =
          """
          {
            "_id": "beforeDoc3",
            "username": "beforeUser3",
            "active_user": true
          }
          """;
      insertDoc(docBefore);
      final String docAfter =
          """
              {
                "_id":"beforeDoc3",
                "username":"beforeUser3",
                "active_user":false,
                "hits": 1
              }
              """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                {
                  "findOneAndUpdate": {
                    "filter" : {"_id" : "beforeDoc3"},
                    "update" : {
                      "$set" : {"active_user": false},
                      "$inc" : {"hits": 1}
                    },
                    "options" : {"returnDocument" : "before"}
                  }
                }
          """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(docBefore))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                {
                  "find": {
                    "filter" : {"_id" : "beforeDoc3"}
                  }
                }
          """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.docs[0]", jsonEquals(docAfter));
    }

    @Test
    public void byColumnUpsert() {
      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"location" : "my_city"},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"returnDocument" : "after", "upsert" : true}
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
          .body("data.docs[0]", is(notNullValue()))
          .body("status.upsertedId", is(notNullValue()))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
          {
            "find": {
              "filter" : {"location" : "my_city"}
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
          .body("data.docs[0]", is(notNullValue()));
    }

    @Test
    public void byIdUpsert() {
      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "afterDoc4"},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"returnDocument" : "after", "upsert" : true}
            }
          }
          """;
      String expected =
          """
          {
            "_id":"afterDoc4",
             "active_user":false
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
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.upsertedId", is("afterDoc4"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "afterDoc4"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void byColumnAndSet() {
      String document =
          """
          {
            "_id": "doc4",
            "username": "user4"
          }
          """;
      insertDoc(document);

      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"username" : "user4"},
              "update" : {"$set" : {"new_col": "new_val"}}
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
          .body("data.docs[0]", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
          {
            "_id":"doc4",
            "username":"user4",
            "new_col": "new_val"
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc4"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void byIdAndUnset() {
      String document =
          """
          {
            "_id": "doc5",
            "username": "user5",
            "unset_col": "val"
          }
          """;
      insertDoc(document);

      String json =
          """
          {
             "findOneAndUpdate": {
               "filter" : {"_id" : "doc5"},
               "update" : {"$unset" : {"unset_col": ""}}
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
          .body("data.docs[0]", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      String expected =
          """
          {
            "_id":"doc5",
            "username":"user5"
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc5"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void withSortReturnDocumentAfter() {
      String document1 =
          """
          {
            "_id": "sortDoc1",
            "username": "sortUser1",
            "active_user" : true,
            "filter_me" : "happy"
          }
          """;
      insertDoc(document1);

      String document2 =
          """
          {
            "_id": "sortDoc2",
            "username": "sortUser2",
            "active_user" : false,
            "filter_me" : "happy"
          }
          """;
      insertDoc(document2);

      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"filter_me" : "happy"},
              "sort" :  {"active_user" : 1},
              "update" : {"$set" : {"add_me": false}},
              "options" : {"returnDocument" : "after"}
            }
          }
          """;
      String expected =
          """
          {
            "_id": "sortDoc2",
            "username": "sortUser2",
            "active_user" : false,
            "filter_me" : "happy",
            "add_me" : false
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
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
        {
          "find": {
            "filter" : {"_id" : "sortDoc2"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void withSortDescendingReturnDocumentAfter() {
      String document1 =
          """
          {
            "_id": "sortDoc1",
            "username": "sortUser1",
            "active_user" : true,
            "filter_me" : "happy"
          }
          """;
      insertDoc(document1);

      String document2 =
          """
          {
            "_id": "sortDoc2",
            "username": "sortUser2",
            "active_user" : false,
            "filter_me" : "happy"
          }
          """;
      insertDoc(document2);

      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"filter_me" : "happy"},
              "sort" :  {"active_user" : -1},
              "update" : {"$set" : {"add_me": false}},
              "options" : {"returnDocument" : "after"}
            }
          }
          """;
      String expected =
          """
          {
            "_id": "sortDoc1",
            "username": "sortUser1",
            "active_user" : true,
            "filter_me" : "happy",
            "add_me" : false
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
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "sortDoc1"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }
  }

  @Nested
  class FindOneAndUpdateFailures {

    @Test
    public void byIdTryUnsetId() {
      final String inputDoc =
          """
          {
            "_id": "update_doc_unset_id",
            "username": "update_user"
          }
          """;
      insertDoc(inputDoc);

      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "update_doc_unset_id"},
              "update" : {"$unset" : {"_id": 1}}
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
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_FOR_DOC_ID"))
          .body("errors[0].message", is("Cannot use operator with '_id' field: $unset"));

      // And finally verify also that nothing was changed:
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_unset_id"}
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
          .body("data.docs[0]", jsonEquals(inputDoc));
    }

    @Test
    public void byIdTrySetId() {
      final String inputDoc =
          """
          {
            "_id": "update_doc_set_id",
            "username": "update_user"
          }
          """;
      insertDoc(inputDoc);

      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "update_doc_set_id"},
              "update" : {"$set" : {"_id": "new-id"}}
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
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_FOR_DOC_ID"))
          .body("errors[0].message", is("Cannot use operator with '_id' field: $set"));

      // And finally verify also that nothing was changed:
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_set_id"}
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
          .body("data.docs[0]", jsonEquals(inputDoc));
    }

    @Test
    public void byIdTrySetPropertyOnArray() {
      final String inputDoc =
          """
          {
            "_id": "update_doc_set_prop_on_array",
            "subdoc": {
              "array": [ 1, 2, true ]
            }
          }
          """;
      insertDoc(inputDoc);
      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "update_doc_set_prop_on_array"},
              "update" : {"$set" : {"subdoc.array.name": "Bob"}}
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
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_OPERATION_PATH"))
          .body(
              "errors[0].message",
              is(
                  "Invalid update operation path: cannot create field ('name') in path 'subdoc.array.name'; only OBJECT nodes have properties (got ARRAY)"));

      // And finally verify also that nothing was changed:
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_set_prop_on_array"}
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
          .body("data.docs[0]", jsonEquals(inputDoc));
    }

    @Test
    public void byIdTryPopNonArray() {
      final String inputDoc =
          """
          {
            "_id": "update_doc_pop_non_array",
            "subdoc": {
               "value": 15
            }
          }
          """;
      insertDoc(inputDoc);

      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "update_doc_pop_non_array"},
              "update" : {"$pop" : {"subdoc.value": 1 }}
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
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_OPERATION_TARGET"))
          .body(
              "errors[0].message",
              is(
                  "Unsupported target JSON value for update operation: $pop requires target to be ARRAY; value at 'subdoc.value' of type NUMBER"));

      // And finally verify also that nothing was changed:
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_pop_non_array"}
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
          .body("data.docs[0]", jsonEquals(inputDoc));
    }

    @Test
    public void byIdTryIncNonNumber() {
      final String inputDoc =
          """
          {
            "_id": "update_doc_inc_non_number",
            "subdoc": {
               "value": "text"
            }
          }
          """;
      insertDoc(inputDoc);
      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "update_doc_inc_non_number"},
              "update" : {"$inc" : {"subdoc.value": -99 }}
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
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_OPERATION_TARGET"))
          .body(
              "errors[0].message",
              is(
                  "Unsupported target JSON value for update operation: $inc requires target to be Number; value at 'subdoc.value' of type STRING"));

      // And finally verify also that nothing was changed:
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_inc_non_number"}
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
          .body("data.docs[0]", jsonEquals(inputDoc));
    }
  }

  @Nested
  class FindOneAndUpdateNested {

    @Test
    public void byIdAndUnsetNested() {
      String document =
          """
          {
            "_id": "update_doc_unset_nested",
            "array": [
                137,
                { "x" : 1, "y" : 2 }
            ],
            "subdoc" : {
                "x" : 5,
                "y" : -19
            }
          }
          """;
      insertDoc(document);

      // NOTE: we mix actual working removals and ones that won't; it is not an error
      // to try to $unset properties that do not (or sometimes cannot) exist. They just
      // have no effect.
      //
      // Ones that do have effect are:
      //
      // * array.0   -> remove first entry, replace with null
      // * array.1.x -> remove property 'x' from second array element (object)
      // * subdoc.y  -> remove subdoc property 'y'
      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "update_doc_unset_nested"},
              "update" : {
                "$unset" : {
                  "array.0": 1,
                  "array.1.x" : 1,
                  "subdoc.x.property" : 1,
                  "subdoc.y" : 1,
                  "nosuchfield.to.remove" : 1
                }
              }
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
          {
            "_id": "update_doc_unset_nested",
            "array": [
               null,
               {"y" : 2}
            ],
            "subdoc": {
               "x" : 5
            }
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_unset_nested"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void byIdAndSetNested() {
      String document =
          """
          {
            "_id": "update_doc_set_nested",
            "array": [
               137,
               {
                 "y": 2,
                 "subarray": []
               }
            ],
            "subdoc" : {
               "x": 5
            }
          }
          """;
      insertDoc(document);

      String json =
          """
          {
             "findOneAndUpdate": {
               "filter" : {"_id" : "update_doc_set_nested"},
               "update" : {"$set" : {
                   "array.0": true,
                   "array.1.subarray.1" : -25,
                   "subdoc.x" : false,
                   "subdoc.y" : 1
                 }
               }
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
          {
            "_id": "update_doc_set_nested",
            "array": [
              true,
              {
                "y": 2,
                "subarray": [ null, -25 ]
              }
            ],
            "subdoc" : {
               "x": false,
               "y": 1
            }
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_set_nested"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }
  }

  @Nested
  class FindOneAndUpdateWithSetOnInsert {

    @Test
    public void byIdUpsertAndAddOnInsert() {
      String json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "setOnInsertDoc1"},
              "update" : {
                  "$set" : {"active_user": true},
                  "$setOnInsert" : {"new_user": true}
              },
              "options" : {"returnDocument" : "after", "upsert" : true}
            }
          }
          """;

      // On Insert (for Upsert) should apply both $set and $setOnInsert
      String expected =
          """
          {
            "_id":"setOnInsertDoc1",
            "new_user":true,
            "active_user":true
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
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.upsertedId", is("setOnInsertDoc1"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));

      // assert state on insert
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
              {
                "find": {
                  "filter" : {"_id" : "setOnInsertDoc1"}
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected));

      // However: with update for upsert, $setOnInsert not to be applied
      json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "setOnInsertDoc1"},
              "update" : {
                  "$set" : {"new_user": false},
                  "$setOnInsert" : {"x": 5}
              },
              "options" : {"returnDocument" : "after", "upsert" : true}
            }
          }
          """;
      expected =
          """
          {
            "_id":"setOnInsertDoc1",
            "new_user":false,
            "active_user":true
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
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("status.upsertedId", nullValue())
          .body("errors", is(nullValue()));

      // And validate to make sure nothing was actually modified
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "setOnInsertDoc1"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }
  }

  @Nested
  class FindOneAndUpdateWithProjection {
    @Test
    public void projectionAfterUpdate() {
      String document =
          """
              {
                "_id": "update_doc_projection_after",
                "x": 1,
                "y": 2,
                "z": 3,
                "subdoc": {
                    "a": 4,
                    "b": 5,
                    "c": 6
                }
              }
              """;
      insertDoc(document);

      String updateQuery =
          """
              {
                "findOneAndUpdate": {
                  "filter" : {"_id" : "update_doc_projection_after"},
                  "options" : {"returnDocument" : "after"},
                  "projection" : { "x":0, "subdoc.c":0 },
                  "update" : {
                    "$unset" : {
                      "subdoc.a": 1,
                      "z": 1
                    }
                  }
                }
              }
              """;
      // assert that returned document shows doc AFTER update WITH given projection
      String expectedFiltered =
          """
                  {
                    "_id": "update_doc_projection_after",
                    "y": 2,
                    "subdoc": {
                      "b": 5
                    }
                  }
                  """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateQuery)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()))
          .body("data.docs[0]", jsonEquals(expectedFiltered));

      // But also that update itself worked ($unset "z" and "subdoc.a")
      String expectedUpdated =
          """
              {
                "_id": "update_doc_projection_after",
                "x": 1,
                "y": 2,
                "subdoc": {
                    "b": 5,
                    "c": 6
                }
              }
                  """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                  {
                    "find": {
                      "filter" : {"_id" : "update_doc_projection_after"}
                    }
                  }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expectedUpdated));
    }

    @Test
    public void projectionBeforeUpdate() {
      String document =
          """
                  {
                    "_id": "update_doc_projection_before",
                    "a": 1,
                    "b": 2,
                    "c": 3,
                    "subdoc": {
                        "x": 4,
                        "y": 5,
                        "z": 6
                    }
                  }
                  """;
      insertDoc(document);

      String updateQuery =
          """
                  {
                    "findOneAndUpdate": {
                      "filter" : {"_id" : "update_doc_projection_before"},
                      "options" : {"returnDocument" : "before"},
                      "projection" : { "a":0, "subdoc.z":0 },
                      "update" : {
                        "$unset" : {
                          "subdoc.x": 1,
                          "c": 1
                        }
                      }
                    }
                  }
                  """;
      // assert state before update, with given projection (so unsets not visible)
      String expectedFiltered =
          """
                      {
                        "_id": "update_doc_projection_before",
                        "b": 2,
                        "c": 3,
                        "subdoc": {
                          "x": 4,
                          "y": 5
                        }
                      }
                      """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateQuery)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()))
          .body("data.docs[0]", jsonEquals(expectedFiltered));

      // And with updates $unset of c and subdoc.x, but no Projection
      String expectedUpdated =
          """
                      {
                        "_id": "update_doc_projection_before",
                        "a": 1,
                        "b": 2,
                        "subdoc": {
                            "y": 5,
                            "z": 6
                        }
                      }
                      """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                          {
                            "find": {
                              "filter" : {"_id" : "update_doc_projection_before"}
                            }
                          }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expectedUpdated));
    }
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }
}
