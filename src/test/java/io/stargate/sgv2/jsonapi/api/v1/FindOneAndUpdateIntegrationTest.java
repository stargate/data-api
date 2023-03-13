package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
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
    public void findByIdAndSet() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc3",
                "username": "user3",
                "active_user" : true
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
          .statusCode(200);

      json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "doc3"},
              "update" : {"$set" : {"active_user": false}}
            }
          }
          """;
      String expected =
          """
          {
            "_id": "doc3",
            "username": "user3",
            "active_user": true
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
      expected =
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
    public void findByIdReturnDocumentAfter() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "afterDoc3",
                "username": "afterUser3",
                "active_user" : true
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
          .statusCode(200);

      json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"_id" : "afterDoc3"},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"returnDocument" : "after"}
            }
          }
          """;
      String expected =
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
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      json =
          """
          {
            "find": {
              "filter" : {"_id" : "afterDoc3"}
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
    public void findByIdUpsert() {
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
    public void findByColumnAndSet() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc4",
                "username": "user4"
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
          .statusCode(200);

      json =
          """
          {
            "findOneAndUpdate": {
              "filter" : {"username" : "user4"},
              "update" : {"$set" : {"new_col": "new_val"}}
            }
          }
          """;
      String expected =
          """
          {
            "_id":"doc4",
            "username":"user4"
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
      expected =
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
    public void findByIdAndUnset() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc5",
                "username": "user5",
                "unset_col": "val"
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
          .statusCode(200);

      json =
          """
          {
             "findOneAndUpdate": {
               "filter" : {"_id" : "doc5"},
               "update" : {"$unset" : {"unset_col": ""}}
             }
          }
          """;
      String expected =
          """
          {
            "_id":"doc5",
            "username":"user5",
            "unset_col":"val"
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

      expected =
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
  }

  @Nested
  class FindOneAndUpdateFailures {

    @Test
    public void findByIdTryUnsetId() {
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
    public void findByIdTrySetId() {
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
    public void findByIdTrySetPropertyOnArray() {
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
    public void findByIdTryPopNonArray() {
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
    public void findByIdTryIncNonNumber() {
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
    public void findByIdAndUnsetNested() {
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
    public void findByIdAndSetNested() {
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
    public void findByIdUpsertAndAddOnInsert() {
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

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }

  private void insertDoc(String docJson) {
    String doc =
        """
        {
          "insertOne": {
            "document": %s
          }
        }
        """
            .formatted(docJson);

    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(doc)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
        .then()
        // Sanity check: let's look for non-empty inserted id
        .body("status.insertedIds[0]", not(emptyString()))
        .statusCode(200);
  }
}
