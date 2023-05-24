package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class UpdateOneIntegrationTest extends AbstractCollectionIntegrationTestBase {

  @Nested
  class UpdateOneWithSet {
    @Test
    public void byIdAndSet() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "update_doc1",
                "username": "update_user3",
                "date_col": {"$date" : 1672531200000},
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200);

      json =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc1"},
              "update" : {"$set" : {"active_user": false}}
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
          {
            "_id":"update_doc1",
            "username":"update_user3",
            "date_col": {"$date" : 1672531200000},
            "active_user":false
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc1"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void emptyOptionsAllowed() {
      String json =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc1"},
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));
    }

    @Test
    public void byIdUpsert() {
      String json =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "afterDoc6"},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"upsert" : true}
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
          .body("status.upsertedId", is("afterDoc6"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "afterDoc6"}
            }
          }
          """;
      String expected =
          """
          {
            "_id":"afterDoc6",
            "active_user":false
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void byIdUpsertSetOnInsert() {
      String json =
          """
              {
                "updateOne": {
                  "filter" : {"_id" : "no-such-doc"},
                  "update" : {
                    "$set" : {"active_user": true},
                    "$setOnInsert" : {"_id": "upsertSetOnInsert1"}
                  },
                  "options" : {"upsert" : true}
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
          .body("status.upsertedId", is("upsertSetOnInsert1"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
              {
                "find": {
                  "filter" : {"_id" : "upsertSetOnInsert1"}
                }
              }
              """;
      String expected =
          """
              {
                "_id": "upsertSetOnInsert1",
                "active_user": true
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void byColumnUpsert() {
      String json =
          """
          {
            "updateOne": {
              "filter" : {"location" : "my_city"},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"upsert" : true}
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", is(notNullValue()));
    }

    @Test
    public void byIdAndColumnUpsert() {
      String json =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "afterDoc7", "username" : "afterName7", "phone" : null},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"upsert" : true}
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
          .body("status.upsertedId", is("afterDoc7"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "afterDoc7"}
            }
          }
          """;
      String expected =
          """
          {
            "_id":"afterDoc7",
            "username" : "afterName7",
            "phone" : null,
            "active_user":false
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void byColumnAndSet() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "update_doc2",
                "username": "update_user2"
              }
            }
          }
          """;
      String jsonOther =
          """
          {
            "insertOne": {
              "document": {
                "_id": "update_doc3",
                "username": "update_user2"
              }
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
          .statusCode(200);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(jsonOther)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200);

      json =
          """
          {
            "updateOne": {
              "filter" : {"username" : "update_user2"},
              "update" : {"$set" : {"new_col": "new_val"}}
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("status.moreData", is(nullValue()))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
          {
            "_id":"update_doc2",
            "username":"update_user2",
            "new_col": "new_val"
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc2"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void byColumnWithSortAndSet() {
      String json =
          """
        {
          "insertOne": {
            "document": {
              "_id": "update_doc2",
              "username": "update_user2",
              "location": "my_city"
            }
          }
        }
        """;
      String jsonOther =
          """
        {
          "insertOne": {
            "document": {
              "_id": "update_doc3",
              "username": "update_user3",
              "location": "my_city"
            }
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
          .statusCode(200);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(jsonOther)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200);

      json =
          """
        {
          "updateOne": {
            "filter" : {"location": "my_city"},
            "update" : {"$set" : {"new_col": "new_val"}},
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("status.moreData", is(nullValue()))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
        {
          "_id":"update_doc2",
          "username":"update_user2",
              "location": "my_city",
          "new_col": "new_val"
        }
        """;
      json =
          """
        {
          "find": {
            "filter" : {"_id" : "update_doc2"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void byColumnAndSetArray() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "update_doc4",
                "username": "update_user4"
              }
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
          .statusCode(200);

      json =
          """
          {
            "updateOne": {
              "filter" : {"username" : "update_user4"},
              "update" : {"$set" : {"new_col": ["new_val", "new_val2"]}}
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      String expected =
          """
          {
            "_id":"update_doc4",
            "username":"update_user4",
            "new_col": ["new_val", "new_val2"]
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc4"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void byColumnAndSetSubDoc() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "update_doc5",
                "username": "update_user5"
              }
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
          .statusCode(200);

      json =
          """
          {
            "updateOne": {
              "filter" : {"username" : "update_user5"},
              "update" : {"$set" : {"new_col": {"sub_doc_col" : "new_val2"}}}
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
          {
            "_id":"update_doc5",
            "username":"update_user5",
            "new_col": {"sub_doc_col":"new_val2"}
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc5"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }
  }

  @Nested
  class UpdateOneWithUnset {
    @Test
    public void byIdAndUnset() {
      String document =
          """
              {
                "_id": "update_doc3",
                "username": "update_user3",
                "unset_col": "val"
              }
              """;
      insertDoc(document);

      String json =
          """
              {
                "findOneAndUpdate": {
                  "filter" : {"_id" : "update_doc3"},
                  "update" : {"$unset" : {"unset_col": ""}}
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
              {
                "_id":"update_doc3",
                "username":"update_user3"
              }
              """;
      json =
          """
              {
                "find": {
                  "filter" : {"_id" : "update_doc3"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }
  }

  @Nested
  class UpdateOneWithPop {

    @Test
    public void byColumnAndPop() {
      String document =
          """
          {
            "_id": "update_doc_pop",
            "array1": [ 1, 2, 3 ],
            "array2": [ 4, 5, 6 ],
            "subdoc" : {
              "array" : [ 0, 1 ]
            },
            "array3": [ ]
          }
          """;
      insertDoc(document);

      // Let's test 6 pop operations, resulting in 3 changes
      String updateBody =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_pop"},
              "update" : {
                "$pop" : {
                  "array1": 1,
                  "array2": -1,
                  "array3": 1,
                  "array4": -1,
                  "subdoc.array" : 1,
                  "subdoc.x.y" : 1
                  }
              }
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateBody)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String findJson =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_pop"}
            }
          }
          """;
      String expected =
          """
          {
            "_id": "update_doc_pop",
            "array1": [ 1, 2 ],
            "array2": [ 5, 6 ],
            "subdoc" : {
              "array" : [ 0 ]
            },
            "array3": [ ]
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
    }
  }

  @Nested
  class UpdateOneWithPush {

    @Test
    public void byColumnAndPush() {
      String document =
          """
          {
            "_id": "update_doc_push",
            "array": [ 2 ]
          }
          """;
      insertDoc(document);

      String json =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_push"},
              "update" : {"$push" : {"array": 13, "subdoc.array": true }}
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
          {
            "_id":"update_doc_push",
            "array": [2, 13],
            "subdoc": { "array" : [ true ] }
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_push"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void byColumnAndPushWithEach() {
      String document =
          """
          {
            "_id": "update_doc_push_each",
            "nested" : { "array": [ 1 ] }
          }
          """;
      insertDoc(document);

      String json =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_push_each"},
              "update" : {
                 "$push" : {
                   "nested.array": { "$each" : [ 2, 3 ] },
                   "newArray": { "$each" : [ true ] }
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
          {
            "_id":"update_doc_push_each",
            "nested" : { "array": [1, 2, 3] },
            "newArray": [true]
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_push_each"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void byColumnAndPushWithEachAndPosition() {
      String document =
          """
          {
            "_id": "update_doc_push_each_position",
            "array": [ 1, 2, 3 ]
          }
          """;
      insertDoc(document);

      String json =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_push_each_position"},
              "update" : {
                 "$push" : {
                   "array": { "$each" : [ 4, 5 ], "$position" : 2 },
                   "nested.values": { "$each" : [ 1, 2, 3 ], "$position" : -999 }
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
          { "_id":"update_doc_push_each_position",
            "array": [1, 2, 4, 5, 3],
            "nested": {
              "values" : [1, 2, 3]
            }
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_push_each_position"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }
  }

  @Nested
  class UpdateOneWithInc {

    @Test
    public void byColumnAndInc() {
      String document =
          """
          {
             "_id": "update_doc_inc",
             "number": 123,
             "numbers": {
                "values": [ 1 ]
              }
           }
           """;
      insertDoc(document);

      String updateJson =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_inc"},
              "update" : {
                "$inc" : {
                  "number": -4,
                  "newProp" : 0.25,
                  "numbers.values.0" : 9,
                  "numbers.values.1" : 0.5
                }
              }
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expectedDoc =
          """
          {
            "_id":"update_doc_inc",
            "number": 119,
            "newProp": 0.25,
            "numbers": {
              "values" : [ 10, 0.5 ]
            }
          }
          """;
      String findJson =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_inc"}
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedDoc));
    }
  }

  @Nested
  class UpdateOneWithMul {
    @Test
    public void byColumnAndMultiply() {
      String document =
          """
          {
            "_id": "update_doc_mul",
            "number": 12,
            "numbers": {
              "values": [ 2 ]
            }
          }
          """;
      insertDoc(document);

      String updateJson =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_mul"},
              "update" : {
                "$mul" : {
                  "number": -4,
                  "newProp" : 0.25,
                  "numbers.values.0" : 0.25,
                  "numbers.values.1" : 5
                }
               }
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      String expectedDoc =
          """
          {
            "_id":"update_doc_mul",
            "number": -48,
            "newProp": 0,
            "numbers": {
              "values" : [ 0.5, 0 ]
            }
          }
          """;
      String findJson =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_mul"}
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedDoc));
    }
  }

  @Nested
  class UpdateOneWithAddToSet {

    @Test
    public void byColumnAndAddToSet() {
      String document =
          """
          {
            "_id": "update_doc_add_to_set",
            "array": [ 2 ]
          }
          """;
      insertDoc(document);

      String json =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_add_to_set"},
              "update" : {"$addToSet" : {"array": 3, "subdoc.array": "value" }}
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
          {
            "_id":"update_doc_add_to_set",
            "array": [2, 3],
            "subdoc" : { "array" : [ "value" ] }
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_add_to_set"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    // Test for case where nothing is actually added
    @Test
    public void byColumnAndAddToSetNoChange() {
      final String originalDoc =
          """
          {
            "_id": "update_doc_add_to_set_unchanged",
            "array": [ 0, 1, 2 ]
          }
          """;
      insertDoc(originalDoc);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
              {
                "updateOne": {
                  "filter" : {"_id" : "update_doc_add_to_set_unchanged"},
                  "update" : {"$addToSet" : {"array": 2 }}
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
              {
                "find": {
                  "filter" : {"_id" : "update_doc_add_to_set_unchanged"}
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(originalDoc));
    }

    @Test
    public void byColumnAndAddToSetWithEach() {
      String document =
          """
          {
            "_id": "update_doc_add_to_set_each",
            "nested" : { "array": [ 2, 3 ] }
          }
          """;
      insertDoc(document);

      String json =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_add_to_set_each"},
              "update" : {
                 "$addToSet" : {
                   "nested.array": { "$each" : [ 1, 3, 4 ] },
                   "newArray": { "$each" : [ true, false ] }
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
          {
            "_id":"update_doc_add_to_set_each",
            "nested" : { "array": [2, 3, 1, 4] },
            "newArray": [true, false]
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_add_to_set_each"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }
  }

  @Nested
  class UpdateOneWithMin {

    @Test
    public void byColumnAndMin() {
      String document =
          """
          {
            "_id": "update_doc_min",
            "min": 1,
            "max": 99,
            "numbers": {
              "values": [ 1 ]
            }
          }
          """;
      insertDoc(document);

      String updateJson =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_min"},
              "update" : {
                "$min" : {
                  "min": 2,
                  "max" : 25,
                  "numbers.values" : [ -9 ]
                }
              }
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expectedDoc =
          """
          {
             "_id": "update_doc_min",
             "min": 1,
             "max": 25,
             "numbers": {
                "values": [ -9 ]
              }
           }
           """;
      String findJson =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_min"}
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedDoc));
    }

    @Test
    public void byColumnMinNonNumeric() {
      insertDoc(
          """
              {
                "_id": "update_doc_min_text",
                "start": "abc",
                "end": "xyz"
              }
              """);
      String updateJson =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_min_text"},
              "update" : {
                "$min" : {
                  "start": "fff",
                  "end" : "fff"
                }
              }
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update: only "end" changed
      String expectedDoc =
          """
          {
            "_id": "update_doc_min_text",
            "start": "abc",
            "end": "fff"
          }
          """;
      String findJson =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_min_text"}
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedDoc));
    }

    @Test
    public void byColumnMinMixedTypes() {
      insertDoc(
          """
              {
                "_id": "update_doc_min_mixed",
                "start": "abc",
                "end": "xyz"
              }
              """);
      String updateJson =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_min_mixed"},
              "update" : {
                "$min" : {
                  "start": 123,
                  "end" : true
                }
              }
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update: only "start" changed (numbers before strings), not
      // "end" (boolean after strings)
      String expectedDoc =
          """
          {
            "_id": "update_doc_min_mixed",
            "start": 123,
            "end": "xyz"
          }
          """;
      String findJson =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_min_mixed"}
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedDoc));
    }
  }

  @Nested
  class UpdateOneWithMax {

    @Test
    public void byColumnAndMax() {
      String document =
          """
          {
            "_id": "update_doc_max",
            "min": 1,
            "max": 99,
            "numbers": {
              "values": { "x":1, "y":2 }
            }
          }
          """;
      insertDoc(document);

      String updateJson =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_max"},
              "update" : {
                "$max" : {
                  "min": 2,
                  "max" : 25,
                  "numbers.values": { "x":1, "y":3 }
                }
              }
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expectedDoc =
          """
          {
            "_id": "update_doc_max",
            "min": 2,
            "max": 99,
            "numbers": {
              "values": { "x":1, "y":3 }
            }
          }
          """;
      String findJson =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_max"}
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedDoc));
    }

    @Test
    public void byColumnMaxNonNumeric() {
      insertDoc(
          """
              {
                 "_id": "update_doc_max_text",
                 "start": "abc",
                 "end": "xyz"
               }
               """);
      String updateJson =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_max_text"},
              "update" : {
                "$max" : {
                  "start": "fff",
                  "end" : "fff"
                }
              }
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update: only "start" changed
      String expectedDoc =
          """
          {
            "_id": "update_doc_max_text",
            "start": "fff",
            "end": "xyz"
          }
          """;
      String findJson =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_max_text"}
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedDoc));
    }

    @Test
    public void byColumnMaxMixedTypes() {
      insertDoc(
          """
              {
                "_id": "update_doc_max_mixed",
                "start": "abc",
                "end": "xyz"
               }
               """);
      String updateJson =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_max_mixed"},
              "update" : {
                "$max" : {
                  "start": 123,
                  "end" : true
                }
              }
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update: only "end" changed (booleans after Strings), not
      // "start" (numbers before Strings)
      String expectedDoc =
          """
          {
            "_id": "update_doc_max_mixed",
            "start": "abc",
            "end": true
          }
          """;
      String findJson =
          """
          {
            "find": {
              "filter" : {"_id" : "update_doc_max_mixed"}
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedDoc));
    }
  }

  @Nested
  class UpdateOneWithRename {

    @Test
    public void byColumnAndRename() {
      String document =
          """
          {
             "_id": "update_doc_rename",
             "total": 1,
             "nested": {
                "x": true
              }
           }
           """;
      insertDoc(document);

      // 4 things to try to rename (2 root, 2 nested) of which only 2 exist
      String updateJson =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_rename"},
              "update" : {
                "$rename" : {
                  "total": "sum",
                  "x" : "y",
                  "nested.x" : "nested.x0",
                  "nested.z" : "nested.z2"
                }
              }
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expectedDoc =
          """
          {
            "_id": "update_doc_rename",
            "sum": 1,
            "nested": {
              "x0": true
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
                  "filter" : {"_id" : "update_doc_rename"}
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedDoc));
    }
  }

  // Tests combining more than update operator, mostly for cross-validation
  @Nested
  class UpdateOneMultipleOperationTypes {
    @Test
    public void byColumnUseSetAndUnset() {
      insertDoc(
          """
                  {
                     "_id": "update_doc_mixed_set_unset",
                     "nested": {
                        "old": "a"
                     }
                   }
                   """);
      String updateJson =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_mixed_set_unset"},
              "update" : {
                "$set" : {
                  "nested.new": "b"
                },
                "$unset" : {
                  "nested.old": 1
                }
              }
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update: only "end" changed (booleans after Strings), not
      // "start" (numbers before Strings)
      String expectedDoc =
          """
          {
            "_id": "update_doc_mixed_set_unset",
            "nested": {
              "new": "b"
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
                  "filter" : {"_id": "update_doc_mixed_set_unset"}
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedDoc));
    }
  }

  @Nested
  class Concurrency {

    @RepeatedTest(10)
    public void concurrentUpdates() throws Exception {
      String document =
          """
          {
            "_id": "concurrent",
            "count": 0
          }
          """;
      insertDoc(document);

      // three threads ensures no retries exhausted
      int threads = 3;
      CountDownLatch latch = new CountDownLatch(threads);

      String updateJson =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "concurrent"},
              "update" : {
                "$inc" : {"count": 1}
              }
            }
          }
          """;
      // start all threads
      AtomicReferenceArray<Exception> exceptions = new AtomicReferenceArray<>(threads);
      for (int i = 0; i < threads; i++) {
        int index = i;
        new Thread(
                () -> {
                  try {
                    given()
                        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
                        .contentType(ContentType.JSON)
                        .body(updateJson)
                        .when()
                        .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
                        .then()
                        .statusCode(200)
                        .body("status.matchedCount", is(1))
                        .body("status.modifiedCount", is(1))
                        .body("errors", is(nullValue()));
                  } catch (Exception e) {

                    // set exception so we can rethrow
                    exceptions.set(index, e);
                  } finally {

                    // count down
                    latch.countDown();
                  }
                })
            .start();
      }

      latch.await();

      // check if there are any exceptions
      // throw first that is seen
      for (int i = 0; i < threads; i++) {
        Exception exception = exceptions.get(i);
        if (null != exception) {
          throw exception;
        }
      }

      // assert state after all updates
      String expectedDoc =
          """
          {
            "_id": "concurrent",
            "count": 3
          }
          """;
      String findJson =
          """
          {
            "find": {
              "filter" : {"_id" : "concurrent"}
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedDoc));
    }
  }

  @Nested
  class ClientErrors {
    @Test
    public void invalidCommand() {
      String updateJson =
          """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_max"}
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors[0].exceptionClass", is("ConstraintViolationException"))
          .body(
              "errors[0].message",
              is(
                  "Request invalid, the field postCommand.command.updateClause not valid: must not be null."));
    }

    @Test
    public void invalidSetAndUnsetPathConflict() {
      // Cannot modify entries that conflict (same path, or parent/child):
      String updateJson =
          """
              {
                "updateOne": {
                  "filter" : {"_id" : "update_doc_whatever"},
                  "update" : {
                    "$set" : {"root.array.1": 13},
                    "$unset" : {"root.array": 1}
                  }
                }
              }
              """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_OPERATION_PARAM"))
          .body(
              "errors[0].message",
              is(
                  "Update operator path conflict due to overlap: 'root.array' ($unset) vs 'root.array.1' ($set)"));
    }
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }

  @AfterAll
  public void checkMetrics() {
    checkMetrics("UpdateOneCommand");
  }
}
