package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneAndUpdateIntegrationTest extends AbstractCollectionIntegrationTestBase {

  @Nested
  @Order(1)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(document))
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void byIdAndSetNoChange() {
      String document =
          """
        {
          "_id": "doc3",
          "username": "admin",
          "active_user" : true
        }
        """;
      insertDoc(document);

      String json =
          """
        {
          "findOneAndUpdate": {
            "filter" : {"_id" : "doc3"},
            "sort": { "username": 1 },
            "update" : {"$set" : {"username": "admin"}},
            "options": {"returnDocument": "before"}
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
          .body("data.document", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", is(nullValue()))
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", is(nullValue()))
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
          .headers(getHeaders())
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(expected))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      given()
          .headers(getHeaders())
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected));
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
          .headers(getHeaders())
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(docBefore))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      given()
          .headers(getHeaders())
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(docAfter));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(notNullValue()))
          .body("data.document._id", any(String.class))
          .body("status.upsertedId", is(notNullValue()))
          .body("status.upsertedId", any(String.class))
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", is(notNullValue()))
          .body("data.documents[0]._id", any(String.class));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(expected))
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
          .headers(getHeaders())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(document))
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(document))
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(expected))
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(expected))
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
          .headers(getHeaders())
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
  @Order(2)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_FOR_DOC_ID"))
          .body("errors[0].message", is("Cannot use operator with '_id' property: $unset"));

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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(inputDoc));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_FOR_DOC_ID"))
          .body("errors[0].message", is("Cannot use operator with '_id' property: $set"));

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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(inputDoc));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(inputDoc));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(inputDoc));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(inputDoc));
    }

    @Test
    public void tryUpdateWithTooLongNumber() {
      insertDoc(
          """
                      {
                        "_id": "update_doc_too_long_number",
                        "value": 123
                      }
                      """);

      // Max number length: 100; use 110
      String tooLongNumStr = "1234567890".repeat(11);
      String json =
              """
                      {
                        "findOneAndUpdate": {
                          "filter" : {"_id" : "update_doc_too_long_number"},
                          "update" : {"$set" : {"value": %s}}
                        }
                      }
                      """
              .formatted(tooLongNumStr);
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
          .body("errors[0].errorCode", is("SHRED_DOC_LIMIT_VIOLATION"))
          .body(
              "errors[0].message",
              startsWith("Document size limitation violated: Number value length"));
    }
  }

  @Nested
  @Order(3)
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
          .headers(getHeaders())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
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
          .headers(getHeaders())
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
          .headers(getHeaders())
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
  @Order(4)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(expected))
          .body("status.upsertedId", is("setOnInsertDoc1"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));

      // assert state on insert
      given()
          .headers(getHeaders())
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));

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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(expected))
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void useGivenDocIdOnInsert() {
      String json =
          """
                      {
                        "findOneAndUpdate": {
                          "filter" : {"_id" : "noSuchItem"},
                          "update" : {
                              "$set" : {
                                  "active_user": true,
                                  "extra": 13
                              },
                              "$setOnInsert" : {
                                  "_id": "setOnInsertDoc2",
                                  "new_user": true
                              }
                          },
                          "options" : {"returnDocument" : "after", "upsert" : true}
                        }
                      }
                      """;

      // On Insert (for Upsert) should apply both $set and $setOnInsert
      String expected =
          """
                      {
                        "_id": "setOnInsertDoc2",
                        "active_user": true,
                        "new_user": true,
                        "extra": 13
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
          .body("data.document", jsonEquals(expected))
          .body("status.upsertedId", is("setOnInsertDoc2"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));

      // And verify that the document was inserted as expected:
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                              {
                                "find": {
                                  "filter" : {"_id" : "setOnInsertDoc2"}
                                }
                              }
                              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
    }
  }

  @Nested
  @Order(5)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(updateQuery)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()))
          .body("data.document", jsonEquals(expectedFiltered));

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
          .headers(getHeaders())
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedUpdated));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(updateQuery)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()))
          .body("data.document", jsonEquals(expectedFiltered));

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
          .headers(getHeaders())
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedUpdated));
    }
  }

  @Nested
  @Order(6)
  class FindOneAndUpdateWithDate {
    @Test
    public void setWithDateField() {
      final String document =
          """
                {
                  "_id": "doc1",
                  "username": "user1"
                }
                """;
      insertDoc(document);
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                {
                  "findOneAndUpdate": {
                    "filter" : {"_id" : "doc1"},
                    "update" : {"$set" : {"date": { "$date": 1234567890 }}},
                    "options": {"upsert": true}
                  }
                }
                """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
                      {
                        "_id": "doc1",
                        "username": "user1",
                        "date": { "$date": 1234567890 }
                      }
                      """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                      {
                        "find": {
                          "filter" : {"_id" : "doc1"}
                        }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void unsetWithDateField() {
      final String document =
          """
                    {
                      "_id": "doc1",
                      "createdAt": {
                        "$date": 1234567
                      }
                    }
                    """;
      insertDoc(document);
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                        {
                          "findOneAndUpdate": {
                            "filter" : {"_id" : "doc1"},
                            "update" : {"$unset" : {"createdAt": 1}}
                          }
                        }
                        """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
                      {
                        "_id": "doc1"
                      }
                      """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                              {
                                "find": {
                                  "filter" : {"_id" : "doc1"}
                                }
                              }
                              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void trySetWithInvalidDateField() {
      final String document =
          """
                        {
                          "_id": "doc1",
                          "createdAt": {
                            "$date": 1234567
                          }
                        }
                        """;
      insertDoc(document);
      String json =
          """
              {
                "findOneAndUpdate": {
                  "filter" : {"_id" : "doc1"},
                  "update" : {"$set" : {"createdAt": { "$date": "2023-01-01T00:00:00Z" }}}
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
          .body("errors[0].errorCode", is("SHRED_BAD_EJSON_VALUE"))
          .body(
              "errors[0].message",
              is(
                  ErrorCode.SHRED_BAD_EJSON_VALUE.getMessage()
                      + ": Date ($date) needs to have NUMBER value, has STRING (path 'createdAt')"));
    }
  }

  @Nested
  @Order(7)
  class FindOneAndUpdateWithCurrentDate {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void simpleCurrentDate() throws Exception {
      final long startTime = System.currentTimeMillis();
      final String document =
          """
                    {
                      "_id": "doc1",
                      "createdAt": {
                        "$date" : 123456
                      }
                    }
                    """;
      insertDoc(document);
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                        {
                          "findOneAndUpdate": {
                            "filter" : {"_id" : "doc1"},
                            "update" : {
                              "$currentDate": {
                                "createdAt": true,
                                "updatedAt": true
                              }
                            }
                          }
                        }
                        """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      String json =
          given()
              .headers(getHeaders())
              .contentType(ContentType.JSON)
              .body(
                  """
                              {
                                "find": {
                                  "filter" : {"_id" : "doc1"}
                                }
                              }
                              """)
              .when()
              .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
              .then()
              .statusCode(200)
              .body("data.documents", hasSize(1))
              .extract()
              .asString();

      // Alas can't compare to static doc due to current-date value varying so need
      // to extract separately
      final long endTime = System.currentTimeMillis();
      JsonNode foundDoc = MAPPER.readTree(json).at("/data/documents/0");
      assertThat(foundDoc.size()).isEqualTo(3);
      assertThat(foundDoc.path("_id").textValue()).isEqualTo("doc1");
      long createdAt = foundDoc.at("/createdAt/$date").longValue();
      assertThat(createdAt).isBetween(startTime, endTime);
      long updatedAt = foundDoc.at("/updatedAt/$date").longValue();
      assertThat(updatedAt).isBetween(startTime, endTime);
      // Also should use same timestamp for all updates of one operation
      assertThat(createdAt).isEqualTo(updatedAt);
    }

    @Test
    public void tryCurrentDateWithInvalidArg() {
      insertDoc("{\"_id\": \"doc1\"}");
      String json =
          """
                  {
                    "findOneAndUpdate": {
                      "filter" : {"_id" : "doc1"},
                      "update" : {"$currentDate" : {"createdAt": 123}}
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
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_OPERATION_PARAM"))
          .body(
              "errors[0].message",
              startsWith(
                  "Unsupported update operation parameter: $currentDate requires argument of"));
    }
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }

  @Nested
  @Order(99)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindOneAndUpdateIntegrationTest.super.checkMetrics("FindOneAndUpdateCommand");
      FindOneAndUpdateIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
