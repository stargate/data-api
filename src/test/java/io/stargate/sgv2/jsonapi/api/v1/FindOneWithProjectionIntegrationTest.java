package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class FindOneWithProjectionIntegrationTest extends AbstractCollectionIntegrationTestBase {
  private static final String DOC1_JSON =
      """
                {
                  "_id": "doc1",
                  "username": "user1",
                  "active_user" : true,
                  "created_at": {
                     "$date": 123456789
                  },
                  "extra" : {
                     "modified": {
                       "$date": 111111111
                     }
                  }
                }
                """;

  private static final String DOC2_JSON =
      """
                {
                  "_id": "doc2",
                  "username": "user2",
                  "tags" : ["tag1", "tag2", "tag42", "tag1972", "zzzz"],
                  "nestedArray" : [["tag1", "tag2"], ["tag3", null]]
                }
                """;

  private static final String DOC3_JSON =
      """
                {
                  "_id": "doc3",
                  "username": "user3",
                  "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
                }
                """;

  @Nested
  class BasicProjection {
    @Test
    public void byIdNestedExclusion() {
      insertDoc(DOC1_JSON);
      insertDoc(DOC2_JSON);
      insertDoc(DOC3_JSON);
      String json =
          """
          {
            "findOne": {
              "filter" : {"_id" : "doc3"},
              "projection": { "sub_doc.b": 0 }
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
          .body(
              "data.document",
              jsonEquals(
                  """
                            {
                              "_id": "doc3",
                              "username": "user3",
                              "sub_doc" : { "a": 5 }
                            }
                            """))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.document", is(not(nullValue())));
    }

    @Test
    public void byIdIncludeDates() {
      insertDoc(DOC1_JSON);
      insertDoc(DOC2_JSON);
      insertDoc(DOC3_JSON);
      String json =
          """
          {
            "findOne": {
              "filter" : {"_id" : "doc1"},
              "projection": {
                "created_at": 1,
                "extra.modified": 1
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
          .body(
              "data.document",
              jsonEquals(
                  """
                          {
                            "_id": "doc1",
                            "created_at": {
                               "$date": 123456789
                            },
                            "extra" : {
                               "modified": {
                                 "$date": 111111111
                               }
                            }
                          }
                          """))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void byIdExcludeDates() {
      insertDoc(DOC1_JSON);
      insertDoc(DOC2_JSON);
      insertDoc(DOC3_JSON);
      String json =
          """
          {
            "findOne": {
              "filter" : {"_id" : "doc1"},
              "projection": {
                "created_at": 0,
                "extra.modified": 0
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
          .body(
              "data.document",
              jsonEquals(
                  """
                            {
                              "_id": "doc1",
                              "username": "user1",
                              "active_user" : true,
                              "extra": {
                              }
                            }
                            """))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @AfterEach
    public void cleanUpData() {
      deleteAllDocuments();
    }
  }

  @Nested
  class ProjectionWithJSONExtensions {
    private static final String UUID1 = "123e4567-e89b-12d3-a456-426614174000";
    private static final String OBJECTID1 = "5f3b3e3b3e3b3e3b3e3b3e3b";
    private static final String EXT_DOC1 =
            """
                      {
                        "_id": "ext1",
                        "username": "user1",
                        "uid": { "$uuid": "%s" },
                        "oid": { "$objectId": "%s" },
                        "enabled": true
                      }
                      """
            .formatted(UUID1, OBJECTID1);

    @Test
    public void byIdDefaultProjection() {
      insertDoc(EXT_DOC1);
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                      {
                        "findOne": {
                          "filter" : {"_id" : "ext1"}
                        }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.document", jsonEquals(EXT_DOC1));
    }

    @Test
    public void byIdIncludeExtValues() {
      insertDoc(EXT_DOC1);
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
              {
                "findOne": {
                  "filter" : {"_id" : "ext1"},
                  "projection": { "uid": 1, "oid": 1 }
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body(
              "data.document",
              jsonEquals(
                      """
                                        {
                                          "_id": "ext1",
                                          "uid": { "$uuid": "%s" },
                                          "oid": { "$objectId": "%s" }
                                        }
                                        """
                      .formatted(UUID1, OBJECTID1)));
    }

    @Test
    public void byIdExcludeExtValues() {
      insertDoc(EXT_DOC1);
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
              {
                "findOne": {
                  "filter" : {"_id" : "ext1"},
                  "projection": { "uid": 0, "oid": 0 }
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body(
              "data.document",
              jsonEquals(
                  """
                      {
                        "_id": "ext1",
                        "username": "user1",
                        "enabled": true
                      }
                                        """));
    }

    @AfterEach
    public void cleanUpData() {
      deleteAllDocuments();
    }
  }

  @Nested
  class ProjectionWithSimpleSlice {
    @Test
    public void byIdRootSliceHead() {
      insertDoc(DOC1_JSON);
      insertDoc(DOC2_JSON);
      insertDoc(DOC3_JSON);
      String json =
          """
          {
            "findOne": {
              "filter" : {"_id" : "doc2"},
              "projection": { "tags": { "$slice" : 2 }  }
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
          .body(
              "data.document",
              jsonEquals(
                  """
                            {
                              "_id": "doc2",
                              "username": "user2",
                              "tags" : ["tag1", "tag2"],
                              "nestedArray" : [["tag1", "tag2"], ["tag3", null]]
                            }
                            """))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void byIdRootSliceTail() {
      insertDoc(DOC1_JSON);
      insertDoc(DOC2_JSON);
      insertDoc(DOC3_JSON);
      String json =
          """
                      {
                        "findOne": {
                          "filter" : {"_id" : "doc2"},
                          "projection": { "tags": { "$slice" : -2 }  }
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
          .body(
              "data.document",
              jsonEquals(
                  """
                            {
                              "_id": "doc2",
                              "username": "user2",
                              "tags" : ["tag1972", "zzzz"],
                              "nestedArray" : [["tag1", "tag2"], ["tag3", null]]
                            }
                            """))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }
  }

  @Nested
  class ProjectionWithFullSlice {
    @Test
    public void byIdRootSliceHeadOverrun() {
      insertDoc(DOC1_JSON);
      insertDoc(DOC2_JSON);
      insertDoc(DOC3_JSON);
      String json =
          """
          {
            "findOne": {
              "filter" : {"_id" : "doc2"},
              "projection": {
                "tags": { "$slice" : [3, 99] },
                "username" : 1
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
          .body(
              "data.document",
              jsonEquals(
                  """
                          {
                            "_id": "doc2",
                            "username": "user2",
                            "tags" : ["tag1972", "zzzz"]
                          }
                          """))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void byIdRootSliceTail() {
      insertDoc(DOC1_JSON);
      insertDoc(DOC2_JSON);
      insertDoc(DOC3_JSON);
      String json =
          """
          {
            "findOne": {
              "filter" : {"_id" : "doc2"},
              "projection": {
                "tags": {
                  "$slice" : [-3, 2]
                },
                "username": 0
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
          .body(
              "data.document",
              jsonEquals(
                  """
                          {
                            "_id": "doc2",
                            "tags" : ["tag42", "tag1972"],
                            "nestedArray" : [["tag1", "tag2"], ["tag3", null]]
                          }
                          """))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void byIdNestedArraySliceHead() {
      insertDoc(DOC1_JSON);
      insertDoc(DOC2_JSON);
      insertDoc(DOC3_JSON);

      String json =
          """
              {
                "findOne": {
                  "filter" : {"_id" : "doc2"},
                  "projection": {
                    "nestedArray": {
                      "$slice" : [1, 1]
                    },
                    "username": 0
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
          .body(
              "data.document",
              jsonEquals(
                  """
                            {
                              "_id": "doc2",
                              "tags" : ["tag1", "tag2", "tag42", "tag1972", "zzzz"],
                              "nestedArray" : [["tag3", null]]
                            }
                            """))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @AfterEach
    public void cleanUpData() {
      deleteAllDocuments();
    }
  }
}
