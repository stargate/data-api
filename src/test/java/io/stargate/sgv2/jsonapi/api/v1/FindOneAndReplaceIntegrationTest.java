package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneAndReplaceIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @Order(1)
  class FindOneAndReplace {
    @Test
    public void byId() {
      final String document =
          """
            {
              "_id": "doc3",
              "username": "user3",
              "active_user" : true
            }
            """;
      insertDoc(document);

      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "findOneAndReplace": {
                "filter" : {"_id" : "doc3"},
                "replacement" : { "username": "user3", "status" : false }
              }
            }
            """)
          .body("$", responseIsFindAndSuccess())
          .body("data.document", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "find": {
                "filter" : {"_id" : "doc3"}
              }
            }
            """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
            {
              "_id": "doc3",
              "username": "user3",
              "status" : false
            }
            """));
    }

    @Test
    public void byIdWithId() {
      final String document =
          """
            {
              "_id": "doc3",
              "username": "user3",
              "active_user" : true
            }
            """;
      insertDoc(document);

      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "findOneAndReplace": {
                "filter" : {"_id" : "doc3"},
                "replacement" : {"_id" : "doc3", "username": "user3", "status" : false }
              }
            }
            """)
          .body("$", responseIsFindAndSuccess())
          .body("data.document", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "find": {
                "filter" : {"_id" : "doc3"}
              }
            }
            """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
            {
              "_id": "doc3",
              "username": "user3",
              "status" : false
            }
            """));
    }

    @Test
    public void byIdWithIdNoChange() {
      final String document =
          """
            {
              "_id": "doc3",
              "username": "user3",
              "active_user" : true
            }
            """;
      insertDoc(document);

      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "findOneAndReplace": {
                "filter" : {"_id" : "doc3"},
                "replacement" : {"_id" : "doc3", "username": "user3", "active_user" : true }
              }
            }
            """)
          .body("$", responseIsFindAndSuccess())
          .body("data.document", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(0));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "find": {
                    "filter" : {"_id" : "doc3"}
                  }
                }
                """)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(document));
    }

    @Test
    public void withSort() {
      final String document =
          """
          {
            "_id": "doc3",
            "username": "user3",
            "active_user" : true
          }
          """;
      insertDoc(document);

      final String document1 =
          """
          {
            "_id": "doc2",
            "username": "user2",
            "active_user" : true
          }
          """;
      insertDoc(document1);
      final String expected =
          """
              {
                "_id": "doc2",
                "username": "username2",
                "status" : true
              }
              """;

      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "findOneAndReplace": {
                  "filter" : {"active_user" : true},
                  "sort" : {"username" : 1},
                  "replacement" : {"username": "username2", "status" : true },
                  "options" : {"returnDocument" : "after"}
                }
              }
              """)
          .body("$", responseIsFindAndSuccess())
          .body("data.document", jsonEquals(expected))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "find": {
                    "filter" : {"_id" : "doc2"}
                  }
                }
                """)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void withUpsert() {
      final String expected =
          """
        {
          "_id": "doc2",
          "username": "username2",
          "status" : true
        }
        """;

      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "findOneAndReplace": {
            "filter" : {"_id" : "doc2"},
            "replacement" : {"username": "username2", "status" : true },
            "options" : {"returnDocument" : "after", "upsert" : true}
          }
        }
        """)
          .body("$", responseIsFindAndSuccess())
          .body("data.document", jsonEquals(expected))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.upsertedId", is("doc2"));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "find": {
            "filter" : {"_id" : "doc2"}
          }
        }
        """)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected));
    }

    /** document does not exist, we upsert a new one, it has a numeric ID See GH issue #2378 */
    @Test
    public void withUpsertNewIdNumeric() {

      insertDoc(
          """
          {
            "_id": 1,
            "hello": "world"
          }""");

      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "findOneAndReplace": {
                "filter": {
                  "_id": 3
                },
                "replacement": {
                  "_id": 3,
                  "hallo": "welt"
                },
                "options": {
                  "upsert": true,
                  "returnDocument": "after"
                }
              }
            }
          """)
          .body("$", responseIsFindAndSuccess())
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.upsertedId", is(3))
          .body("data.document._id", is(3));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "find": {
                "filter" : {"hallo": "welt"}
              }
            }
            """)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]._id", is(3));
    }

    @Test
    public void withUpsertNewId() {
      final String newId = "new-id-1234";
      givenHeadersPostJsonThenOkNoErrors(
                  """
                {
                  "findOneAndReplace": {
                    "filter" : {},
                    "replacement" : {
                      "username": "aaronm",
                      "_id": "%s"
                    },
                    "options" : {
                      "returnDocument": "after",
                      "upsert": true
                    }
                  }
                }
                """
                  .formatted(newId))
          .body("$", responseIsFindAndSuccess())
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("data.document._id", is(newId))
          // Should we return id of new document as upsertedId?
          .body("status.upsertedId", is(newId));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "find": {
                    "filter" : {"username" : "aaronm"}
                  }
                }
                """)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]._id", is(newId));
    }

    @Test
    public void withUpsertNoId() {
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "findOneAndReplace": {
                "filter" : {"username" : "username2"},
                "replacement" : {"username": "username2", "status" : true },
                "options" : {"returnDocument" : "after", "upsert" : true}
              }
            }
            """)
          .body("$", responseIsFindAndSuccess())
          .body("data.document._id", is(notNullValue()))
          .body("data.document._id", any(String.class))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.upsertedId", is(notNullValue()))
          .body("status.upsertedId", any(String.class));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "find": {
                "filter" : {"username" : "username2"}
              }
            }
            """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body("data.documents[0]._id", is(notNullValue()))
          .body("data.documents[0]._id", any(String.class));
    }

    @Test
    public void byIdWithDifferentId() {
      final String document =
          """
            {
              "_id": "doc3",
              "username": "user3",
              "active_user" : true
            }
            """;
      insertDoc(document);
      givenHeadersPostJsonThenOk(
              """
                {
                  "findOneAndReplace": {
                    "filter" : {"_id" : "doc3"},
                    "replacement" : {"_id" : "doc4", "username": "user3", "status" : false }
                  }
                }
                """)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("DOCUMENT_REPLACE_DIFFERENT_DOCID"))
          .body(
              "errors[0].message",
              startsWith(
                  "The replace document and document resolved using filter have different '_id's: StringId('doc4') (replace document) vs. StringId('doc3') (document that filter matches)."));
    }

    @Test
    public void byIdWithEmptyDocument() {
      final String document =
          """
                {
                  "_id": "doc3",
                  "username": "user3",
                  "active_user" : true
                }
                """;
      insertDoc(document);

      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "findOneAndReplace": {
                    "filter" : {"_id" : "doc3"},
                    "replacement" : {}
                  }
                }
                """)
          .body("$", responseIsFindAndSuccess())
          .body("data.document", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "find": {
                    "filter" : {"_id" : "doc3"}
                  }
                }
                """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                {
                  "_id": "doc3"
                }
                """));
    }
  }

  @Nested
  @Order(2)
  class FindOneAndReplaceWithProjection {
    @Test
    public void byIdProjectionAfter() {
      insertDoc(
          """
                {
                  "_id": "docProjAfter",
                  "username": "userP",
                  "active_user" : true
                }
                """);

      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "findOneAndReplace": {
                    "filter" : {"_id" : "docProjAfter"},
                    "options" : {"returnDocument" : "after"},
                    "projection" : { "active_user":1, "status":1 },
                    "replacement" : { "username": "userP", "status" : false }
                  }
                }
                """)
          .body("$", responseIsFindAndSuccess())
          .body(
              "data.document",
              jsonEquals(
                  """
                {
                  "_id": "docProjAfter",
                  "status" : false
                }
                """))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "find": {
                    "filter" : {"_id" : "docProjAfter"}
                  }
                }
                """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                {
                  "_id": "docProjAfter",
                  "username": "userP",
                  "status" : false
                }
                """));
    }

    @Test
    public void byIdProjectionBefore() {
      insertDoc(
          """
                {
                  "_id": "docProjBefore",
                  "username": "userP",
                  "active_user" : true
                }
                """);

      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "findOneAndReplace": {
                    "filter" : {"_id" : "docProjBefore"},
                    "options" : {"returnDocument" : "before"},
                    "projection" : { "active_user":1, "status":1 },
                    "replacement" : { "username": "userP", "status" : false }
                  }
                }
                """)
          .body("$", responseIsFindAndSuccess())
          .body(
              "data.document",
              jsonEquals(
                  """
                {
                  "_id": "docProjBefore",
                  "active_user" : true
                }
                """))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "find": {
                    "filter" : {"_id" : "docProjBefore"}
                  }
                }
                """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                {
                  "_id": "docProjBefore",
                  "username": "userP",
                  "status" : false
                }
                """));
    }

    // Reproduction to verify https://github.com/stargate/data-api/issues/1000
    // is fixed in v1.0.6
    @Test
    public void projectionBeforeWithoutId() {
      insertDoc(
          """
              {
                "_id": "docProjBeforeNoId",
                "username": "aaron"
              }
              """);

      String upsertedId =
          givenHeadersPostJsonThenOkNoErrors(
                  """
                        {
                          "findOneAndReplace": {
                            "filter": { "no.such.field": "or.value" },
                            "replacement": { },
                            "options": { "returnDocument": "before", "upsert": true },
                            "projection": { "*": 0 }
                          }
                        }
                        """)
              .body("$", responseIsFindAndSuccess())
              .body("status.matchedCount", is(0))
              .body("status.modifiedCount", is(0))
              // Does upsert
              .body("status.upsertedId", is(notNullValue()))
              // No match so no before-document:
              .body("data.document", is(nullValue()))
              .extract()
              .path("status.upsertedId");

      // assert state after update
      String expectedAfterReplace = "{\"_id\":\"%s\"}".formatted(upsertedId);
      givenHeadersPostJsonThenOkNoErrors(
                  """
                        {
                          "find": {
                            "filter" : {"_id" : "%s"}
                          }
                        }
                        """
                  .formatted(upsertedId))
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expectedAfterReplace));
    }
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }

  @Nested
  @Order(3)
  class FindOneAndReplaceFailing {
    @Test
    public void tryReplaceWithTooLongNumber() {
      insertDoc(
          """
                {
                  "_id": "tooLongNumber1",
                  "value" : 123
                }
                """);

      // Max number length: 100; use 110
      String tooLongNumStr = "1234567890".repeat(11);
      String json =
              """
                {
                  "findOneAndReplace": {
                    "filter" : {"_id" : "tooLongNumber1"},
                    "replacement" : {
                        "_id" : "tooLongNumber1",
                        "value" : %s
                    }
                  }
                }
                """
              .formatted(tooLongNumStr);
      givenHeadersPostJsonThenOk(json)
          .body("errors", hasSize(1))
          .body("$", responseIsError())
          .body("errors[0].errorCode", is(DocumentException.Code.SHRED_DOC_LIMIT_VIOLATION.name()))
          .body(
              "errors[0].message",
              containsString("Document size limitation violated: Number value length"));
    }
  }

  @Nested
  @Order(99)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindOneAndReplaceIntegrationTest.checkMetrics("FindOneAndReplaceCommand");
      FindOneAndReplaceIntegrationTest.checkDriverMetricsTenantId();
      FindOneAndReplaceIntegrationTest.checkIndexUsageMetrics("FindOneAndReplaceCommand", false);
    }
  }
}
