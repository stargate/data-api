package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
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
      givenHeadersPostJsonThenOkNoErrors("{\"findOne\": { } }")
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void noFilter() {
      givenHeadersPostJsonThenOkNoErrors("{\"findOne\": { } }")
          .body("$", responseIsFindSuccess())
          // Test run after documents inserted
          .body("data.document", is(not(nullValue())));
    }

    @Test
    public void emptyOptionsAllowed() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "options": {}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(not(nullValue())));
    }

    @Test
    public void includeSortVectorOptionsAllowed() {
      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "findOne": {
                  "options": {
                    "includeSortVector": true
                  }
                }
              }
              """)
          .body("$", responseIsFindAndSuccess())
          .body("data.document", is(not(nullValue())))
          .body("status.sortVector", nullValue());
    }

    @Test
    public void noFilterSortAscending() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "sort" : {"username" : 1}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC4_JSON)); // missing value is the lowest precedence
    }

    @Test
    public void noFilterSortDescending() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "sort" : {"username" : -1 }
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC5_JSON)); // missing value is the lowest precedence
    }

    @Test
    public void byId() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"_id" : "doc1"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC1_JSON));
    }

    // https://github.com/stargate/jsonapi/issues/572 -- is passing empty Object for "sort" ok?
    @Test
    public void byIdEmptySort() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "findOne": {
                    "filter": {"_id" : "doc1"},
                    "sort": {}
                  }
                }
                """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC1_JSON));
    }

    @Test
    public void byIdNotFound() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"_id" : "none"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void inCondition() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"_id" : {"$in": ["doc5", "doc4"]}}
            }
          }
          """)
          // findOne resolves any one of the resolved documents. So the order of the documents in
          // the
          // $in clause is not guaranteed.
          .body("$", responseIsFindSuccess())
          .body("data.document", anyOf(jsonEquals(DOC5_JSON), jsonEquals(DOC4_JSON)));
    }

    @Test
    public void inConditionEmptyArray() {
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "findOne": {
            "filter" : {"_id" : {"$in": []}}
          }
        }
            """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void inConditionNonArrayArray() {
      givenHeadersPostJsonThenOk(
              """
        {
          "findOne": {
            "filter" : {"_id" : {"$in": true}}
          }
        }
        """)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("FILTER_INVALID_EXPRESSION"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body("errors[0].message", containsString("'$in' operator must have `Array`"));
    }

    @Test
    public void ninConditionNonArrayArray() {
      givenHeadersPostJsonThenOk(
              """
            {
              "findOne": {
                "filter" : {"_id" : {"$nin": false}}
              }
            }
            """)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("FILTER_INVALID_EXPRESSION"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body("errors[0].message", containsString("'$nin' operator must have `Array`"));
    }

    @Test
    public void inConditionNonIdField() {
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "findOne": {
            "filter" : {"non_id" : {"$in": ["a", "b", "c"]}}
          }
        }
        """)
          .body("$", responseIsFindSuccess());
    }

    @Test
    public void byColumn() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"username" : "user1"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC1_JSON));
    }

    @Test
    public void byColumnMissing() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"nickname" : "user1"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void byColumnNotMatching() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"username" : "batman"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void withExistsOperatorSortAsc() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"username" : {"$exists" : true}},
              "sort" : {"username" : 1 }
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC1_JSON));
    }

    @Test
    public void withExistsOperatorSortDesc() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"username" : {"$exists" : true}},
              "sort" : {"username" : -1}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          // post sorting by sort id , it uses document id by default.
          .body("data.document", jsonEquals(DOC5_JSON));
    }

    @Test
    public void withExistsOperator() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"active_user" : {"$exists" : true}}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC1_JSON));
    }

    @Test
    public void withExistsOperatorFalse() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"active_user" : {"$exists" : false}}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(not(nullValue())));
    }

    @Test
    public void withExistsNotMatching() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"power_rating" : {"$exists" : true}}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void withAllOperatorMissing() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"tags-and-button" : {"$all" : ["tag1", "tag2"]}}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void withAllOperatorNotMatching() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"tags" : {"$all" : ["tag1", "tag2", "tag-not-there"]}}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void withAllOperatorNotArray() {
      givenHeadersPostJsonThenOk(
              """
          {
            "findOne": {
              "filter" : {"tags" : {"$all" : 1}}
            }
          }
          """)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("FILTER_INVALID_EXPRESSION"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body("errors[0].message", containsString("'$all' operator must have `Array` value"));
    }

    @Test
    public void withSizeOperator() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"tags" : {"$size" : 6}}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC3_JSON));
    }

    @Test
    public void withSizeOperatorNotMatching() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"tags" : {"$size" : 78}}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void withSizeOperatorNotNumber() {
      givenHeadersPostJsonThenOk(
              """
          {
            "findOne": {
              "filter" : {"tags" : {"$size" : true}}
            }
          }
          """)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("FILTER_INVALID_EXPRESSION"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body("errors[0].message", containsString("'$size' operator must have integer"));
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
      givenHeadersPostJsonThenOkNoErrors(request)
          .body("$", responseIsFindSuccess())
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
      givenHeadersPostJsonThenOkNoErrors(request)
          .body("$", responseIsFindSuccess())
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
      givenHeadersPostJsonThenOkNoErrors(request)
          .body("$", responseIsFindSuccess())
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
      givenHeadersPostJsonThenOkNoErrors(request)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC4));
    }
  }

  @Nested
  @Order(3)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindOneFilterWithDottedPaths {
    private final String DOC1 =
        """
                    {
                      "_id": "dotted1",
                      "app.kubernetes.io/name": "dotted1",
                      "pricing": {
                        "price.usd": 25.5,
                        "price&.aud": 10.5,
                        "currency": "USD"
                      }
                    }
                    """;

    private final String DOC2 =
        """
                    {
                      "_id": "dotted2",
                      "app.kubernetes.io/name": "dotted2",
                      "pricing.price.usd": 12.5,
                      "pricing&price&aud": 25.5,
                      "pricing.currency": "USD"
                    }
                    """;

    @Test
    @Order(1)
    public void setUp() {
      insertDoc(DOC1);
      insertDoc(DOC2);
    }

    @Test
    public void byDottedFieldSimpleEq() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"pricing.price&.usd" : 25.5}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC1));
    }

    @Test
    public void byDottedFieldSimpleEqWithoutEscape() {
      // should find nothing if the document is not correctly escaped
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
                "findOne": {
                "filter" : {"pricing.price.usd" : 25.5}
                }
            }
            """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void byDottedFieldTwoEqs() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter": {
                "pricing&.currency": {"$eq": "USD"},
                "app&.kubernetes&.io/name": {"$eq": "dotted2"}
              }
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC2));
    }

    @Test
    public void byDottedFieldTwoEqsWithoutEscape() {
      // should find nothing if the document is not correctly escaped
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter": {
                "pricing.currency": {"$eq": "USD"},
                "app.kubernetes.io/name": {"$eq": "dotted2"}
              }
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void byDottedFieldComplexEscapeEq() {
      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "findOne": {
                  "filter" : {"pricing.price&&&.aud" : 10.5}
                }
              }
              """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(DOC1));
    }

    @Test
    public void failWithInvalidEscape() {
      // assume the user forgot to escape the ampersand
      givenHeadersPostJsonThenOk(
              """
              {
                "findOne": {
                  "filter" : {"pricing&price&aud" : 25.5}
                }
              }
              """)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("FILTER_INVALID_EXPRESSION"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body(
              "errors[0].message",
              containsString(
                  "Unsupported filter clause: filter expression path ('pricing&price&aud') is not valid"));
    }
  }

  @Nested
  @Order(4)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindOneFail {
    @Test
    public void failForMissingCollection() {
      givenHeadersAndJson("{ \"findOne\": { \"filter\" : {\"_id\": \"doc1\"}}}")
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, "no_such_collection")
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("COLLECTION_NOT_EXIST"))
          .body("errors[0].exceptionClass", is("SchemaException"))
          .body(
              "errors[0].message",
              containsString("No collection or table with name 'no_such_collection' exists."));
    }

    @Test
    public void failForInvalidJsonExtension() {
      givenHeadersPostJsonThenOk("{ \"findOne\": { \"filter\" : {\"_id\": {\"$guid\": \"doc1\"}}}}")
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("FILTER_UNSUPPORTED_OPERATOR"))
          .body("errors[0].exceptionClass", is("FilterException"))
          .body("errors[0].message", startsWith("Unsupported filter operator '$guid'"));
    }

    @Test
    public void failForInvalidUUIDAsId() {
      givenHeadersPostJsonThenOk(
              "{ \"findOne\": { \"filter\" : {\"_id\": {\"$uuid\": \"not-an-uuid\"}}}}")
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("SHRED_BAD_DOCID_TYPE"))
          .body("errors[0].exceptionClass", is("DocumentException"))
          .body(
              "errors[0].message",
              containsString(
                  "Bad JSON Extension value: '$uuid' value has to be 36-character UUID String, instead got (\"not-an-uuid\")"));
    }

    @Test
    public void failForInvalidObjectIdAsId() {
      givenHeadersPostJsonThenOk(
              "{ \"findOne\": { \"filter\" : {\"_id\": {\"$objectId\": \"bogus\"}}}}")
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("SHRED_BAD_DOCID_TYPE"))
          .body("errors[0].exceptionClass", is("DocumentException"))
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
      FindOneIntegrationTest.checkMetrics("FindOneCommand");
      FindOneIntegrationTest.checkDriverMetricsTenantId();
      FindOneIntegrationTest.checkIndexUsageMetrics("FindOneCommand", false);
    }
  }
}
