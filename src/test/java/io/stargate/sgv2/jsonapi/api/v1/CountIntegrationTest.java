package io.stargate.sgv2.jsonapi.api.v1;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
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
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
/**
 * To run this test DseTestResource is updated to have maxCountLimit to `5` and getCountPageSize to
 * 2 so pagination and moreData flag can be tested
 */
public class CountIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(1)
  class Count {

    @Test
    @Order(1)
    public void setUp() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc1",
                "username": "user1",
                "active_user" : true
              }
            }
          }
          """;
      insert(json);

      json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc2",
                "username": "user2",
                "subdoc" : {
                   "id" : "abc"
                },
                "array" : [
                    "value1"
                ]
              }
            }
          }
          """;
      insert(json);

      json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc3",
                "username": "user3",
                "tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true],
                "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]
              }
            }
          }
          """;
      insert(json);

      json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc4",
                "indexedObject" : { "0": "value_0", "1": "value_1" }
              }
            }
          }
          """;
      insert(json);

      json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc5",
                "username": "user5",
                "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
              }
            }
          }
          """;
      insert(json);

      json =
          """
              {
                "insertOne": {
                  "document": {}
                }
              }
              """;
      insert(json);
    }

    private void insert(String json) {
      givenHeadersPostJsonThenOkNoErrors(json);
    }

    @Test
    public void noFilter() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                          { "countDocuments": { } }
                          """)
          .body("status.count", is(5))
          .body("status.moreData", is(true));
    }

    @Test
    public void emptyOptionsAllowed() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "countDocuments": {
              "options": {}
            }
          }
          """)
          .body("status.count", is(5))
          .body("status.moreData", is(true));
    }

    @Test
    public void byColumn() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "countDocuments": {
              "filter" : {"username" : "user1"}
            }
          }
          """)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void countBySimpleOr() {
      String json =
          """
          {
              "countDocuments": {
                  "filter": {
                      "$or": [
                          {
                              "username": "user1"
                          },
                          {
                              "username": "user2"
                          }
                      ]
                  }
              }
          }
              """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(2))
          .body("data", is(nullValue()));
    }

    @Test
    public void withEqComparisonOperator() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"username" : {"$eq" : "user1"}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void withEqSubDoc() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"subdoc.id" : {"$eq" : "abc"}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void withEqSubDocWithIndex() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"indexedObject.1" : {"$eq" : "value_1"}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void withEqArrayElement() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"array.0" : {"$eq" : "value1"}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void withExistFalseOperator() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"active_user" : {"$exists" : false}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(5))
          .body("data", is(nullValue()));
    }

    @Test
    public void withExistOperator() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"active_user" : {"$exists" : true}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void withAllOperator() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$all" : ["tag1", "tag2"]}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void withAllOperatorAnd() {
      String json =
          """
                              {
                                  "countDocuments": {
                                      "filter": {
                                          "$and": [
                                              {
                                                  "tags": {
                                                      "$all": [
                                                          "tag1",
                                                          "tag2"
                                                      ]
                                                  }
                                              },
                                              {
                                                  "active_user": {
                                                      "$exists": true
                                                  }
                                              }
                                          ]
                                      }
                                  }
                              }
                      """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(0))
          .body("data", is(nullValue()));
    }

    @Test
    public void withAllOperatorLongerString() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$all" : ["tag1", "tag1234567890123456789012345"]}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void withAllOperatorMixedAFormatArray() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$all" : ["tag1", 1, true, null]}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void withAllOperatorNoMatch() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$all" : ["tag1", 2, true, null]}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(0))
          .body("data", is(nullValue()));
    }

    @Test
    public void withEqSubDocumentShortcut() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } } }
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void withEqSubDocument() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"sub_doc" : { "$eq" : { "a": 5, "b": { "c": "v1", "d": false } } } }
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void withEqSubDocumentOrderChangeNoMatch() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"sub_doc" : { "$eq" : { "a": 5, "b": { "d": false, "c": "v1" } } } }
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(0))
          .body("data", is(nullValue()));
    }

    @Test
    public void withEqSubDocumentNoMatch() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"sub_doc" : { "$eq" : { "a": 5, "b": { "c": "v1", "d": true } } } }
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(0))
          .body("data", is(nullValue()));
    }

    @Test
    public void withSizeOperator() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$size" : 6}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void withSizeOperatorNoMatch() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$size" : 1}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(0))
          .body("data", is(nullValue()));
    }

    @Test
    public void withEqOperatorArray() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$eq" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true]}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void withEqOperatorNestedArray() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"nestedArray" : {"$eq" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }

    @Test
    public void withEqOperatorArrayNoMatch() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$eq" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1]}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(0))
          .body("data", is(nullValue()));
    }

    @Test
    public void withEqOperatorNestedArrayNoMatch() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"nestedArray" : {"$eq" : [["tag1", "tag2"], ["tag1234567890123456789012345", null], ["abc"]]}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(0))
          .body("data", is(nullValue()));
    }

    @Test
    public void withNEComparisonOperator() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"username" : {"$ne" : "user1"}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("status.count", is(5))
          .body("data", is(nullValue()));
    }

    @Test
    public void byBooleanColumn() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "countDocuments": {
              "filter" : {"active_user" : true}
            }
          }
          """)
          .body("status.count", is(1))
          .body("data", is(nullValue()));
    }
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      CountIntegrationTest.super.checkMetrics("CountDocumentsCommand");
      CountIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
