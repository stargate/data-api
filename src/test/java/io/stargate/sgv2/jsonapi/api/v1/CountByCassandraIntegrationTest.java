package io.stargate.sgv2.jsonapi.api.v1;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value = CountByCassandraIntegrationTest.CassandraCounterTestResource.class,
    restrictToAnnotatedClass = true)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CountByCassandraIntegrationTest extends AbstractCollectionIntegrationTestBase {

  // Need to set max count limit to -1, so cassandra count(*) is used
  // to trigger test failure for "not enough indexes"
  public static class CassandraCounterTestResource extends DseTestResource {
    public CassandraCounterTestResource() {}

    @Override
    public int getMaxCountLimit() {
      return -1;
    }
  }

  @Test
  @Order(1)
  public void setUp() {
    createNamespace();
    createSimpleCollection();

    insertDoc(
        """
            {
              "_id": "doc1",
              "username": "user1",
              "active_user" : true
            }
            """);

    insertDoc(
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
            """);

    insertDoc(
        """
            {
              "_id": "doc3",
              "username": "user3",
              "tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true],
              "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]
            }
            """);

    insertDoc(
        """
            {
              "_id": "doc4",
              "indexedObject" : { "0": "value_0", "1": "value_1" }
            }
            """);
    insertDoc(
        """
            {
              "_id": "doc5",
              "username": "user5",
              "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
            }
            """);
  }

  @Test
  public void noFilter() {
    verifyCountCommand(
        5,
        """
            {
              "countDocuments": {
              }
            }
            """);
  }

  @Test
  public void emptyOptionsAllowed() {
    verifyCountCommand(
        5,
        """
            {
              "countDocuments": {
                "options": {}
              }
            }
            """);
  }

  @Test
  public void byColumn() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"username" : "user1"}
              }
            }
            """);
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
    verifyCountCommand(2, json);
  }

  @Test
  public void withEqComparisonOperator() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"username" : {"$eq" : "user1"}}
              }
            }
            """);
  }

  @Test
  public void withEqSubDoc() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"subdoc.id" : {"$eq" : "abc"}}
              }
            }
            """);
  }

  @Test
  public void withEqSubDocWithIndex() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"indexedObject.1" : {"$eq" : "value_1"}}
              }
            }
            """);
  }

  @Test
  public void withEqArrayElement() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"array.0" : {"$eq" : "value1"}}
              }
            }
            """);
  }

  @Test
  public void withExistFalseOperator() {
    verifyCountCommand(
        4,
        """
            {
              "countDocuments": {
                "filter" : {"active_user" : {"$exists" : false}}
              }
            }
            """);
  }

  @Test
  public void withExistOperator() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"active_user" : {"$exists" : true}}
              }
            }
            """);
  }

  @Test
  public void withAllOperator() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"tags" : {"$all" : ["tag1", "tag2"]}}
              }
            }
            """);
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

    verifyCountCommand(0, json);
  }

  @Test
  public void withAllOperatorLongerString() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"tags" : {"$all" : ["tag1", "tag1234567890123456789012345"]}}
              }
            }
            """);
  }

  @Test
  public void withAllOperatorMixedAFormatArray() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"tags" : {"$all" : ["tag1", 1, true, null]}}
              }
            }
            """);
  }

  @Test
  public void withAllOperatorNoMatch() {
    verifyCountCommand(
        0,
        """
            {
              "countDocuments": {
                "filter" : {"tags" : {"$all" : ["tag1", 2, true, null]}}
              }
            }
            """);
  }

  @Test
  public void withEqSubDocumentShortcut() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } } }
              }
            }
            """);
  }

  @Test
  public void withEqSubDocument() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"sub_doc" : { "$eq" : { "a": 5, "b": { "c": "v1", "d": false } } } }
              }
            }
            """);
  }

  @Test
  public void withEqSubDocumentOrderChangeNoMatch() {
    verifyCountCommand(
        0,
        """
            {
              "countDocuments": {
                "filter" : {"sub_doc" : { "$eq" : { "a": 5, "b": { "d": false, "c": "v1" } } } }
              }
            }
            """);
  }

  @Test
  public void withEqSubDocumentNoMatch() {
    verifyCountCommand(
        0,
        """
            {
              "countDocuments": {
                "filter" : {"sub_doc" : { "$eq" : { "a": 5, "b": { "c": "v1", "d": true } } } }
              }
            }
            """);
  }

  @Test
  public void withSizeOperator() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"tags" : {"$size" : 6}}
              }
            }
            """);
  }

  @Test
  public void withSizeOperatorNoMatch() {
    verifyCountCommand(
        0,
        """
            {
              "countDocuments": {
                "filter" : {"tags" : {"$size" : 1}}
              }
            }
            """);
  }

  @Test
  public void withEqOperatorArray() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"tags" : {"$eq" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true]}}
              }
            }
            """);
  }

  @Test
  public void withEqOperatorNestedArray() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"nestedArray" : {"$eq" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}}
              }
            }
            """);
  }

  @Test
  public void withEqOperatorArrayNoMatch() {
    verifyCountCommand(
        0,
        """
            {
              "countDocuments": {
                "filter" : {"tags" : {"$eq" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1]}}
              }
            }
            """);
  }

  @Test
  public void withEqOperatorNestedArrayNoMatch() {
    verifyCountCommand(
        0,
        """
            {
              "countDocuments": {
                "filter" : {"nestedArray" : {"$eq" : [["tag1", "tag2"], ["tag1234567890123456789012345", null], ["abc"]]}}
              }
            }
            """);
  }

  @Test
  public void withNEComparisonOperator() {
    verifyCountCommand(
        4,
        """
            {
              "countDocuments": {
                "filter" : {"username" : {"$ne" : "user1"}}
              }
            }
            """);
  }

  @Test
  public void byBooleanColumn() {
    verifyCountCommand(
        1,
        """
            {
              "countDocuments": {
                "filter" : {"active_user" : true}
              }
            }
            """);
  }

  protected ValidatableResponse verifyCountCommand(int expectedCount, String json) {
    return givenHeadersPostJsonThenOkNoErrors(json)
        .body("data", is(nullValue()))
        .body("status.count", is(expectedCount));
  }
}
