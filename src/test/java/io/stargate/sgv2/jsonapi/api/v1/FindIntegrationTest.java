package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindIntegrationTest extends AbstractCollectionIntegrationTestBase {

  private void insert(String json) {
    givenHeadersPostJsonThenOkNoErrors(json).body("$", responseIsWriteSuccess());
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(1)
  class Find {
    @Test
    @Order(1)
    public void setUp() {
      insert(
          """
                        {
                          "insertOne": {
                            "document": {
                              "_id": "doc1",
                              "username": "user1",
                              "active_user" : true,
                              "date" : {"$date": 1672531200000},
                              "age" : 20,
                              "null_column": null
                            }
                          }
                        }
                      """);

      insert(
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
                      """);

      insert(
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
                      """);

      insert(
          """
                        {
                          "insertOne": {
                            "document": {
                              "_id": "doc4",
                              "username" : "user4",
                              "indexedObject" : { "0": "value_0", "1": "value_1" }
                            }
                          }
                        }
                      """);

      insert(
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
                      """);

      insert(
          """
                        {
                          "insertOne": {
                            "document": {
                              "_id": {"$date": 6},
                              "user-name": "user6"
                            }
                          }
                        }
                      """);
    }

    @Test
    public void wrongKeyspace() {
      givenHeadersPostJsonThenOk(
              "something_else",
              collectionName,
              """
          {
            "find": {
              "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
              "options" : {
                  "limit" : 100
              }
            }
          }
          """)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body(
              "errors[0].message",
              containsString(
                  "The command tried to use a Keyspace that does not exist in the Database"))
          .body("errors[0].errorCode", is("UNKNOWN_KEYSPACE"));
    }

    @Test
    public void failWithNonEmptySortAndPageState() {
      givenHeadersPostJsonThenOk(
              """
                {
                 "find": {
                    "filter" : {"username" : "user1"},
                    "sort" : {"username" : 1},
                    "options" : {
                      "pageState" : "someState"
                    }
                  }
                }
                """)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              is("Invalid sort clause: pageState is not supported with non-empty sort clause"))
          .body("errors[0].errorCode", is("INVALID_SORT_CLAUSE"));
    }

    @Test
    public void happyWithEmptyPageStateAndNonEmptySort() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                 "find": {
                    "filter" : {"username" : "user1"},
                    "sort" : {"username" : 1},
                    "options" : {
                      "pageState" : ""
                    }
                  }
                }
                """)
          .body("$", responseIsFindSuccess());
    }

    @Test
    public void happyWithNullPageStateAndNonEmptySort() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                 "find": {
                    "filter" : {"username" : "user1"},
                    "sort" : {"username" : 1},
                    "options" : {
                      "pageState" : null
                    }
                  }
                }
                """)
          .body("$", responseIsFindSuccess());
    }

    @Test
    public void noFilter() {
      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "find": {
                }
              }
              """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(6));
    }

    @Test
    public void noFilterWithOptions() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "options" : {
                            "limit" : 2
                          }
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(2));
    }

    @Test
    public void noFilterWithIncludeSortVectorOptions() {
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "find": {
                "options" : {
                  "limit" : 2,
                  "includeSortVector" : true
                }
              }
            }
            """)
          .body("$", responseIsFindAndSuccess())
          .body("status.sortVector", nullValue())
          .body("data.documents", hasSize(2));
    }

    @Test
    public void byId() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                      {
                        "find": {
                          "filter" : {"_id" : "doc1"}
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                      {
                          "_id": "doc1",
                          "username": "user1",
                          "active_user" : true,
                          "date" : {"$date": 1672531200000},
                          "age" : 20,
                          "null_column": null
                      }
                      """));
    }

    // https://github.com/stargate/jsonapi/issues/572 -- is passing empty Object for "sort" ok?
    @Test
    public void byIdEmptySort() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "find": {
                    "filter": {"username" : "user1"},
                    "projection": {},
                    "options": {},
                    "sort": { }
                  }
                }
              """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                {
                    "_id": "doc1",
                    "username": "user1",
                    "active_user" : true,
                    "date" : {"$date": 1672531200000},
                    "age" : 20,
                    "null_column": null
                }
                """))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void byDateId() {
      String json =
          """
                      {
                        "find": {
                          "filter" : {"_id" : {"$date": 6 }}
                        }
                      }
                      """;

      String expected =
          """
                      {
                        "_id": {"$date": 6},
                        "user-name": "user6"
                      }
                      """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void byColumn() {
      String json =
          """
                      {
                        "find": {
                          "filter" : {"username" : "user1"}
                        }
                      }
                      """;

      String expected =
          """
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    // [https://github.com/stargate/jsonapi/issues/521]: allow hyphens in property names
    @Test
    public void byColumnWithHyphen() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                  {
                    "find": {
                      "filter" : {"user-name" : "user6"}
                    }
                  }
              """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                  {
                    "_id": {"$date": 6},
                    "user-name": "user6"
                  }
                """));
    }

    @Test
    public void withEqComparisonOperator() {
      String json =
          """
          {
            "find": {
              "filter" : {"username" : {"$eq" : "user1"}}
            }
          }
          """;

      String expected =
          """
          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void withEqSubDoc() {
      String json =
          """
          {
            "find": {
              "filter" : {"subdoc.id" : {"$eq" : "abc"}}
            }
          }
          """;

      String expected =
          """
              {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
              """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    @Order(2)
    public void withEqSubDocWithIndex() {
      String json =
          """
          {
            "find": {
              "filter" : {"indexedObject.1" : {"$eq" : "value_1"}}
            }
          }
          """;

      String expected =
          """
          {
            "_id": "doc4",
            "username":"user4",
            "indexedObject" : { "0": "value_0", "1": "value_1" }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void withEqArrayElement() {
      String json =
          """
          {
            "find": {
              "filter" : {"array.0" : {"$eq" : "value1"}}
            }
          }
          """;

      String expected =
          """
          {
            "_id": "doc2",
            "username": "user2",
            "subdoc": {"id": "abc"},
            "array": ["value1"]
          }
          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void withExistFalseOperator() {
      String json =
          """
          {
            "find": {
              "filter" : {"active_user" : {"$exists" : false}}
            }
          }
          """;

      String expected2 =
          """
                      {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                      """;
      String expected3 =
          """
              {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
              """;

      String expected4 =
          """
              {
                "_id": "doc4",
                "username":"user4",
                "indexedObject" : { "0": "value_0", "1": "value_1" }
              }
              """;
      String expected5 =
          """
              {
                "_id": "doc5",
                "username": "user5",
                "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
              }
              """;
      String expected6 =
          """
                          {
                            "_id": {"$date": 6},
                            "user-name": "user6"
                          }
                          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(5))
          .body(
              "data.documents",
              containsInAnyOrder(
                  jsonEquals(expected2),
                  jsonEquals(expected3),
                  jsonEquals(expected4),
                  jsonEquals(expected5),
                  jsonEquals(expected6)));
    }

    @Test
    public void withExistOperator() {
      String json =
          """
          {
            "find": {
              "filter" : {"active_user" : {"$exists" : true}}
            }
          }
          """;

      String expected =
          """
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void withAllOperator() {
      String json =
          """
          {
            "find": {
              "filter" : {"tags" : {"$all" : ["tag1", "tag2"]}}
            }
          }
          """;

      String expected =
          """
          {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    @Order(2)
    public void withAllOperatorLongerString() {
      String json =
          """
          {
            "find": {
              "filter" : {"tags" : {"$all" : ["tag1", "tag1234567890123456789012345"]}}
            }
          }
          """;

      String expected =
          """
          {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void withAllOperatorMixedAFormatArray() {
      String json =
          """
          {
            "find": {
              "filter" : {"tags" : {"$all" : ["tag1", 1, true, null]}}
            }
          }
          """;

      String expected =
          """
           {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void withAllOperatorNoMatch() {
      String json =
          """
          {
            "find": {
              "filter" : {"tags" : {"$all" : ["tag1", 2, true, null]}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    public void withEqSubDocumentShortcut() {
      String json =
          """
          {
            "find": {
              "filter" : {"sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } } }
            }
          }
          """;

      String expected =
          """
          {
            "_id": "doc5",
            "username": "user5",
            "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
          }
          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void withEqSubDocument() {
      String json =
          """
          {
            "find": {
              "filter" : {"sub_doc" : { "$eq" : { "a": 5, "b": { "c": "v1", "d": false } } } }
            }
          }
          """;

      String expected =
          """
          {
            "_id": "doc5",
            "username": "user5",
            "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
          }
          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void withEqSubDocumentOrderChangeNoMatch() {
      String json =
          """
          {
            "find": {
              "filter" : {"sub_doc" : { "$eq" : { "a": 5, "b": { "d": false, "c": "v1" } } } }
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    public void withEqSubDocumentNoMatch() {
      String json =
          """
          {
            "find": {
              "filter" : {"sub_doc" : { "$eq" : { "a": 5, "b": { "c": "v1", "d": true } } } }
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    public void withSizeOperator() {
      String json =
          """
          {
            "find": {
              "filter" : {"tags" : {"$size" : 6}}
            }
          }
          """;

      String expected =
          """
          {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void withSizeOperatorNoMatch() {
      String json =
          """
          {
            "find": {
              "filter" : {"tags" : {"$size" : 1}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    @Disabled("Different behavior with HCD 1.2, temporarily skipped")
    public void withSizeOperatorNotZero() {
      String json =
          """
                  {
                      "find": {
                          "filter": {
                              "$not": {
                                  "tags": {
                                      "$size": 0
                                  }
                              }
                          }
                      }
                  }
                      """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          // should have all the documents
          .body("data.documents", hasSize(6));
    }

    @Test
    public void withEqOperatorArray() {
      String json =
          """
          {
            "find": {
              "filter" : {"tags" : {"$eq" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true]}}
            }
          }
          """;

      String expected =
          """
          {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void withEqOperatorNestedArray() {
      String json =
          """
          {
            "find": {
              "filter" : {"nestedArray" : {"$eq" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}}
            }
          }
          """;

      String expected =
          """
          {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void withEqOperatorArrayNoMatch() {
      String json =
          """
          {
            "find": {
              "filter" : {"tags" : {"$eq" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1]}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    public void withEqOperatorNestedArrayNoMatch() {
      String json =
          """
          {
            "find": {
              "filter" : {"nestedArray" : {"$eq" : [["tag1", "tag2"], ["tag1234567890123456789012345", null], ["abc"]]}}
            }
          }
          """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    public void byBooleanColumn() {
      String json =
          """
          {
            "find": {
              "filter" : {"active_user" : true}
            }
          }
          """;

      String expected =
          """
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    @Order(2)
    public void byDateColumn() {
      String json =
          """
          {
            "find": {
              "filter" : {"date" : {"$date": 1672531200000}}
            }
          }
          """;

      String expected =
          """
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void simpleOr() {
      String json =
          """
                {
                    "find": {
                        "filter": {
                            "$or": [
                                {"username" : "user1"},
                                {"username" : "user2"}
                            ]
                        }
                    }
                }

              """;

      String expected1 =
          """
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """;
      String expected2 =
          """
                  {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                  """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(2))
          .body("data.documents", containsInAnyOrder(jsonEquals(expected1), jsonEquals(expected2)));
    }

    @Test
    public void nestedAndOr() {
      String json =
          """
            {
                "find": {
                    "filter": {
                        "$and": [
                            {
                                "$or": [
                                    {
                                        "username": "user3"
                                    },
                                    {
                                        "subdoc.id": {
                                            "$eq": "abc"
                                        }
                                    }
                                ]
                            },
                            {
                                "$or": [
                                    {
                                        "username": "user2"
                                    },
                                    {
                                        "subdoc.id": {
                                            "$eq": "xyz"
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                }
            }

              """;

      String expected =
          """
                  {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                  """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void OrWithIdIn() {
      String json =
          """
                      {
                          "find": {
                              "filter": {
                                  "$or": [
                                      {
                                          "username": "user1"
                                      },
                                      {
                                          "username": {
                                              "$in": [
                                                  "user2",
                                                  "user3"
                                              ]
                                          }
                                      }
                                  ]
                              }
                          }
                      }

                  """;

      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(3));
    }

    @Test
    void dollarOperatorInFilterPathExpression() {
      String json =
          """
              { "find": { "filter" : {"$gt" : {"test" : 5}} } }
              """;
      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_INVALID_EXPRESSION"))
          .body(
              "errors[0].message",
              containsString("filter expression path ('$gt') cannot start with '$'"));
    }

    @Test
    void invalidDollarOperatorPathExpression() {
      String json =
          """
                  { "find": { "filter" : {"address" : {"city" : {"$eq" : "Beijing"}}}}}
                  """;
      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_INVALID_EXPRESSION"))
          .body("errors[0].message", endsWith("Invalid use of $eq operator"));
    }

    @Test
    public void exceedMaxFieldInFilter() {
      // Max allowed 64, so fail with 65
      String json = createJsonStringWithNFilterFields(65);
      givenHeadersPostJsonThenOk(json)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("FILTER_FIELDS_LIMIT_VIOLATION"))
          .body(
              "errors[0].message",
              containsString(
                  "Filter has 65 fields, exceeds maximum allowed ("
                      + OperationsConfig.DEFAULT_MAX_FILTER_SIZE
                      + ")"));
    }

    private static String createJsonStringWithNFilterFields(int numberOfFields) {
      StringBuilder sb = new StringBuilder();

      sb.append("{\n");
      sb.append("  \"find\": {\n");
      sb.append("    \"filter\": {\n");

      for (int i = 1; i <= numberOfFields; i++) {
        sb.append("      \"name").append(i).append("\": \"").append(i).append("\"");
        if (i < numberOfFields) {
          sb.append(",\n");
        } else {
          sb.append("\n");
        }
      }

      sb.append("    }\n");
      sb.append("  }\n");
      sb.append("}");

      return sb.toString();
    }

    @Test
    public void NeText() {
      String json =
          """
              {
                "find": {
                  "filter" : {"username" : {"$ne" : "user1"}}
                }
              }
              """;
      String expected2 =
          """
                      {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                              """;
      String expected3 =
          """
                      {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
                      """;

      String expected4 =
          """
                      {
                        "_id": "doc4",
                        "username":"user4",
                        "indexedObject" : { "0": "value_0", "1": "value_1" }
                      }
                      """;
      String expected5 =
          """
                      {
                        "_id": "doc5",
                        "username": "user5",
                        "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
                      }
                      """;
      String expected6 =
          """
                                  {
                                    "_id": {"$date": 6},
                                    "user-name": "user6"
                                  }
                                  """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(5))
          .body(
              "data.documents",
              containsInAnyOrder(
                  jsonEquals(expected2),
                  jsonEquals(expected3),
                  jsonEquals(expected4),
                  jsonEquals(expected5),
                  jsonEquals(expected6)));
    }

    @Test
    public void NeNumber() {
      String json =
          """
              {
                "find": {
                  "filter" : {"age" : {"$ne" : 20}}
                }
              }
              """;
      String expected2 =
          """
                      {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                              """;
      String expected3 =
          """
                      {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
                      """;

      String expected4 =
          """
                      {
                        "_id": "doc4",
                        "username":"user4",
                        "indexedObject" : { "0": "value_0", "1": "value_1" }
                      }
                      """;
      String expected5 =
          """
                      {
                        "_id": "doc5",
                        "username": "user5",
                        "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
                      }
                      """;
      String expected6 =
          """
                                  {
                                    "_id": {"$date": 6},
                                    "user-name": "user6"
                                  }
                                  """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(5))
          .body(
              "data.documents",
              containsInAnyOrder(
                  jsonEquals(expected2),
                  jsonEquals(expected3),
                  jsonEquals(expected4),
                  jsonEquals(expected5),
                  jsonEquals(expected6)));
    }

    @Test
    public void NeBool() {
      String json =
          """
              {
                "find": {
                  "filter" : {"active_user" : {"$ne" : true}}
                }
              }
              """;
      String expected2 =
          """
                      {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                              """;
      String expected3 =
          """
                      {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
                      """;

      String expected4 =
          """
                      {
                        "_id": "doc4",
                        "username":"user4",
                        "indexedObject" : { "0": "value_0", "1": "value_1" }
                      }
                      """;
      String expected5 =
          """
                      {
                        "_id": "doc5",
                        "username": "user5",
                        "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
                      }
                      """;
      String expected6 =
          """
                                  {
                                    "_id": {"$date": 6},
                                    "user-name": "user6"
                                  }
                                  """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(5))
          .body(
              "data.documents",
              containsInAnyOrder(
                  jsonEquals(expected2),
                  jsonEquals(expected3),
                  jsonEquals(expected4),
                  jsonEquals(expected5),
                  jsonEquals(expected6)));
    }

    @Test
    public void NeNull() {
      String json =
          """
              {
                "find": {
                  "filter" : {"null_column" : {"$ne" : null}}
                }
              }
              """;
      String expected2 =
          """
                      {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                              """;
      String expected3 =
          """
                      {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
                      """;

      String expected4 =
          """
                      {
                        "_id": "doc4",
                        "username":"user4",
                        "indexedObject" : { "0": "value_0", "1": "value_1" }
                      }
                      """;
      String expected5 =
          """
                      {
                        "_id": "doc5",
                        "username": "user5",
                        "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
                      }
                      """;
      String expected6 =
          """
                                  {
                                    "_id": {"$date": 6},
                                    "user-name": "user6"
                                  }
                                  """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(5))
          .body(
              "data.documents",
              containsInAnyOrder(
                  jsonEquals(expected2),
                  jsonEquals(expected3),
                  jsonEquals(expected4),
                  jsonEquals(expected5),
                  jsonEquals(expected6)));
    }

    @Test
    public void NeDate() {
      String json =
          """
              {
                "find": {
                  "filter" : {"date" : {"$ne" : {"$date" : 1672531200000}}}
                }
              }
              """;
      String expected2 =
          """
                      {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                              """;
      String expected3 =
          """
                      {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
                      """;

      String expected4 =
          """
                      {
                        "_id": "doc4",
                        "username":"user4",
                        "indexedObject" : { "0": "value_0", "1": "value_1" }
                      }
                      """;
      String expected5 =
          """
                      {
                        "_id": "doc5",
                        "username": "user5",
                        "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
                      }
                      """;
      String expected6 =
          """
                                  {
                                    "_id": {"$date": 6},
                                    "user-name": "user6"
                                  }
                                  """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(5))
          .body(
              "data.documents",
              containsInAnyOrder(
                  jsonEquals(expected2),
                  jsonEquals(expected3),
                  jsonEquals(expected4),
                  jsonEquals(expected5),
                  jsonEquals(expected6)));
    }

    @Test
    public void NeSubdoc() {
      String json =
          """
              {
                "find": {
                  "filter" : {"subdoc" : {"$ne" : {"id":"abc"}}}
                }
              }
              """;
      String expected1 =
          """
                        {
                          "_id": "doc1",
                          "username": "user1",
                          "active_user" : true,
                          "date" : {"$date": 1672531200000},
                          "age" : 20,
                          "null_column": null
                      }
                              """;

      String expected3 =
          """
                      {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
                      """;

      String expected4 =
          """
                      {
                        "_id": "doc4",
                        "username":"user4",
                        "indexedObject" : { "0": "value_0", "1": "value_1" }
                      }
                      """;
      String expected5 =
          """
                      {
                        "_id": "doc5",
                        "username": "user5",
                        "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
                      }
                      """;
      String expected6 =
          """
                                  {
                                    "_id": {"$date": 6},
                                    "user-name": "user6"
                                  }
                                  """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(5))
          .body(
              "data.documents",
              containsInAnyOrder(
                  jsonEquals(expected1),
                  jsonEquals(expected3),
                  jsonEquals(expected4),
                  jsonEquals(expected5),
                  jsonEquals(expected6)));
    }

    @Test
    public void simpleNot() {
      String json =
          """
              {
                "find": {
                  "filter" : {
                    "$not": {"username": "user1"}
                  }
                }
              }
              """;
      String expected2 =
          """
                              {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                                      """;
      String expected3 =
          """
                              {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
                              """;

      String expected4 =
          """
                              {
                                "_id": "doc4",
                                "username":"user4",
                                "indexedObject" : { "0": "value_0", "1": "value_1" }
                              }
                              """;
      String expected5 =
          """
                              {
                                "_id": "doc5",
                                "username": "user5",
                                "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
                              }
                              """;
      String expected6 =
          """
                                          {
                                            "_id": {"$date": 6},
                                            "user-name": "user6"
                                          }
                                          """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(5))
          .body(
              "data.documents",
              containsInAnyOrder(
                  jsonEquals(expected2),
                  jsonEquals(expected3),
                  jsonEquals(expected4),
                  jsonEquals(expected5),
                  jsonEquals(expected6)));
    }

    @Test
    public void nestedNotPushDown() {
      String json =
          """
                {
                    "find": {
                        "filter": {
                            "$not":
                            {
                              "$and": [
                                {
                                    "$or": [
                                        {
                                            "username": "user3"
                                        },
                                        {
                                            "subdoc.id": {
                                                "$eq": "abc"
                                            }
                                        }
                                    ]
                                },
                                {
                                    "$or": [
                                        {
                                            "username": "user2"
                                        },
                                        {
                                            "subdoc.id": {
                                                "$eq": "xyz"
                                            }
                                        }
                                    ]
                                }
                              ]
                            }
                        }
                    }
                }
                  """;

      String expected =
          """
                      {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                      """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", not(containsInAnyOrder(expected)))
          .body("data.documents", hasSize(5));
    }

    @Test
    public void multipleNotCancelling() {
      String json =
          """
          {
            "find": {
              "filter" : {
                "$not":{ "$not" : {"username" : "user1"} }
              }
            }
          }
          """;

      String expected =
          """
                              {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                              """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body("data.documents", containsInAnyOrder(jsonEquals(expected)));
    }

    @Test
    public void withNotAllOperator() {
      String json =
          """
              {
                "find": {
                  "filter" : {"$not" : {"tags" : {"$all" : ["tag1", "tag2"]}} }
                }
              }
              """;

      String expected =
          """
              {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
              """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", not(containsInAnyOrder(expected)))
          .body("data.documents", hasSize(5));
    }

    @Test
    public void rangeWithNot() {
      String json =
          """
                  {
                    "find": {
                      "filter" : {
                        "$not": {"age": {"$gte" : 21}}
                      }
                    }
                  }
                  """;
      String expected =
          """
                              {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                              """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body("data.documents", containsInAnyOrder(jsonEquals(expected)));
    }

    @Test
    public void notWithComparisonExpressionAndLogicalExpression() {
      // exclude all the username by using a combination of logicalExpression and
      // comparisonExpression
      String json =
          """
                        {
                            "find": {
                                "filter": {
                                    "$not": {
                                        "$or": [
                                            {
                                                "username": "user2"
                                            },
                                            {
                                                "username": "user3"
                                            },
                                            {
                                                "username": "user4"
                                            },
                                            {
                                                "username": "user5"
                                            },
                                            {
                                                "user-name": "user6"
                                            }
                                        ],
                                        "username": {"$ne": "user1"}
                                    }
                                }
                            }
                        }
                      """;

      String expected =
          """
                                  {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                                  """;
      givenHeadersPostJsonThenOkNoErrors(json)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body("data.documents", containsInAnyOrder(jsonEquals(expected)));
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(2)
  class FindWithProjection {
    @Test
    @Order(1)
    public void insertData() {
      insert(
          """
                            {
                              "insertOne": {
                                "document": {
                                  "_id": "doc6",
                                  "price.usd": 5
                                }
                              }
                            }
                          """);
    }

    @Test
    public void byIdWithProjection() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                          {
                            "find": {
                              "filter" : {"_id" : "doc1"},
                              "projection": { "_id":0, "username":1 }
                            }
                          }
                          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals("{\"username\":\"user1\"}"))
          .body("data.documents", hasSize(1));
    }

    // For [json-api#634]: empty Object as Projection should work same as missing one,
    // that is, include everything
    @Test
    public void byIdEmptyProjection() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                              {
                                "find": {
                                  "filter" : {"_id" : "doc1"},
                                  "projection": { }
                                }
                              }
                              """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                                  {
                                      "_id": "doc1",
                                      "username": "user1",
                                      "active_user" : true,
                                      "date" : {"$date": 1672531200000},
                                      "age" : 20,
                                      "null_column": null
                                  }
                                  """));
    }

    @Test
    public void byIdIncludeAllProjection() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                                      {
                                        "find": {
                                          "filter" : {"_id" : "doc5"},
                                          "projection": { "*": 1 }
                                        }
                                      }
                                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                                    {
                                        "_id": "doc5",
                                        "username": "user5",
                                        "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
                                    }
                                    """));
    }

    @Test
    public void byIdExcludeAllProjection() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                                      {
                                        "find": {
                                          "filter" : {"_id" : "doc5"},
                                          "projection": { "*": 0 }
                                        }
                                      }
                                      """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body("data.documents[0]", jsonEquals("{}"));
    }

    @Test
    public void withAmpersandEscape() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                  {
                    "find": {
                        "filter" : {"_id" : "doc6"},
                        "projection" : {
                            "price&.usd": 1
                        }
                    }
                  }
                  """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                 {
                     "_id": "doc6",
                     "price.usd": 5
                 }
                 """));
    }

    @Test
    public void failWithAmpersandEscapeAtTheEnd() {
      givenHeadersPostJsonThenOk(
              """
                    {
                        "find": {
                            "projection" : {
                                "price&": 1
                            }
                        }
                    }
                    """)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              containsString(
                  "Unsupported projection parameter: projection path ('price&') is not a valid path."))
          .body("errors[0].errorCode", is("UNSUPPORTED_PROJECTION_PARAM"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failWithAmpersandEscapeNotFollowedByAmpersandOrDot() {
      givenHeadersPostJsonThenOk(
              """
                        {
                            "find": {
                                "projection" : {
                                    "price&abc": 1
                                }
                            }
                        }
                        """)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              containsString(
                  "Unsupported projection parameter: projection path ('price&abc') is not a valid path."))
          .body("errors[0].errorCode", is("UNSUPPORTED_PROJECTION_PARAM"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void failWithEmptySegment() {
      givenHeadersPostJsonThenOk(
              """
                        {
                            "find": {
                                "projection" : {
                                    "foo..bar": 1
                                }
                            }
                        }
                        """)
          .body("$", responseIsError())
          .body(
              "errors[0].message",
              containsString(
                  "Unsupported projection parameter: projection path ('foo..bar') is not a valid path."))
          .body("errors[0].errorCode", is("UNSUPPORTED_PROJECTION_PARAM"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }
  }

  @Nested
  @Order(2)
  class Metrics {

    @Test
    public void checkMetrics() {
      FindIntegrationTest.checkMetrics("FindCommand");
      FindIntegrationTest.checkDriverMetricsTenantId();
      FindIntegrationTest.checkIndexUsageMetrics("FindCommand", false);
    }
  }
}
