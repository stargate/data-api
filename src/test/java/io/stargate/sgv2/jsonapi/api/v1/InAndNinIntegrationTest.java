package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.stream.Stream;
import net.javacrumbs.jsonunit.ConfigurableJsonMatcher;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class InAndNinIntegrationTest extends AbstractCollectionIntegrationTestBase {

  private void insert(String json) {
    givenHeadersPostJsonThenOkNoErrors(json).body("$", responseIsWriteSuccess());
  }

  private ConfigurableJsonMatcher[] getJsonEquals(int... docs) {
    String expected1 =
        """
                    {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                    """;
    String expected2 =
        """
                    { "_id": "doc2", "username": "user2", "subdoc" : { "id" : "abc" }, "array" : [ "value1" ] }
                    """;
    String expected3 =
        """
                    { "_id": "doc3", "username": "user3", "tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]] }
                            """;
    String expected4 =
        """
                    { "_id": "doc4", "username" : "user4", "indexedObject" : { "0": "value_0", "1": "value_1" } }
                            """;
    String expected5 =
        """
                    { "_id": "doc5", "username": "user5", "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } } }
                            """;
    String expected6 =
        """
                    { "_id": {"$date": 6}, "username": "user6" }
                            """;

    ConfigurableJsonMatcher[] result = new ConfigurableJsonMatcher[docs.length];
    for (int i = 0; i < docs.length; i++) {
      int doc = docs[i];
      if (doc == 1) {
        result[i] = (jsonEquals(expected1));
      } else if (doc == 2) {
        result[i] = (jsonEquals(expected2));
      } else if (doc == 3) {
        result[i] = (jsonEquals(expected3));
      } else if (doc == 4) {
        result[i] = (jsonEquals(expected4));
      } else if (doc == 5) {
        result[i] = (jsonEquals(expected5));
      } else if (doc == 6) {
        result[i] = (jsonEquals(expected6));
      }
    }
    return result;
  }

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
                    "username": "user6"
                  }
                }
              }
            """);
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(2)
  class In {

    @Test
    public void inCondition() {
      // findOne resolves any one of the resolved documents. So the order of the documents in the
      // $in clause is not guaranteed.
      givenHeadersPostJsonThenOkNoErrors(
              """
                          {
                            "find": {
                              "filter" : {"_id" : {"$in": ["doc1", "doc4"]}}
                            }
                          }
                          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(2))
          .body(
              "data.documents",
              containsInAnyOrder(
                  jsonEquals(
                      """
                  {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                  """),
                  jsonEquals(
                      """
                  {"_id":"doc4", "username":"user4", "indexedObject":{"0":"value_0","1":"value_1"}}
                  """)));
    }

    private static Stream<Arguments> IN_FOR_ID_WITH_LIMIT() {
      return Stream.of(
          // filter, limit, expected
          Arguments.of("{\"_id\" : {\"$in\": [\"doc1\", \"doc2\", \"doc3\"]}}", 2, 2),
          Arguments.of("{\"_id\" : {\"$in\": [\"doc1\", \"doc2\", \"doc3\"]}}", 1, 1),
          Arguments.of("{\"_id\" : {\"$in\": [\"doc1\", \"doc2\", \"doc3\"]}}", 5, 3),
          Arguments.of("{\"_id\" : {\"$in\": [\"doc1\", \"doc2\", \"doc3\"]}}", 3, 3));
    }

    @ParameterizedTest
    @MethodSource("IN_FOR_ID_WITH_LIMIT")
    public void inForIdWithLimit(String filter, int limit, int expected) {
      givenHeadersPostJsonThenOkNoErrors(
                  """
              {
                "find": {
                  "filter" : %s,
                  "options": {"limit": %s}
                }
              }
              """
                  .formatted(filter, limit))
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(expected));
    }

    @Test
    public void inConditionWithSubDoc() {
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "find": {
            "filter" : {"sub_doc" : {"$in" : [{ "a": 5, "b": { "c": "v1", "d": false }}]} }
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
    public void inConditionWithArray() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                              {
                                "find": {
                                  "filter" : {"array" : {"$in" : [["value1"]] } }
                                }
                              }
                  """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                          {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                          """));
    }

    @Test
    public void inConditionWithOtherCondition() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                          {
                            "find": {
                              "filter" : {"_id" : {"$in": ["doc1", "doc4"]}, "username" : "user1" }
                            }
                          }
                          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """));
    }

    @Test
    public void idInConditionEmptyArray() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                          {
                            "find": {
                              "filter" : {"_id" : {"$in": []}}
                            }
                          }
                          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    public void nonIDInConditionEmptyArray() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                            {
                              "find": {
                                  "filter" : {
                                       "username" : {"$in" : []}
                                  }
                                }
                            }
                          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    public void nonIDInConditionEmptyArrayAnd() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                            {
                              "find": {
                                  "filter" : {
                                    "$and": [
                                        {
                                            "age": {
                                                "$in": []
                                            }
                                        },
                                        {
                                            "username": "user1"
                                        }
                                    ]
                                  }
                                }
                            }
                          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    public void nonIDInConditionEmptyArrayOr() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                            {
                              "find": {
                                  "filter" : {
                                    "$or": [
                                        {
                                            "age": {
                                                "$in": []
                                            }
                                        },
                                        {
                                            "username": "user1"
                                        }
                                    ]
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
          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
          """));
    }

    @Test
    public void inOperatorEmptyArrayWithAdditionalFilters() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                          {
                            "find": {
                              "filter" : {"username": "user1", "_id" : {"$in": []}}
                            }
                          }
                          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    public void inConditionNonArrayArray() {
      givenHeadersPostJsonThenOk(
              """
                          {
                            "find": {
                              "filter" : {"_id" : {"$in": true}}
                            }
                          }
                          """)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("FILTER_INVALID_EXPRESSION"))
          .body("errors[0].message", containsString("'$in' operator must have `Array`"));
    }

    @Test
    public void inConditionNonIdField() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                          {
                            "find": {
                                "filter" : {
                                     "username" : {"$in" : ["user1", "user10"]}
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
          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
          """));
    }

    @Test
    public void inConditionNonIdFieldMulti() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                          {
                            "find": {
                                "filter" : {
                                     "username" : {"$in" : ["user1", "user4"]}
                                }
                              }
                          }
                          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(2))
          .body("data.documents", containsInAnyOrder(getJsonEquals(1, 4)));
    }

    @Test
    public void inConditionNonIdFieldIdField() {
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "find": {
                  "filter" : {
                       "username" : {"$in" : ["user1", "user10"]},
                       "_id" : {"$in" : ["doc1", "???"]}
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
          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
          """));
    }

    @Test
    public void inConditionNonIdFieldIdFieldSort() {
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "find": {
                  "filter" : {
                       "username" : {"$in" : ["user1", "user10"]},
                       "_id" : {"$in" : ["doc1", "???"]}
                  },
                  "sort": { "username": -1 }
                }
            }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
          """));
    }

    @Test
    public void inConditionWithDuplicateValues() {
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "find": {
                  "filter" : {
                       "username" : {"$in" : ["user1", "user1"]},
                       "_id" : {"$in" : ["doc1", "???"]}
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
          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
          """));
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(3)
  class Nin {

    @Test
    public void nonIdSimpleNinCondition() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"username" : {"$nin": ["user2", "user3","user4","user5","user6"]}}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
        {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
        """));
    }

    @Test
    public void ninConditionWithSubDoc() {
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "find": {
            "filter" : {"sub_doc" : {"$nin": [{ "a": 5, "b": { "c": "v1", "d": false } }]}}
          }
        }
        """)
          .body("$", responseIsFindSuccess())
          // except doc 5
          .body("data.documents", hasSize(5))
          .body("data.documents", notNullValue())
          .body("data.documents", containsInAnyOrder(getJsonEquals(1, 2, 3, 4, 6)));
    }

    @Test
    public void ninConditionWithArray() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                              {
                                "find": {
                                  "filter" : {"array" : {"$nin" : [["value1"]] } }
                                }
                              }
                  """)
          .body("$", responseIsFindSuccess())
          // except doc 2
          .body("data.documents", hasSize(5))
          .body("data.documents", notNullValue())
          .body("data.documents", containsInAnyOrder(getJsonEquals(1, 3, 4, 5, 6)));
    }

    @Test
    public void nonIdNinEmptyArray() {
      // should find everything
      givenHeadersPostJsonThenOkNoErrors(
              """
                          {
                            "find": {
                              "filter" : {"username" : {"$nin": []}}
                            }
                          }
                          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(6))
          .body("data.documents", containsInAnyOrder(getJsonEquals(1, 2, 3, 4, 5, 6)));
    }

    @Test
    public void idNinEmptyArray() {
      // should find everything
      givenHeadersPostJsonThenOkNoErrors(
              """
                              {
                                "find": {
                                  "filter" : {"_id" : {"$nin": []}}
                                }
                              }
                              """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(6))
          .body("data.documents", containsInAnyOrder(getJsonEquals(1, 2, 3, 4, 5, 6)));
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(4)
  class Combination {

    @Test
    public void nonIdInEmptyAndNonIdNinEmptyAnd() {
      // should find nothing
      givenHeadersPostJsonThenOkNoErrors(
              """
                          {
                            "find": {
                              "filter" : {"username" : {"$in": []}, "age": {"$nin" : []}}
                            }
                          }
                          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    public void nonIdInEmptyOrNonIdNinEmptyOr() {
      // should find everything
      givenHeadersPostJsonThenOkNoErrors(
              """
                          {
                            "find": {
                              "filter" :{
                                "$or" :
                                [
                                {"username" : {"$in": []}},
                                {"age": {"$nin" : []}}
                                ]
                              }
                            }
                          }
                          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(6))
          .body("data.documents", containsInAnyOrder(getJsonEquals(1, 2, 3, 4, 5, 6)));
    }

    @Test
    public void nonIdInEmptyAndIdNinEmptyAnd() {
      // should find nothing
      givenHeadersPostJsonThenOkNoErrors(
              """
                              {
                                "find": {
                                  "filter" : {"username" : {"$in": []}, "_id": {"$nin" : []}}
                                }
                              }
                              """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }
  }
}
