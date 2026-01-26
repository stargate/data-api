package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class UpdateOneIntegrationTest extends AbstractCollectionIntegrationTestBase {

  @Nested
  @Order(1)
  class UpdateOneWithSet {
    @Test
    public void byIdAndSet() {
      insertDoc(
          """
              {
                "_id": "update_doc1",
                "username": "update_user3",
                "date_col": {"$date" : 1672531200000},
                "active_user" : true
              }
          """);

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc1"},
              "update" : {"$set" : {"active_user": false}}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc1"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                  {
                    "_id":"update_doc1",
                    "username":"update_user3",
                    "date_col": {"$date" : 1672531200000},
                    "active_user":false
                  }
                  """));
    }

    @Test
    public void emptyOptionsAllowed() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc1"},
              "update" : {"$set" : {"active_user": false}},
              "options": {}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0));
    }

    @Test
    public void byIdUpsert() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateOne": {
              "filter" : {"_id" : "afterDoc6"},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"upsert" : true}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.upsertedId", is("afterDoc6"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "afterDoc6"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"afterDoc6",
            "active_user":false
          }
          """));
    }

    @Test
    public void byIdUpsertSetOnInsert() {
      givenHeadersPostJsonThenOkNoErrors(
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
              """)
          .body("$", responseIsStatusOnly())
          .body("status.upsertedId", is("upsertSetOnInsert1"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "find": {
                  "filter" : {"_id" : "upsertSetOnInsert1"}
                }
              }
              """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
              {
                "_id": "upsertSetOnInsert1",
                "active_user": true
              }
              """));
    }

    @Test
    public void byColumnUpsert() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateOne": {
              "filter" : {"location" : "my_city"},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"upsert" : true}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.upsertedId", is(notNullValue()))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "find": {
                "filter" : {"location" : "my_city"}
              }
            }
            """)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", is(notNullValue()));
    }

    @Test
    public void byIdAndColumnUpsert() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateOne": {
              "filter" : {"_id" : "afterDoc7", "username" : "afterName7", "phone" : null},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"upsert" : true}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.upsertedId", is("afterDoc7"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "afterDoc7"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"afterDoc7",
            "username" : "afterName7",
            "phone" : null,
            "active_user":false
          }
          """));
    }

    @Test
    public void byColumnAndSet() {
      insertDoc(
          """
              {
                "_id": "update_doc2",
                "username": "update_user2"
              }
          """);

      insertDoc(
          """
              {
                "_id": "update_doc3",
                "username": "update_user2"
              }
          """);

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateOne": {
              "filter" : {"username" : "update_user2"},
              "update" : {"$set" : {"new_col": "new_val"}}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("status.moreData", is(nullValue()));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc2"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"update_doc2",
            "username":"update_user2",
            "new_col": "new_val"
          }
          """));
    }

    @Test
    public void byColumnWithSortAndSet() {
      insertDoc(
          """
            {
              "_id": "update_doc2",
              "username": "update_user2",
              "location": "my_city"
            }
        """);

      insertDoc(
          """
            {
              "_id": "update_doc3",
              "username": "update_user3",
              "location": "my_city"
            }
        """);

      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "updateOne": {
            "filter" : {"location": "my_city"},
            "update" : {"$set" : {"new_col": "new_val"}},
            "sort" : {"username" : 1}
          }
        }
        """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("status.moreData", is(nullValue()));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "find": {
            "filter" : {"_id" : "update_doc2"}
          }
        }
        """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
        {
          "_id":"update_doc2",
          "username":"update_user2",
              "location": "my_city",
          "new_col": "new_val"
        }
        """));
    }

    @Test
    public void byColumnAndSetArray() {
      insertDoc(
          """
              {
                "_id": "update_doc4",
                "username": "update_user4"
              }
          """);

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateOne": {
              "filter" : {"username" : "update_user4"},
              "update" : {"$set" : {"new_col": ["new_val", "new_val2"]}}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc4"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"update_doc4",
            "username":"update_user4",
            "new_col": ["new_val", "new_val2"]
          }
          """));
    }

    @Test
    public void byColumnAndSetSubDoc() {
      insertDoc(
          """
              {
                "_id": "update_doc5",
                "username": "update_user5"
              }
          """);

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateOne": {
              "filter" : {"username" : "update_user5"},
              "update" : {"$set" : {"new_col": {"sub_doc_col" : "new_val2"}}}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc5"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"update_doc5",
            "username":"update_user5",
            "new_col": {"sub_doc_col":"new_val2"}
          }
          """));
    }

    @Test
    public void withDotInPathName() {
      insertDoc(
          """
            {
              "_id": "doc_with_dot",
              "price.usd": 5
            }
          """);

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateOne": {
              "filter" : {"_id" : "doc_with_dot"},
              "update" : {"$set" : {"price&.usd": 6}}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "find": {
                  "filter" : {"_id" : "doc_with_dot"}
                }
              }
            """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                              {
                                "_id": "doc_with_dot",
                                "price.usd": 6
                              }
                              """));
    }

    @Test
    public void withEscapeInPathName() {
      insertDoc(
          """
                {
                "_id": "doc_with_escape",
                "price&usd": 5
                }
            """);

      givenHeadersPostJsonThenOkNoErrors(
              """
            {
                "updateOne": {
                "filter" : {"_id" : "doc_with_escape"},
                "update" : {"$set" : {"price&&usd": 6}}
                }
            }
            """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                    "find": {
                    "filter" : {"_id" : "doc_with_escape"}
                    }
                }
                """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                                {
                                    "_id": "doc_with_escape",
                                    "price&usd": 6
                                }
                                """));

      // fail if the path is not correctly escaped
      givenHeadersPostJsonThenOk(
              """
            {
                "updateOne": {
                "filter" : {"_id" : "doc_with_escape"},
                "update" : {"$set" : {"price&usd": 7}}
                }
            }
            """)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_OPERATION_PATH"))
          .body(
              "errors[0].message",
              containsString(
                  "Unsupported update operation path: update path ('price&usd') is not a valid path."));
    }
  }

  @Nested
  @Order(2)
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

      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "findOneAndUpdate": {
                  "filter" : {"_id" : "update_doc3"},
                  "update" : {"$unset" : {"unset_col": ""}}
                }
              }
              """)
          .body("$", responseIsFindAndSuccess())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "find": {
                  "filter" : {"_id" : "update_doc3"}
                }
              }
              """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
              {
                "_id":"update_doc3",
                "username":"update_user3"
              }
              """));
    }
  }

  @Nested
  @Order(3)
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
      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_pop"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
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
          """));
    }
  }

  @Nested
  @Order(4)
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

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_push"},
              "update" : {"$push" : {"array": 13, "subdoc.array": true }}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_push"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"update_doc_push",
            "array": [2, 13],
            "subdoc": { "array" : [ true ] }
          }
          """));
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

      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_push_each"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"update_doc_push_each",
            "nested" : { "array": [1, 2, 3] },
            "newArray": [true]
          }
          """));
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

      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_push_each_position"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                { "_id":"update_doc_push_each_position",
                  "array": [1, 2, 4, 5, 3],
                  "nested": {
                    "values" : [1, 2, 3]
                  }
                }
          """));
    }
  }

  @Nested
  @Order(5)
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

      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_inc"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"update_doc_inc",
            "number": 119,
            "newProp": 0.25,
            "numbers": {
              "values" : [ 10, 0.5 ]
            }
          }
          """));
    }
  }

  @Nested
  @Order(6)
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

      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_mul"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"update_doc_mul",
            "number": -48,
            "newProp": 0,
            "numbers": {
              "values" : [ 0.5, 0 ]
            }
          }
          """));
    }
  }

  @Nested
  @Order(7)
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

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_add_to_set"},
              "update" : {"$addToSet" : {"array": 3, "subdoc.array": "value" }}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_add_to_set"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"update_doc_add_to_set",
            "array": [2, 3],
            "subdoc" : { "array" : [ "value" ] }
          }
          """));
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

      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "updateOne": {
                  "filter" : {"_id" : "update_doc_add_to_set_unchanged"},
                  "update" : {"$addToSet" : {"array": 2 }}
                }
              }
              """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(0));

      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "find": {
                  "filter" : {"_id" : "update_doc_add_to_set_unchanged"}
                }
              }
              """)
          .body("$", responseIsFindSuccess())
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

      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_add_to_set_each"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"update_doc_add_to_set_each",
            "nested" : { "array": [2, 3, 1, 4] },
            "newArray": [true, false]
          }
          """));
    }
  }

  @Nested
  @Order(8)
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

      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_min"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
             "_id": "update_doc_min",
             "min": 1,
             "max": 25,
             "numbers": {
                "values": [ -9 ]
              }
           }
          """));
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

      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update: only "end" changed
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_min_text"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id": "update_doc_min_text",
            "start": "abc",
            "end": "fff"
          }
          """));
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

      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update: only "start" changed (numbers before strings), not
      // "end" (boolean after strings)
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_min_mixed"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id": "update_doc_min_mixed",
            "start": 123,
            "end": "xyz"
          }
          """));
    }
  }

  @Nested
  @Order(9)
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

      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_max"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id": "update_doc_max",
            "min": 2,
            "max": 99,
            "numbers": {
              "values": { "x":1, "y":3 }
            }
          }
          """));
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

      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update: only "start" changed
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_max_text"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id": "update_doc_max_text",
            "start": "fff",
            "end": "xyz"
          }
          """));
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

      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update: only "end" changed (booleans after Constants), not
      // "start" (numbers before Constants)
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "update_doc_max_mixed"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id": "update_doc_max_mixed",
            "start": "abc",
            "end": true
          }
          """));
    }
  }

  @Nested
  @Order(10)
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
      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "find": {
                  "filter" : {"_id" : "update_doc_rename"}
                }
              }
              """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                {
                  "_id": "update_doc_rename",
                  "sum": 1,
                  "nested": {
                    "x0": true
                  }
                }
              """));
    }
  }

  // Tests combining more than update operator, mostly for cross-validation
  @Nested
  @Order(11)
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

      givenHeadersPostJsonThenOkNoErrors(
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
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      // assert state after update: only "end" changed (booleans after Constants), not
      // "start" (numbers before Constants)
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "find": {
                "filter" : {"_id": "update_doc_mixed_set_unset"}
              }
            }
            """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id": "update_doc_mixed_set_unset",
            "nested": {
              "new": "b"
            }
          }
          """));
    }
  }

  @Nested
  @Order(12)
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

      // start all threads
      AtomicReferenceArray<Exception> exceptions = new AtomicReferenceArray<>(threads);
      for (int i = 0; i < threads; i++) {
        int index = i;
        new Thread(
                () -> {
                  try {
                    givenHeadersPostJsonThenOkNoErrors(
                            """
                        {
                          "updateOne": {
                            "filter" : {"_id" : "concurrent"},
                            "update" : {
                              "$inc" : {"count": 1}
                            }
                          }
                        }
                        """)
                        .body("$", responseIsStatusOnly())
                        .body("status.matchedCount", is(1))
                        .body("status.modifiedCount", is(1));
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
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "concurrent"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id": "concurrent",
            "count": 3
          }
          """));
    }
  }

  @Nested
  @Order(13)
  class ClientErrors {
    @Test
    public void invalidCommand() {
      givenHeadersPostJsonThenOk(
              """
          {
            "updateOne": {
              "filter" : {"_id" : "update_doc_max"}
            }
          }
          """)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("COMMAND_FIELD_VALUE_INVALID"))
          .body(
              "errors[0].message",
              startsWith(
                  "Command field 'command.updateClause' value `null` not valid: must not be null."));
    }

    @Test
    public void invalidSetAndUnsetPathConflict() {
      // Cannot modify entries that conflict (same path, or parent/child):
      givenHeadersPostJsonThenOk(
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
              """)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_OPERATION_PARAM"))
          .body(
              "errors[0].message",
              containsString(
                  "Update operator path conflict due to overlap: 'root.array' ($unset) vs 'root.array.1' ($set)"));
    }
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }

  @Nested
  @Order(14)
  class Metrics {
    @Test
    public void checkMetrics() {
      UpdateOneIntegrationTest.checkMetrics("UpdateOneCommand");
      UpdateOneIntegrationTest.checkDriverMetricsTenantId();
      UpdateOneIntegrationTest.checkIndexUsageMetrics("UpdateOneCommand", false);
    }
  }
}
