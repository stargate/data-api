package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

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
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

/** Tests for update operation for Collection Documents with Lexical (BM25) sort. */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class UpdateOneWithLexicalCollectionIntegrationTest
    extends AbstractCollectionIntegrationTestBase {
  protected UpdateOneWithLexicalCollectionIntegrationTest() {
    super("col_lexical_operations_");
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(1)
  class SetupCollection {
    @Test
    void createCollectionWithLexical() {
      // Create a Collection with default Lexical settings
      createComplexCollection(
              """
                            {
                              "name": "%s",
                              "options" : {
                                "lexical": {
                                  "enabled": true,
                                  "analyzer": "standard"
                                }
                              }
                            }
                            """
              .formatted(collectionName));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(2)
  class SetOperation {
    @Test
    @Order(1)
    public void setupDocuments() {
      insertDoc(lexicalDoc(1, "monkey banana"));
      insertDoc(lexicalDoc(2, "monkey"));
      insertDoc(lexicalDoc(3, null));

      // and then verify that we can read them back as expected. First, all:
      givenHeadersPostJsonThenOkNoErrors("{ \"find\": {} }")
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(3));

      // and then by sort
      givenHeadersPostJsonThenOkNoErrors(
              """
                                {
                                  "find": {
                                    "projection": {"*": 1},
                                    "sort" : {"$lexical": "monkey" }
                                  }
                                }
                                """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents",
              jsonEquals(
                  """
                [{"_id": "lexical-2", "$lexical": "monkey"},
                {"_id": "lexical-1", "$lexical": "monkey banana"}]
            """));
    }

    // First change: add $lexical to entry that didn't have it -- but one that
    // has lower score (longer text)
    @Test
    @Order(2)
    public void testOverwrite1_EmptyWithNonEmpty() {
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "updateOne": {
            "filter" : {"_id": "lexical-3"},
            "update" : {"$set" : {"$lexical": "monkey bread is so tasty"}}
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
                                    "projection": {"*": 1},
                                    "sort" : {"$lexical": "monkey" }
                                  }
                                }
                                """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents",
              jsonEquals(
                  """
                [{"_id": "lexical-2", "$lexical": "monkey"},
                {"_id": "lexical-1", "$lexical": "monkey banana"},
                {"_id": "lexical-3", "$lexical": "monkey bread is so tasty"}]
            """));
    }

    // Second change: overwrite existing value with a different one -- one with
    // higher matching ("more monkeys")
    @Test
    @Order(3)
    public void testOverwrite2_NonEmptyWithDiffValue() {
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "updateOne": {
            "filter" : {"_id": "lexical-1"},
            "update" : {"$set" : {"$lexical": "monkey monkey monkey"}}
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
                    "projection": {"*": 1},
                    "sort" : {"$lexical": "monkey" }
                  }
                }
                """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents",
              jsonEquals(
                  """
                            [{"_id": "lexical-1", "$lexical": "monkey monkey monkey"},
                            {"_id": "lexical-2", "$lexical": "monkey"},
                            {"_id": "lexical-3", "$lexical": "monkey bread is so tasty" }]
                        """));
    }

    // Third change: remove $lexical from existing value, to drop from results
    @Test
    @Order(4)
    public void testOverwrite3_NonEmptyWithNull() {
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "updateOne": {
            "filter" : {"_id": "lexical-2"},
            "update" : {"$set" : {"$lexical": null}}
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
                                    "projection": {"*": 1},
                                    "sort" : {"$lexical": "monkey" }
                                  }
                                }
                                """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents",
              jsonEquals(
                  """
                                [{"_id": "lexical-1", "$lexical": "monkey monkey monkey"},
                                {"_id": "lexical-3", "$lexical": "monkey bread is so tasty" }]
                            """));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(3)
  class UnsetOperation {
    @Test
    @Order(1)
    public void setupDocuments() {
      deleteAllDocuments();

      insertDoc(lexicalDoc(1, "biking"));
      insertDoc(lexicalDoc(2, "banana bread is good"));
      insertDoc(lexicalDoc(3, "banana"));

      // and then verify that we can read them back as expected. First, all:
      givenHeadersPostJsonThenOkNoErrors("{ \"find\": {} }")
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(3));

      // and then by sort
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "find": {
                    "projection": {"*": 1},
                    "sort" : {"$lexical": "banana" }
                  }
                }
                """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents",
              jsonEquals(
                  """
                            [{"_id": "lexical-3", "$lexical": "banana"},
                            {"_id": "lexical-2", "$lexical": "banana bread is good"}],
                        """));
    }

    @Test
    @Order(2)
    public void unsetOneToRemoveFromResults() {
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "updateOne": {
            "filter" : {"_id": "lexical-2"},
            "update" : {"$unset" : {"$lexical": 1}}
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
                                    "projection": {"*": 1},
                                    "sort" : {"$lexical": "banana" }
                                  }
                                }
                                """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents",
              jsonEquals(
                  """
                            [{"_id": "lexical-3", "$lexical": "banana"}]
                        """));
    }
  }

  /**
   * Tests to ensure only allowed operations for $lexical are $set and $unset, rest are not allowed.
   * Will not do exhaustive coverage, just spot checks.
   */
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(10)
  class NotAllowedOperations {
    @Test
    @Order(1)
    public void setupDocuments() {
      deleteAllDocuments();
      insertDoc(lexicalDoc(10, "key words"));
    }

    @Test
    @Order(2)
    public void failForAddToSet() {
      failUpdateFor("$addToSet", "{\"$addToSet\" : {\"$lexical\": \"token\" }}");
    }

    @Test
    @Order(3)
    public void failForPush() {
      failUpdateFor("$push", "{\"$push\" : {\"$lexical\": \"token\" }}");
    }

    @Test
    @Order(4)
    public void failForPop() {
      failUpdateFor("$pop", "{\"$pop\" : {\"$lexical\": 1 }}");
    }

    @Test
    @Order(5)
    public void failForRename() {
      failUpdateFor("$rename", "{\"$rename\" : {\"$lexical\": \"stuff\" }}");
    }

    private void failUpdateFor(String opName, String updateOperation) {
      givenHeadersPostJsonThenOk(updateQuery(updateOperation))
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_OPERATOR_FOR_LEXICAL"))
          .body("errors[0].exceptionClass", is("UpdateException"))
          .body("errors[0].title", is("Update operator cannot be used on $lexical field"))
          .body("errors[0].message", containsString("command used the update operator: " + opName));
    }

    private String updateQuery(String updateOperation) {
      return
          """
              {
                "updateOne": {
                  "filter" : {"_id" : "lexical-10"},
                  "update" : %s
                }
              }
              """
          .formatted(updateOperation);
    }
  }

  static String lexicalDoc(int id, String keywords) {
    String keywordsStr = (keywords == null) ? "null" : String.format("\"%s\"", keywords);
    return
        """
                    {
                      "_id": "lexical-%d",
                      "$lexical": %s
                    }
                """
        .formatted(id, keywordsStr);
  }
}
