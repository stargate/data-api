package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

/**
 * Tests for the Lexical sort feature in the JSON API, with the following commands:
 *
 * <ul>
 *   <li>"find" and "findOne"
 *   <li>"findOneAndUpdate", "findOneAndReplace", "findOneAndDelete"
 *   <li>"updateOne" and "deleteOne"
 * </ul>
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindCollectionWithLexicalSortIntegrationTest
    extends AbstractCollectionIntegrationTestBase {
  static final String COLLECTION_WITH_LEXICAL =
      "coll_lexical_sort_" + RandomStringUtils.randomNumeric(16);

  static final String COLLECTION_WITHOUT_LEXICAL =
      "coll_no_lexical_sort_" + RandomStringUtils.randomNumeric(16);

  static final String DOC1_JSON = lexicalDoc(1, "monkey banana", "value1");
  static final String DOC2_JSON = lexicalDoc(2, "monkey", "value2");
  static final String DOC3_JSON = lexicalDoc(3, "biking fun", "value3");
  static final String DOC4_JSON = lexicalDoc(4, "banana bread with butter", "value4");
  static final String DOC5_JSON = lexicalDoc(5, "fun", "value5");

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(1)
  class Setup {
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
              .formatted(COLLECTION_WITH_LEXICAL));
      // And then insert 5 documents
      insertDoc(COLLECTION_WITH_LEXICAL, DOC1_JSON);
      insertDoc(COLLECTION_WITH_LEXICAL, DOC2_JSON);
      insertDoc(COLLECTION_WITH_LEXICAL, DOC3_JSON);
      insertDoc(COLLECTION_WITH_LEXICAL, DOC4_JSON);
      insertDoc(COLLECTION_WITH_LEXICAL, DOC5_JSON);
    }

    @Test
    void createCollectionWithoutLexical() {
      // Create a Collection with lexical feature disabled
      createComplexCollection(
              """
                      {
                        "name": "%s",
                        "options" : {
                          "lexical": {
                            "enabled": false
                          }
                        }
                      }
                      """
              .formatted(COLLECTION_WITHOUT_LEXICAL));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(2)
  class HappyCasesFindMany {
    @Test
    void findManyWithLexicalSort() {
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
                          {
                            "find": {
                              "sort" : {"$lexical": "banana" }
                            }
                          }
                          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(2))
          .body("data.documents[0]._id", is("lexical-1"))
          .body("data.documents[1]._id", is("lexical-4"));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(2)
  class HappyCasesFindOne {
    @Test
    void findOneWithLexicalSortBiking() {
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
                      {
                        "findOne": {
                          "projection": {"$lexical": 1 },
                          "sort" : {"$lexical": "biking" }
                        }
                      }
                      """)
          .body("$", responseIsFindSuccess())
          // Needs to get "lexical-3" with "biking fun"
          .body("data.document", jsonEquals(DOC3_JSON));
    }

    @Test
    void findOneWithLexicalSortMonkeyBananas() {
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
                          {
                            "findOne": {
                              "projection": {"$lexical": 1 },
                              "sort" : {"$lexical": "monkey banana" }
                            }
                          }
                          """)
          .body("$", responseIsFindSuccess())
          // Needs to get "lexical-1" with "monkey banana"
          .body("data.document", jsonEquals(DOC1_JSON));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(3)
  class FailingCasesFindMany {
    @Test
    void failIfLexicalDisabledForCollection() {
      givenHeadersPostJsonThenOk(
              keyspaceName,
              COLLECTION_WITHOUT_LEXICAL,
              """
                          {
                            "find": {
                              "sort" : {"$lexical": "banana" }
                            }
                          }
                          """)
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("LEXICAL_NOT_ENABLED_FOR_COLLECTION"))
          .body("errors[0].message", containsString("Lexical search is not enabled"));
    }

    @Test
    void failForBadLexicalSortValueType() {
      givenHeadersPostJsonThenOk(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
                          {
                            "find": {
                              "sort" : {"$lexical": -1 }
                            }
                          }
                          """)
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_SORT_CLAUSE"))
          .body(
              "errors[0].message",
              containsString("if sorting by '$lexical' value must be String, not Number"));
    }

    @Test
    void failForLexicalSortWithOtherExpressions() {
      givenHeadersPostJsonThenOk(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
                          {
                            "find": {
                              "sort" : {
                                 "a": 1,
                                 "$lexical": "bananas"
                               }
                            }
                          }
                          """)
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_SORT_CLAUSE"))
          .body(
              "errors[0].message",
              containsString("if sorting by '$lexical' no other sort expressions allowed"));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(10)
  class HappyCasesFindOneAndUpdate {
    @Test
    void findOneAndUpdateWithSort() {
      final String expectedAfterChange = lexicalDoc(1, "monkey banana", "value1-updated");
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
           {
             "findOneAndUpdate": {
               "sort": { "$lexical": "banana" },
               "update" : {"$set" : {"value": "value1-updated"}},
               "projection": {"$lexical": 1 },
               "options": {"returnDocument": "after"}
             }
           }
           """)
          .body("data.document", jsonEquals(expectedAfterChange))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));
      // Plus query to check that the document was updated
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
          {
            "findOne": {
              "filter" : {"_id" : "lexical-1"},
              "projection": {"*": 1 }
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(expectedAfterChange));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(11)
  class HappyCasesUpdateOne {
    @Test
    void updateOneWithSort() {
      final String expectedAfterChange = lexicalDoc(1, "monkey banana", "value1-updated-2");
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
           {
             "updateOne": {
               "sort": { "$lexical": "banana" },
               "update" : {"$set" : {"value": "value1-updated-2"}}
             }
           }
           """)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));
      // Plus query to check that the document was updated
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
          {
            "findOne": {
              "filter" : {"_id" : "lexical-1"},
              "projection": {"*": 1 }
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(expectedAfterChange));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(12)
  class HappyCasesFindOneAndReplace {
    @Test
    void findOneAndReplaceWithSort() {
      final String expectedAfterChange = lexicalDoc(1, "monkey banana", "value1-replaced");
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
                  """
           {
             "findOneAndReplace": {
               "sort": { "$lexical": "banana" },
               "replacement" : %s,
               "projection": {"$lexical": 1 },
               "options": {"returnDocument": "after"}
             }
           }
           """
                  .formatted(expectedAfterChange))
          .body("data.document", jsonEquals(expectedAfterChange))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));
      // Plus query to check that the document was updated
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
          {
            "findOne": {
              "filter" : {"_id" : "lexical-1"},
              "projection": {"*": 1 }
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", jsonEquals(expectedAfterChange));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(13)
  class HappyCasesFindOneAndDelete {
    @Test
    void findOneAndDeleteWithSort() {
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
                      {
                        "findOneAndDelete": {
                          "sort": { "$lexical": "monkey" },
                          "projection": {"$lexical": 1 }
                        }
                      }
                      """)
          .body("status.deletedCount", is(1))
          .body("data.document", jsonEquals(DOC2_JSON));

      // Since "lexical-2" deleted, should only have 4 documents left
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
          {
            "find": {
              "projection": {"_id": 1, "value": 0 }
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents",
              containsInAnyOrder(
                  Map.of("_id", "lexical-1"),
                  Map.of("_id", "lexical-3"),
                  Map.of("_id", "lexical-4"),
                  Map.of("_id", "lexical-5")));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(14)
  class HappyCasesDeleteOne {
    @Test
    void deleteOneWithSort() {
      // delete doc "lexical-3":
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
                      {
                        "deleteOne": {
                          "filter": { },
                          "sort": { "$lexical": "biking" }
                        }
                      }
                      """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(1));

      // Should now delete "lexical-1", leaving 3 documents
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
          {
            "find": {
              "projection": {"_id": 1, "value": 0 }
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents",
              containsInAnyOrder(
                  Map.of("_id", "lexical-1"),
                  Map.of("_id", "lexical-4"),
                  Map.of("_id", "lexical-5")));
    }
  }

  static String lexicalDoc(int id, String keywords, String value) {
    return
        """
            {
              "_id": "lexical-%d",
              "$lexical": "%s",
              "value": "%s"
            }
        """
        .formatted(id, keywords, value);
  }
}
