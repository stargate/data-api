package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsWritePartialSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsWriteSuccess;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
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
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class InsertLexicalInCollectionIntegrationTest
    extends AbstractCollectionIntegrationTestBase {
  protected InsertLexicalInCollectionIntegrationTest() {
    super("col_insert_lexical_");
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

  // Tests for inserting documents with "$lexical" field: simple pass (for Lexical-enabled
  // Collection), simple fail (for Lexical-disabled Collection)
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(2)
  class InsertLexicalBasics {
    static final String DOC_WITH_LEXICAL =
        """
                        {
                          "_id": "lexical1",
                          "username": "user-lexical",
                          "extra": 123,
                          "$lexical": "monkeys and bananas"
                        }
                        """;

    // NOTE: Relies on default settings allowing insertion of "$lexical" content
    // (that is: by default Collection created has "$lexical" enabled)
    @Test
    public void insertDocWithLexicalOk() {
      givenHeadersPostJsonThenOkNoErrors(
                  """
                    {
                      "insertOne": {
                        "document": %s
                      }
                    }
                    """
                  .formatted(DOC_WITH_LEXICAL))
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is("lexical1"));

      givenHeadersPostJsonThenOkNoErrors(
              """
                            {
                              "find": {
                                "filter" : {"_id" : "lexical1"}
                              }
                            }
                            """)
          .body("$", responseIsFindSuccess())
          // NOTE: "$lexical" is not included in the response by default, ensure
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                            {
                                "_id": "lexical1",
                                "username": "user-lexical",
                                "extra": 123
                            }
                            """));

      // But can explicitly include: either via "include-it-all"
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "find": {
                    "filter" : {"_id" : "lexical1"},
                    "projection": { "*": 1 }
                  }
                }
                """)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(DOC_WITH_LEXICAL));

      // Or just the "$lexical" field (plus always _id)
      givenHeadersPostJsonThenOkNoErrors(
              """
                            {
                              "find": {
                                "filter" : {"_id" : "lexical1"},
                                "projection": {
                                  "$lexical": 1,
                                  "extra": 1
                                }
                              }
                            }
                            """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                                      {
                                          "_id": "lexical1",
                                          "extra": 123,
                                          "$lexical": "monkeys and bananas"
                                      }
                                      """));
    }

    @Test
    public void failInsertDocWithLexicalIfNotEnabled() {
      final String COLLECTION_WITHOUT_LEXICAL =
          "coll_insert_no_lexical_" + RandomStringUtils.randomNumeric(16);
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

      givenHeadersAndJson(
                  """
                    {
                      "insertOne": {
                        "document": %s
                      }
                    }
                    """
                  .formatted(DOC_WITH_LEXICAL))
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, COLLECTION_WITHOUT_LEXICAL)
          .then()
          .body("$", responseIsWritePartialSuccess())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("LEXICAL_NOT_ENABLED_FOR_COLLECTION"))
          .body("errors[0].message", containsString("Lexical search is not enabled"));

      // And finally, drop the Collection after use
      deleteCollection(COLLECTION_WITHOUT_LEXICAL);
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(2)
  class InsertHybridOk {
    @Test
    public void insertSimpleHybrid() {
      final String HYBRID_DOC =
          """
                      {
                        "_id": "hybrid-1",
                        "$hybrid": "monkeys and bananas"
                      }
                      """;
      givenHeadersPostJsonThenOkNoErrors(
                  """
                {
                  "insertOne": {
                    "document": %s
                  }
                }
                """
                  .formatted(HYBRID_DOC))
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is("hybrid-1"));

      givenHeadersPostJsonThenOkNoErrors(
              """
                            {
                              "find": {
                                "filter" : {"_id" : "hybrid-1"}
                              }
                            }
                            """)
          .body("$", responseIsFindSuccess())
          // NOTE: "$lexical" is not included in the response by default, ensure
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                      {
                        "_id": "hybrid-1",
                        "$lexical": "monkeys and bananas",
                        "$vectorize": "monkeys and bananas"
                      }
                      """));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(3)
  class InsertHybridFail {
    @Test
    @Order(1)
    public void setupDocuments() {}
  }
}
