package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsWritePartialSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsWriteSuccess;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.fixtures.TestTextUtil;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

/** Tests for update operation for Collection Documents with Lexical (BM25) sort. */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class InsertLexicalInCollectionIntegrationTest
    extends AbstractCollectionIntegrationTestBase {
  private static final int MAX_LEXICAL_LENGTH = 8192;

  protected InsertLexicalInCollectionIntegrationTest() {
    super("col_insert_lexical_");
  }

  // Let's prevent creation of default Collection, and only create one when
  // Lexical enabled
  @Override
  public void createDefaultCollection() {}

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(1)
  class SetupCollection {
    @Test
    void createCollectionWithLexicalAndVectorize() {
      // Create a Collection with default Lexical settings
      createComplexCollection(
              """
                        {
                          "name": "%s",
                          "options" : {
                            "lexical": {
                              "enabled": true,
                              "analyzer": "standard"
                            },
                            "vector": {
                                "metric": "cosine",
                                "dimension": 5,
                                "service": {
                                    "provider": "custom",
                                    "modelName": "text-embedding-ada-002",
                                    "authentication": {
                                        "providerKey" : "shared_creds.providerKey"
                                    },
                                    "parameters": {
                                        "projectId": "test lexical project"
                                    }
                                }
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
    public void insertDocWithLongestLexicalOk() {
      final String docId = "lexical-long-ok";
      final String text = TestTextUtil.generateTextDoc(MAX_LEXICAL_LENGTH, " ");
      String doc =
              """
              {
                "_id": "%s",
                "$lexical": "%s"
              }
              """
              .formatted(docId, text);

      givenHeadersPostJsonThenOkNoErrors("{ \"insertOne\": {  \"document\": %s }}".formatted(doc))
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is(docId));

      givenHeadersPostJsonThenOkNoErrors(
              "{ \"find\": { \"filter\" : { \"_id\" : \"%s\"}}}}".formatted(docId))
          .body("$", responseIsFindSuccess())
          // NOTE: "$lexical" is not included in the response by default, ensure
          .body("data.documents", hasSize(1));
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
          .body(
              "errors[0].message",
              containsString("only be used on Collections for which Lexical feature is enabled"));

      // And finally, drop the Collection after use
      deleteCollection(COLLECTION_WITHOUT_LEXICAL);
    }

    @Test
    public void failInsertDocWithTooLongLexical() {
      final String docId = "lexical-too-long";
      // Limit not based on the length of the string, but on total length of unique
      // tokens. So need to guesstimate "too big" size
      final String text = TestTextUtil.generateTextDoc((int) (MAX_LEXICAL_LENGTH * 1.5), " ");
      String doc =
              """
              {
                "_id": "%s",
                "$lexical": "%s"
              }
              """
              .formatted(docId, text);

      givenHeadersPostJsonThenOk("{ \"insertOne\": {  \"document\": %s }}".formatted(doc))
          .body("$", responseIsWritePartialSuccess())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is(ErrorCodeV1.LEXICAL_CONTENT_TOO_BIG.name()))
          .body("errors[0].message", containsString("Lexical content is too big"));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(2)
  class InsertHybridOk {
    @Test
    public void insertSimpleHybridString() {
      givenHeadersPostJsonThenOkNoErrors(
              """
                {
                  "insertOne": {
                    "document": {
                        "_id": "hybrid-1",
                        "$hybrid": "monkeys and bananas"
                      }
                  }
                }
                """)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is("hybrid-1"));

      givenHeadersPostJsonThenOkNoErrors(
              // NOTE: "$lexical" is not included in the response by default, use projection
              """
                            {
                              "find": {
                                "filter" : {"_id" : "hybrid-1"},
                                "projection": { "*": 1 }
                              }
                            }
                            """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              // NOTE: vectorization uses bogus values for easier testing
              jsonEquals(
                  """
                      {
                        "_id": "hybrid-1",
                        "$lexical": "monkeys and bananas",
                        "$vectorize": "monkeys and bananas",
                        "$vector": [0.25, 0.25, 0.25, 0.25, 0.25]
                      }
                      """));
    }

    @Test
    public void insertSimpleHybridObject() {
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "insertOne": {
                "document": {
                    "_id": "hybrid-2",
                    "$hybrid": {
                       "$lexical": "monkey banana",
                        "$vectorize": "monkeys like bananas!"
                     }
                  }
              }
            }
            """)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is("hybrid-2"));

      givenHeadersPostJsonThenOkNoErrors(
              // NOTE: "$lexical" is not included in the response by default, use projection
              """
                            {
                              "find": {
                                "filter" : {"_id" : "hybrid-2"},
                                "projection": { "*": 1 }
                              }
                            }
                            """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                                  {
                                    "_id": "hybrid-2",
                                    "$lexical": "monkey banana",
                                    "$vectorize": "monkeys like bananas!",
                                    "$vector": [0.25, 0.25, 0.25, 0.25, 0.25]
                                  }
                                  """));
    }
  }

  // A subset of failing cases: bigger set in "HybridFieldExpanderTest"
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(3)
  class InsertHybridFail {
    @Test
    public void failForWrongNodeType() {
      givenHeadersPostJsonThenOk(
              """
            {
              "insertOne": {
                "document": {
                    "_id": "hybrid-fail-array",
                    "$hybrid": [ 1, 2, 3]
                }
              }
            }
            """)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("HYBRID_FIELD_UNSUPPORTED_VALUE_TYPE"))
          .body(
              "errors[0].message",
              containsString(
                  "Unsupported JSON value type for '$hybrid' field: expected String, Object or `null` but received Array"));
    }

    @Test
    public void failForUnknownSubFields() {
      givenHeadersPostJsonThenOk(
              """
            {
              "insertOne": {
                "document": {
                    "_id": "hybrid-fail-unknown-fields",
                    "$hybrid": {
                      "$lexical": "monkeys bananas",
                      "extra": "cannot have this",
                      "stuff": "or this"
                     }
                }
              }
            }
            """)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("HYBRID_FIELD_UNKNOWN_SUBFIELDS"))
          .body(
              "errors[0].message",
              containsString(
                  "Unrecognized sub-field(s) for '$hybrid' Object: expected '$lexical' and/or '$vectorize' but encountered: 'extra', 'stuff' (Document 1 of 1)"));
    }

    @Test
    public void failForConflictWithVectorize() {
      givenHeadersPostJsonThenOk(
              """
            {
              "insertOne": {
                "document": {
                    "_id": "hybrid-fail-conflict",
                    "$vectorize": "monkeys eat bananas",
                    "$hybrid": {
                      "$vectorize": "monkeys like bananas"
                     }
                }
              }
            }
            """)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is(ErrorCodeV1.HYBRID_FIELD_CONFLICT.name()))
          .body(
              "errors[0].message", containsString(ErrorCodeV1.HYBRID_FIELD_CONFLICT.getMessage()));
    }
  }
}
