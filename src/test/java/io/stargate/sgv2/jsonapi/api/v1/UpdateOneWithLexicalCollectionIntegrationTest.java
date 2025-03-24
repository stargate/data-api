package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
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
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class UpdateOneWithLexicalCollectionIntegrationTest
    extends AbstractCollectionIntegrationTestBase {
  static final String COLLECTION_WITH_LEXICAL =
      "coll_lexical_operations_" + RandomStringUtils.randomNumeric(16);

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
              .formatted(COLLECTION_WITH_LEXICAL));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(2)
  class SetOperation {
    @Test
    @Order(1)
    void setupDocuments() {
      insertDoc(COLLECTION_WITH_LEXICAL, lexicalDoc(1, "monkey banana"));
      insertDoc(COLLECTION_WITH_LEXICAL, lexicalDoc(2, "monkey"));
      insertDoc(COLLECTION_WITH_LEXICAL, lexicalDoc(3, null));

      // and then verify that we can read them back as expected
      givenHeadersPostJsonThenOkNoErrors(
              keyspaceName,
              COLLECTION_WITH_LEXICAL,
              """
                                {
                                  "find": {
                                    "sort" : {"$lexical": "monkey" }
                                  }
                                }
                                """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(2))
          .body("data.documents[0]._id", is("lexical-2"))
          .body("data.documents[1]._id", is("lexical-1"));
    }

    @Test
    @Order(2)
    void testOverwriteEmpty() {}

    @Test
    @Order(3)
    void testOverwriteNonEmpty() {}

    @Test
    @Order(4)
    void testOverwriteNonEmptyWithNull() {}
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(3)
  class UnsetOperation {}

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(3)
  class UnsupportedOperations {}

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
