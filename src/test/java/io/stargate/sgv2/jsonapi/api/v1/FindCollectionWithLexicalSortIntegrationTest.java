package io.stargate.sgv2.jsonapi.api.v1;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindCollectionWithLexicalSortIntegrationTest
    extends AbstractCollectionIntegrationTestBase {
  static final String COLLECTION_WITH_LEXICAL =
      "coll_lexical_sort_" + RandomStringUtils.randomNumeric(16);

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
      insertDoc(COLLECTION_WITH_LEXICAL, lexicalDoc(1, "monkey banana"));
      insertDoc(COLLECTION_WITH_LEXICAL, lexicalDoc(2, "monkey"));
      insertDoc(COLLECTION_WITH_LEXICAL, lexicalDoc(3, "biking fun"));
      insertDoc(COLLECTION_WITH_LEXICAL, lexicalDoc(4, "banana"));
      insertDoc(COLLECTION_WITH_LEXICAL, lexicalDoc(5, "fun"));
    }
  }

  @Nested
  @Order(2)
  class HappyPath {}

  private String lexicalDoc(int id, String keywords) {
    return
        """
            {
              "_id": "lexical-%d",
              "$lexical": "%s"
            }
        """
        .formatted(id, keywords);
  }
}
