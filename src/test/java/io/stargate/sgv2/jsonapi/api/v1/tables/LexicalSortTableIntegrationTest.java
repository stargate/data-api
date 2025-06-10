package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class LexicalSortTableIntegrationTest extends AbstractTableIntegrationTestBase {
  private static final String TABLE_NAME = "lexicalSortTableTest";

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(1)
  class Setup {
    @Test
    void createCollectionWithLexicalIndex() {
      // Create a table with a lexical index on the "tags" column
      assertNamespaceCommand(keyspaceName)
          .templated()
          .createTable(
              TABLE_NAME,
              Map.ofEntries(
                  Map.entry("id", Map.of("type", "text")),
                  Map.entry("value", Map.of("type", "text")),
                  Map.entry("tags", Map.of("type", "text"))),
              "id")
          .wasSuccessful();

      assertTableCommand(keyspaceName, TABLE_NAME)
          .postCreateTextIndex(
              """
                            {
                              "name": "tags_lexical_idx",
                              "definition": {
                                "column": "tags",
                                "options": {
                                  "analyzer": "standard"
                                }
                              }
                            }
                            """)
          .wasSuccessful();
      // And then insert some data
      insertOneInTable(
          TABLE_NAME,
          Map.of(
              "id", "1",
              "value", "First value",
              "tags", "tag1"));
      insertOneInTable(
          TABLE_NAME,
          Map.of(
              "id", "2",
              "value", "First value",
              "tags", "tag2"));
      insertOneInTable(
          TABLE_NAME,
          Map.of(
              "id", "3",
              "value", "First value",
              "tags", "tag3"));
      insertOneInTable(
          TABLE_NAME,
          Map.of(
              "id", "4",
              "value", "First value",
              "tags", "tag1 tag2 tag3"));
    }
  }
}
