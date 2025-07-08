package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.hasSize;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.LinkedHashMap;
import java.util.List;
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
                  Map.entry("tags", Map.of("type", "text")),
                  Map.entry("tags2", Map.of("type", "text"))),
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
      assertTableCommand(keyspaceName, TABLE_NAME)
          .postCreateTextIndex(
              """
                                    {
                                      "name": "tags2_lexical_idx",
                                      "definition": {
                                        "column": "tags2",
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
              "tags", "tag1",
              "tags2", "abc"));
      insertOneInTable(
          TABLE_NAME,
          Map.of("id", "2", "value", "Second value", "tags", "tag2", "tags2", "abc def"));
      insertOneInTable(
          TABLE_NAME,
          Map.of(
              "id", "3",
              "value", "Third value",
              "tags", "tag3",
              "tags2", "def xyz"));
      insertOneInTable(
          TABLE_NAME,
          Map.of(
              "id", "4",
              "value", "Fourth value",
              "tags", "tag1 tag2 tag3",
              "tags2", "xyz"));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(5)
  class HappyLexicalSort {
    @Test
    void simpleSort() {
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .find(null, List.of("id"), Map.of("tags", "tag2"))
          .wasSuccessful()
          .body("data.documents", hasSize(2))
          .body("data.documents", jsonEquals(List.of(Map.of("id", "2"), Map.of("id", "4"))));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(10)
  class SadLexicalSort {
    @Test
    void unknownColumn() {
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .find(null, List.of("id"), Map.of("notTags", "tag2"))
          .hasSingleApiError(
              SortException.Code.CANNOT_SORT_UNKNOWN_COLUMNS,
              SortException.class,
              "command attempted to sort using columns that are not in the table schema",
              "\"lexicalSortTableTest\" defines the columns");
    }

    @Test
    void noIndexOnColumn() {
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .find(null, List.of("id"), Map.of("value", "tag2"))
          .hasSingleApiError(
              SortException.Code.CANNOT_LEXICAL_SORT_NON_INDEXED_COLUMNS,
              SortException.class,
              "command attempted to lexical sort on column that is not indexed",
              "has indexes on columns: tags, tags2");
    }

    @Test
    void lexicalNotAlone() {
      // 2 lexical columns -> cannot do
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .find(null, List.of("id"), sortedMapOf("tags", "tag1", "tags2", "abc def"))
          .hasSingleApiError(
              SortException.Code.CANNOT_SORT_ON_SPECIAL_WITH_OTHERS,
              SortException.class,
              "command used a sort clause with a special (lexical/vector/vectorize) sort combined with one or more other sort expressions",
              "The command attempted to use lexical sort on columns: tags, tags2",
              "The command attempted to use vector/vectorize sort on columns: [None]",
              "The command attempted to use regular sort on columns: [None]");
      // 1 lexical column + 1 regular column -> cannot do
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .find(null, List.of("id"), sortedMapOf("tags", "tag1", "value", 1))
          .hasSingleApiError(
              SortException.Code.CANNOT_SORT_ON_SPECIAL_WITH_OTHERS,
              SortException.class,
              "The command attempted to use lexical sort on columns: tags",
              "The command attempted to use vector/vectorize sort on columns: [None]",
              "The command attempted to use regular sort on columns: value");
    }
  }

  private static Map<String, Object> sortedMapOf(Object... keysAndValues) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }
}
