package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindWithLexicalFilterTableIntegrationTest extends AbstractTableIntegrationTestBase {
  private static final String TABLE_NAME = "lexicalFilterTableTest";

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(1)
  class Setup {
    @Test
    void createAndPopulateTableWithLexicalIndex() {
      // Create a table with a lexical index on the "tags" column
      assertNamespaceCommand(keyspaceName)
          .templated()
          .createTable(
              TABLE_NAME,
              Map.ofEntries(
                  Map.entry("id", Map.of("type", "text")),
                  Map.entry("value", Map.of("type", "text")),
                  Map.entry("tags", Map.of("type", "text")),
                  Map.entry("no_index_tags", Map.of("type", "text"))),
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
          .postCreateIndex(
              """
                                {
                                        "name": "value_idx",
                                        "definition": {
                                           "column": "value"
                                        }
                                }
                                """)
          .wasSuccessful();

      // And then insert some data
      insertOneInTable(
          TABLE_NAME,
          Map.of(
              "id", "1",
              "value", "a",
              "tags", "tag1",
              "no_index_tags", "abc"));
      insertOneInTable(
          TABLE_NAME, Map.of("id", "2", "value", "b", "tags", "tag2", "no_index_tags", "def"));
      insertOneInTable(
          TABLE_NAME,
          Map.of(
              "id", "3",
              "value", "c",
              "tags", "tag3 tag1",
              "no_index_tags", "ghi"));
      insertOneInTable(
          TABLE_NAME,
          Map.of(
              "id", "4",
              "value", "d",
              "tags", "tag2 tag1 tag3",
              "no_index_tags", "jkl"));
    }
  }

  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(5)
  class HappyLexicalFilter {
    @Test
    void simpleFilter() {
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .find(Map.of("tags", Map.of("$match", "tag3")), List.of("id"), null)
          .wasSuccessful()
          .body("data.documents", hasSize(2))
          .body("data.documents", containsInAnyOrder(Map.of("id", "3"), Map.of("id", "4")));
    }
  }

  // Failing cases
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  @Nested
  @Order(10)
  class SadLexicalFilter {
    // 21-Aug-2025, tatu: Disabled for now to get working cases in: enable when
    //   https://github.com/stargate/data-api/issues/2175
    //   implemented
    @Disabled("Disabled until [https://github.com/stargate/data-api/issues/2175] implemented")
    @Test
    void failOnMissingColumn() {
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .find(Map.of("unknown-column", Map.of("$match", "value")), List.of("id"), null)
          .hasSingleApiError(
              FilterException.Code.UNKNOWN_TABLE_COLUMNS,
              FilterException.class,
              "Only columns defined in the table schema can be",
              "defines the columns: ");
    }

    // 21-Aug-2025, tatu: Disabled for now to get working cases in: enable when
    //   https://github.com/stargate/data-api/issues/2175
    //   implemented
    @Disabled("Disabled until [https://github.com/stargate/data-api/issues/2175] implemented")
    @Test
    void failOnNonIndexedColumn() {
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .find(Map.of("no_index_tags", Map.of("$match", "value")), List.of("id"), null)
          // NOT RIGHT ERROR CODE: just a placeholder
          .hasSingleApiError(
              FilterException.Code.UNKNOWN_TABLE_COLUMNS,
              FilterException.class,
              "Only columns defined in the table schema can be",
              "defines the columns: ");
    }
  }
}
