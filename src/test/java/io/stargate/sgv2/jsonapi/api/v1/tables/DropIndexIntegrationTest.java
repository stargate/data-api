package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class DropIndexIntegrationTest extends AbstractTableIntegrationTestBase {

  String simpleTableName = "simple_table_drop_index_test";

  @BeforeAll
  public final void createSimpleTable() {
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            simpleTableName,
            Map.ofEntries( // create table
                Map.entry("id", Map.of("type", "text")),
                Map.entry("age", Map.of("type", "int")),
                Map.entry("name", Map.of("type", "text"))),
            "id")
        .wasSuccessful();
    assertTableCommand(keyspaceName, simpleTableName)
        .templated()
        .createIndex("age_idx", "age")
        .wasSuccessful();
    assertTableCommand(keyspaceName, simpleTableName)
        .templated()
        .createIndex("name_idx", "name")
        .wasSuccessful();
    assertTableCommand(keyspaceName, simpleTableName)
        .templated()
        .listIndexes(false)
        .wasSuccessful()
        .hasIndexes("age_idx", "name_idx");
  }

  @Nested
  @Order(1)
  class DropIndexSuccess {
    @Test
    public void dropIndex() {
      assertNamespaceCommand(keyspaceName).templated().dropIndex("age_idx", false).wasSuccessful();
      assertTableCommand(keyspaceName, simpleTableName)
          .templated()
          .listIndexes(false)
          .doesNotHaveIndexes("age_idx");
    }

    @Test
    public void dropIndexNotExisting() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .dropIndex("invalid_idx", false)
          .hasSingleApiError(
              SchemaException.Code.CANNOT_DROP_UNKNOWN_INDEX,
              SchemaException.class,
              "The command attempted to drop the unknown index: invalid_idx.");
    }

    @Test
    public void dropIndexIfExists() {

      for (int i = 0; i < 2; i++) {
        assertNamespaceCommand(keyspaceName)
            .templated()
            .dropIndex("name_idx", true)
            .wasSuccessful();
      }
      assertTableCommand(keyspaceName, simpleTableName)
          .templated()
          .listIndexes(false)
          .doesNotHaveIndexes("name_idx");
    }
  }
}
