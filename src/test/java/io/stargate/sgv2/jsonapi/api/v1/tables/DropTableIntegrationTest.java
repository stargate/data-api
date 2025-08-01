package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class DropTableIntegrationTest extends AbstractTableIntegrationTestBase {

  String simpleTableName = "simple_table_drop_index_test";
  String duplicateTableName = "duplicate_table_drop_index_test";

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

    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            duplicateTableName,
            Map.ofEntries( // create table
                Map.entry("id", Map.of("type", "text")),
                Map.entry("age", Map.of("type", "int")),
                Map.entry("name", Map.of("type", "text"))),
            "id")
        .wasSuccessful();
  }

  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class DropTableSuccess {
    @Order(1)
    @Test
    public void dropTableWithoutIfExist() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .dropTable(simpleTableName, false)
          .wasSuccessful();
      assertNamespaceCommand(keyspaceName)
          .templated()
          .listTables(false)
          .wasSuccessful()
          .body("status.tables", hasSize(1))
          .body("status.tables[0]", equalTo(duplicateTableName));
    }

    @Test
    @Order(2)
    public void dropTableWithIfExists() {
      for (int i = 0; i < 2; i++) {
        assertNamespaceCommand(keyspaceName)
            .templated()
            .dropTable(duplicateTableName, true)
            .wasSuccessful();
      }
      assertNamespaceCommand(keyspaceName)
          .templated()
          .listTables(false)
          .wasSuccessful()
          .body("status.tables", hasSize(0));
    }
  }

  @Nested
  @Order(2)
  class DropTableFails {
    @Test
    public void dropInvalidTableIfExistsFalse() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .dropTable("invalid_table", false)
          .hasSingleApiError(
              SchemaException.Code.CANNOT_DROP_UNKNOWN_TABLE,
              SchemaException.class,
              "The command attempted to drop the unknown table: invalid_table.");
    }

    // [data-api#1811]: 500 for empty Table name
    @Test
    public void dropInvalidTableWithEmptyName() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .dropTable("", false)
          .hasSingleApiError(
              ErrorCodeV1.COMMAND_FIELD_INVALID,
              "field 'command.name' value \"\" not valid. Problem: must not be empty.");
    }
  }
}
