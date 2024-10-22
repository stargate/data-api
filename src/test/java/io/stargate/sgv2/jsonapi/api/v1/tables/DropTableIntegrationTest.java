package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
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
  class DropTableSuccess {
    @Test
    public void dropTableWithoutIfExist() {
      assertNamespaceCommand(keyspaceName).templated().dropTable(simpleTableName, false);
      assertNamespaceCommand(keyspaceName)
          .templated()
          .listTables(false)
          .wasSuccessful()
          .body("status.tables", hasSize(1))
          .body("status.tables[0]", equalTo(duplicateTableName));
    }

    @Test
    public void dropTableWithIfExists() {
      for (int i = 0; i < 2; i++) {
        assertNamespaceCommand(keyspaceName).templated().dropTable(duplicateTableName, true);
      }
      assertNamespaceCommand(keyspaceName)
          .templated()
          .listTables(false)
          .wasSuccessful()
          .body("status.tables", hasSize(0));
    }
  }
}
