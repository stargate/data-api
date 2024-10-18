package io.stargate.sgv2.jsonapi.api.v1.tables;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class AlterTableIntegrationTest extends AbstractTableIntegrationTestBase {
  String testTableName = "alter_table_test";
  String listTablesWithSchema =
      """
                  {
                    "options" : {
                      "explain" : true
                    }
                  }
                  """;

  @BeforeAll
  public final void createSimpleTable() {
    createTableWithColumns(
        testTableName,
        Map.ofEntries(
            Map.entry("id", Map.of("type", "text")),
            Map.entry("age", Map.of("type", "int")),
            Map.entry("comment", Map.of("type", "text")),
            Map.entry("vehicle_id", Map.of("type", "text"))),
        "id");

    createIndex(testTableName, "age", "age_idx");
  }

  @Nested
  @Order(1)
  class AlterTableAddColumnsSuccess {
    @Test
    public void shouldAddColumnsToTable() {
      alterTableAddColumns(
              testTableName,
              Map.ofEntries(
                  Map.entry("vehicle_id_4", Map.of("type", "text")),
                  Map.entry("physicalAddress", Map.of("type", "text")),
                  Map.entry("list_type", Map.of("type", "list", "valueType", "text")),
                  Map.entry("set_type", Map.of("type", "set", "valueType", "text")),
                  Map.entry(
                      "map_type", Map.of("type", "map", "keyType", "text", "valueType", "text")),
                  Map.entry(
                      "vector_type_1",
                      Map.of(
                          "type",
                          "vector",
                          "dimension",
                          1024,
                          "service",
                          Map.of("provider", "nvidia", "modelName", "NV-Embed-QA"))),
                  Map.entry("vector_type_2", Map.of("type", "vector", "dimension", 1024))))
          .hasNoErrors()
          .body("status.ok", is(1));

      listTables(listTablesWithSchema)
          .body("status.tables[0].definition.columns.vehicle_id_4.type", equalTo("text"))
          .body("status.tables[0].definition.columns.physicalAddress.type", equalTo("text"))
          .body("status.tables[0].definition.columns.list_type.type", equalTo("list"))
          .body("status.tables[0].definition.columns.list_type.valueType", equalTo("text"))
          .body("status.tables[0].definition.columns.set_type.type", equalTo("set"))
          .body("status.tables[0].definition.columns.set_type.valueType", equalTo("text"))
          .body("status.tables[0].definition.columns.map_type.type", equalTo("map"))
          .body("status.tables[0].definition.columns.map_type.keyType", equalTo("text"))
          .body("status.tables[0].definition.columns.map_type.valueType", equalTo("text"))
          .body("status.tables[0].definition.columns.vector_type_1.type", equalTo("vector"))
          .body("status.tables[0].definition.columns.vector_type_1.dimension", equalTo(1024))
          .body(
              "status.tables[0].definition.columns.vector_type_1.service.provider",
              equalTo("nvidia"))
          .body(
              "status.tables[0].definition.columns.vector_type_1.service.modelName",
              equalTo("NV-Embed-QA"))
          .body("status.tables[0].definition.columns.vector_type_2.type", equalTo("vector"))
          .body("status.tables[0].definition.columns.vector_type_2.dimension", equalTo(1024));
    }
  }

  @Nested
  @Order(2)
  class AlterTableAddColumnsFailure {
    @Test
    public void shouldAddColumnsToTable() {
      final SchemaException schemaException =
          SchemaException.Code.COLUMN_ALREADY_EXISTS.get(Map.of("column", "age"));
      alterTableAddColumns(testTableName, Map.ofEntries(Map.entry("age", Map.of("type", "int"))))
          .hasSingleApiError(
              SchemaException.Code.COLUMN_ALREADY_EXISTS,
              SchemaException.class,
              schemaException.body);
    }
  }

  @Nested
  @Order(3)
  class AlterTableDropColumnsSuccess {
    @Test
    public void shouldDropColumnsFromTable() {
      alterTableDropColumns(testTableName, List.of("vehicle_id_4", "list_type"))
          .hasNoErrors()
          .body("status.ok", is(1));

      listTables(listTablesWithSchema)
          .body("status.tables[0].definition.columns.vehicle_id_4", nullValue())
          .body("status.tables[0].definition.columns.list_type", nullValue());
    }
  }

  @Nested
  @Order(4)
  class AlterTableDropColumnsFailure {
    @Test
    public void dropInvalidColumns() {
      final SchemaException schemaException =
          SchemaException.Code.COLUMN_NOT_FOUND.get(Map.of("column", "invalid_column"));
      alterTableDropColumns(testTableName, List.of("invalid_column"))
          .hasSingleApiError(
              SchemaException.Code.COLUMN_NOT_FOUND, SchemaException.class, schemaException.body);
    }

    @Test
    public void dropPrimaryKeyColumns() {
      final SchemaException schemaException =
          SchemaException.Code.COLUMN_CANNOT_BE_DROPPED.get(
              Map.of("reason", "Primary key column `%s` cannot be dropped".formatted("id")));
      alterTableDropColumns(testTableName, List.of("id"))
          .hasSingleApiError(
              SchemaException.Code.COLUMN_CANNOT_BE_DROPPED,
              SchemaException.class,
              schemaException.body);
    }

    @Test
    public void dropColumnWithIndex() {
      final SchemaException schemaException =
          SchemaException.Code.COLUMN_CANNOT_BE_DROPPED.get(
              Map.of(
                  "reason",
                  "Index exists on the column `%s`, drop `%s` index to drop the column"
                      .formatted("age", "age_idx")));
      alterTableDropColumns(testTableName, List.of("age"))
          .hasSingleApiError(
              SchemaException.Code.COLUMN_CANNOT_BE_DROPPED,
              SchemaException.class,
              schemaException.body);
    }
  }

  @Nested
  @Order(5)
  class AlterTableAddVectorizeSuccess {
    @Test
    public void shouldAddVectorizeToColumns() {
      alterTableAddVectorize(
              testTableName,
              Map.of("vector_type_2", Map.of("provider", "mistral", "modelName", "mistral-embed")))
          .hasNoErrors()
          .body("status.ok", is(1));

      listTables(listTablesWithSchema)
          .body(
              "status.tables[0].definition.columns.vector_type_1.service.provider",
              equalTo("nvidia"))
          .body(
              "status.tables[0].definition.columns.vector_type_1.service.modelName",
              equalTo("NV-Embed-QA"))
          .body(
              "status.tables[0].definition.columns.vector_type_2.service.provider",
              equalTo("mistral"))
          .body(
              "status.tables[0].definition.columns.vector_type_2.service.modelName",
              equalTo("mistral-embed"));
    }
  }

  @Nested
  @Order(6)
  class AlterTableAddVectorizeFailure {
    @Test
    public void addingToInvalidColumn() {
      final SchemaException schemaException =
          SchemaException.Code.COLUMN_NOT_FOUND.get(Map.of("column", "invalid_column"));
      alterTableAddVectorize(
              testTableName,
              Map.of("invalid_column", Map.of("provider", "mistral", "modelName", "mistral-embed")))
          .hasSingleApiError(
              SchemaException.Code.COLUMN_NOT_FOUND, SchemaException.class, schemaException.body);
    }

    @Test
    public void addingToNonVectorTypeColumn() {
      final SchemaException schemaException =
          SchemaException.Code.NON_VECTOR_TYPE_COLUMN.get(Map.of("column", "age"));
      alterTableAddVectorize(
              testTableName,
              Map.of("age", Map.of("provider", "mistral", "modelName", "mistral-embed")))
          .hasSingleApiError(
              SchemaException.Code.NON_VECTOR_TYPE_COLUMN,
              SchemaException.class,
              schemaException.body);
    }
  }

  @Nested
  @Order(7)
  class AlterTableDropVectorizeSuccess {
    @Test
    public void shouldDropVectorizeForColumns() {
      alterTableDropVectorize(testTableName, List.of("vector_type_1"))
          .hasNoErrors()
          .body("status.ok", is(1));

      listTables(listTablesWithSchema)
          .body("status.tables[0].definition.columns.vector_type_1.service", nullValue())
          .body(
              "status.tables[0].definition.columns.vector_type_2.service.provider",
              equalTo("mistral"))
          .body(
              "status.tables[0].definition.columns.vector_type_2.service.modelName",
              equalTo("mistral-embed"));
    }
  }

  @Nested
  @Order(8)
  class AlterTableDropVectorizeFailure {
    @Test
    public void dropInvalidColumns() {
      final SchemaException schemaException =
          SchemaException.Code.COLUMN_NOT_FOUND.get(Map.of("column", "invalid_column"));
      alterTableDropColumns(testTableName, List.of("invalid_column"))
          .hasSingleApiError(
              SchemaException.Code.COLUMN_NOT_FOUND, SchemaException.class, schemaException.body);
    }
  }
}
