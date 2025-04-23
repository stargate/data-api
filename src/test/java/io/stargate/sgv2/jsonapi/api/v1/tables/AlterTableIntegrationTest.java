package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class AlterTableIntegrationTest extends AbstractTableIntegrationTestBase {
  String testTableName = "alter_table_test";

  @BeforeAll
  public final void createSimpleTable() {
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(
            testTableName,
            Map.ofEntries(
                Map.entry("id", Map.of("type", "text")),
                Map.entry("age", Map.of("type", "int")),
                Map.entry("comment", Map.of("type", "text")),
                Map.entry("vehicle_id", Map.of("type", "text"))),
            "id")
        .wasSuccessful();

    assertTableCommand(keyspaceName, testTableName)
        .templated()
        .createIndex("age_idx", "age")
        .wasSuccessful();
  }

  @Nested
  @Order(1)
  class AlterTableAddColumnsSuccess {
    @Test
    public void shouldAddColumnsToTable() {
      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable(
              "add" /* alterType */,
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
          .wasSuccessful();

      assertNamespaceCommand(keyspaceName)
          .templated()
          .listTables(true)
          .wasSuccessful()
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

      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable("add", Map.ofEntries(Map.entry("age", Map.of("type", "int"))))
          .hasSingleApiError(
              SchemaException.Code.CANNOT_ADD_EXISTING_COLUMNS,
              SchemaException.class,
              "The request included the following duplicate columns: age(int).");
    }

    @Test
    public void addColumnUnsupportedDataTypes() {
      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable("add", Map.ofEntries(Map.entry("timeuuidColumn", Map.of("type", "timeuuid"))))
          .hasSingleApiError(
              SchemaException.Code.CANNOT_ADD_UNSUPPORTED_DATA_TYPE_COLUMNS,
              SchemaException.class,
              "The command attempted to add columns with unsupported data types: timeuuid");
    }

    @Test
    public void addColumnUnsupportedCollectionTypes() {
      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable(
              "add",
              Map.ofEntries(
                  Map.entry("listColumn", Map.of("type", "list", "valueType", "counter"))))
          .hasSingleApiError(
              SchemaException.Code.UNSUPPORTED_LIST_DEFINITION,
              SchemaException.class,
              "The command used the value type: counter.");
      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable(
              "add",
              Map.ofEntries(Map.entry("setColumn", Map.of("type", "set", "valueType", "timeuuid"))))
          .hasSingleApiError(
              SchemaException.Code.UNSUPPORTED_SET_DEFINITION,
              SchemaException.class,
              "The command used the value type: timeuuid.");
      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable(
              "add",
              Map.ofEntries(
                  Map.entry(
                      "mapColumn",
                      Map.of("type", "map", "keyType", "counter", "valueType", "text"))))
          .hasSingleApiError(
              SchemaException.Code.UNSUPPORTED_MAP_DEFINITION,
              SchemaException.class,
              "The command used the key type: counter.");
    }
  }

  @Nested
  @Order(3)
  class AlterTableDropColumnsSuccess {
    @Test
    public void shouldDropColumnsFromTable() {
      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable("drop", List.of("vehicle_id_4", "list_type"))
          .wasSuccessful();

      assertNamespaceCommand(keyspaceName)
          .templated()
          .listTables(true)
          .wasSuccessful()
          .wasSuccessful()
          .body("status.tables[0].definition.columns.vehicle_id_4", nullValue())
          .body("status.tables[0].definition.columns.list_type", nullValue());
    }
  }

  @Nested
  @Order(4)
  class AlterTableDropColumnsFailure {
    @Test
    public void dropInvalidColumns() {
      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable("drop", List.of("invalid_column"))
          .hasSingleApiError(
              SchemaException.Code.CANNOT_DROP_UNKNOWN_COLUMNS,
              SchemaException.class,
              "The command attempted to drop the unknown columns: invalid_column.");
    }

    @Test
    public void dropPrimaryKeyColumns() {
      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable("drop", List.of("id"))
          .hasSingleApiError(
              SchemaException.Code.CANNOT_DROP_PRIMARY_KEY_COLUMNS,
              SchemaException.class,
              "The command attempted to drop the primary key columns: id.");
    }

    @Test
    public void dropColumnWithIndex() {

      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable("drop", List.of("age"))
          .hasSingleApiError(
              SchemaException.Code.CANNOT_DROP_INDEXED_COLUMNS,
              SchemaException.class,
              "The command attempted to drop the indexed columns: age.");
    }
  }

  @Nested
  @Order(5)
  class AlterTableAddVectorizeSuccess {
    @Test
    public void shouldAddVectorizeToColumns() {
      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable(
              "addVectorize",
              Map.of("vector_type_2", Map.of("provider", "mistral", "modelName", "mistral-embed")))
          .hasNoErrors()
          .body("status.ok", is(1));

      assertNamespaceCommand(keyspaceName)
          .templated()
          .listTables(true)
          .wasSuccessful()
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

      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable(
              "addVectorize",
              Map.of("invalid_column", Map.of("provider", "mistral", "modelName", "mistral-embed")))
          .hasSingleApiError(
              SchemaException.Code.CANNOT_VECTORIZE_UNKNOWN_COLUMNS,
              SchemaException.class,
              "The command attempted to vectorize the unknown columns: invalid_column.");
    }

    @Test
    public void addingToNonVectorTypeColumn() {

      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable(
              "addVectorize",
              Map.of("age", Map.of("provider", "mistral", "modelName", "mistral-embed")))
          .hasSingleApiError(
              SchemaException.Code.CANNOT_VECTORIZE_NON_VECTOR_COLUMNS,
              SchemaException.class,
              "The command attempted to vectorize the non-vector columns: age.");
    }

    @Test
    public void deprecatedEmbeddingModel() {
      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable(
              "addVectorize",
              Map.of(
                  "vector_type_1",
                  Map.of("provider", "nvidia", "modelName", "a-deprecated-nvidia-embedding-model")))
          .hasSingleApiError(
              SchemaException.Code.UNSUPPORTED_PROVIDER_MODEL,
              SchemaException.class,
              "The model a-deprecated-nvidia-embedding-model is at DEPRECATED status");
    }

    private static Stream<Arguments> deprecatedEmbeddingModelSource() {
      return Stream.of(
          Arguments.of(
              "DEPRECATED",
              "a-deprecated-nvidia-embedding-model",
              SchemaException.Code.DEPRECATED_PROVIDER_MODEL),
          Arguments.of(
              "END_OF_LIFE",
              "a-EOL-nvidia-embedding-model",
              SchemaException.Code.END_OF_LIFE_PROVIDER_MODEL));
    }

    @ParameterizedTest
    @MethodSource("deprecatedEmbeddingModelSource")
    public void deprecatedEmbeddingModel(
        String status, String modelName, SchemaException.Code errorCode) {
      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable(
              "addVectorize",
              Map.of("vector_type_1", Map.of("provider", "nvidia", "modelName", modelName)))
          .hasSingleApiError(
              errorCode,
              SchemaException.class,
              "The model %s is at %s status".formatted(modelName, status));
    }
  }

  @Nested
  @Order(7)
  class AlterTableDropVectorizeSuccess {
    @Test
    public void shouldDropVectorizeForColumns() {
      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable("dropVectorize", List.of("vector_type_1"))
          .hasNoErrors()
          .body("status.ok", is(1));
      assertNamespaceCommand(keyspaceName)
          .templated()
          .listTables(true)
          .wasSuccessful()
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

      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable("dropVectorize", List.of("invalid_column"))
          .hasSingleApiError(
              SchemaException.Code.CANNOT_DROP_VECTORIZE_FROM_UNKNOWN_COLUMNS,
              SchemaException.class,
              "The command attempted to drop vectorize configuration from the unknown columns: invalid_column.");
    }
  }

  @Nested
  @Order(9)
  class AlterNullOrEmptyColumns {

    // Those combinations of alterTable should work as NO_OP
    private static Stream<Arguments> nullOrEmptyColumns() {
      return Stream.of(
          Arguments.of("add", "{}"),
          Arguments.of("add", "{\"columns\": null}"),
          Arguments.of("add", "{\"columns\": {}}"),
          Arguments.of("drop", "{}"),
          Arguments.of("drop", "{\"columns\": null}"),
          Arguments.of("drop", "{\"columns\": []}"),
          Arguments.of("addVectorize", "{}"),
          Arguments.of("addVectorize", "{\"columns\": null}"),
          Arguments.of("addVectorize", "{\"columns\": {}}"),
          Arguments.of("dropVectorize", "{}"),
          Arguments.of("dropVectorize", "{\"columns\": null}"),
          Arguments.of("dropVectorize", "{\"columns\": []}"));
    }

    @ParameterizedTest
    @MethodSource("nullOrEmptyColumns")
    public void dropInvalidColumns(String operation, String columnsJson) {
      assertTableCommand(keyspaceName, testTableName)
          .templated()
          .alterTable(operation, columnsJson)
          .hasSingleApiError(
              SchemaException.Code.MISSING_ALTER_TABLE_OPERATIONS,
              SchemaException.class,
              "The command included the empty operation: " + operation);
    }
  }
}
