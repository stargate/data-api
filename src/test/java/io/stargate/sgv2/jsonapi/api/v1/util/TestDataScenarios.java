package io.stargate.sgv2.jsonapi.api.v1.util;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.fixtures.data.DefaultData;
import io.stargate.sgv2.jsonapi.fixtures.types.ApiDataTypesForTesting;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs;
import io.stargate.sgv2.jsonapi.service.schema.tables.PrimitiveApiDataTypeDef;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Re-usable scenarios of tables and sample data for testing.
 *
 * <p>Will create the table, insert rows, and verify
 */
public class TestDataScenarios {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestDataScenarios.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static final String ID_COL = "id";
  public static final String COL_NAME_PREFIX = "col_";

  private static final DefaultData dataSource = new DefaultData();

  /**
   * Create a table with a col for all {@link
   * io.stargate.sgv2.jsonapi.fixtures.types.ApiDataTypesForTesting#ALL_SCALAR_TYPES_FOR_CREATE} and
   * insert a rows, one with all set, one for each col with a null value.
   *
   * @param keyspaceName
   * @param tableName
   */
  public void allScalarTypesRowsWithNulls(String keyspaceName, String tableName) {

    createTableWithTypes(
        keyspaceName, tableName, ApiDataTypesForTesting.ALL_SCALAR_TYPES_FOR_CREATE);

    var rows = new ArrayList<>();
    for (int i = -1; i < ApiDataTypesForTesting.ALL_SCALAR_TYPES_FOR_CREATE.size(); i++) {

      // linked to keep order
      var row = new LinkedHashMap<String, Object>();
      row.put(ID_COL, "row" + i);
      addRowValues(row, ApiDataTypesForTesting.ALL_SCALAR_TYPES_FOR_CREATE, i);
      rows.add(row);
    }
    var rowsJson =
        rows.stream()
            .map(
                row -> {
                  try {
                    return MAPPER.writeValueAsString(row);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .toList();
    LOGGER.warn("Inserting rows {}", rowsJson);

    assertTableCommand(keyspaceName, tableName).templated().insertMany(rowsJson).wasSuccessful();
  }

  private void createTableWithTypes(
      String keyspaceName, String tableName, List<PrimitiveApiDataTypeDef> types) {

    // linked to keep order
    var columns = new LinkedHashMap<String, Object>();
    columns.put(ID_COL, columnDef(ApiDataTypeDefs.TEXT));
    buildColumns(columns, types);

    LOGGER.warn("Creating table {} with columns {}", tableName, columns);
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(tableName, columns, ID_COL)
        .wasSuccessful();
  }

  private LinkedHashMap<String, Object> buildColumns(
      LinkedHashMap<String, Object> columns, List<PrimitiveApiDataTypeDef> types) {

    types.forEach(
        type -> {
          columns.put(columnName(type), columnDef(type));
        });
    return columns;
  }

  private static Map<String, String> columnDef(PrimitiveApiDataTypeDef type) {
    return Map.of("type", type.getName().getApiName());
  }

  public static String columnName(PrimitiveApiDataTypeDef type) {
    return COL_NAME_PREFIX + type.getName().getApiName();
  }

  public Object columnValue(PrimitiveApiDataTypeDef type) {
    return dataSource.fromJSON(type.getCqlType()).value();
  }

  private LinkedHashMap<String, Object> addRowValues(
      LinkedHashMap<String, Object> row, List<PrimitiveApiDataTypeDef> types, int nullIndex) {
    for (int i = 0; i < types.size(); i++) {
      var type = types.get(i);
      row.put(columnName(type), i == nullIndex ? null : columnValue(type));
    }
    return row;
  }
}
