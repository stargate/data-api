package io.stargate.sgv2.jsonapi.api.v1.util.scenarios;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.TemplateRunner.asJSON;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.fixtures.data.DefaultData;
import io.stargate.sgv2.jsonapi.fixtures.types.ApiDataTypesForTesting;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Creates a table with: - PK of the single field column {@link TestDataScenario#ID_COL} - a column
 * for each scalar type in {@link ApiDataTypesForTesting#ALL_SCALAR_TYPES_FOR_CREATE} - the non PK
 * columns are named with the prefix {@link #COL_NAME_PREFIX} then the data type name - gets data
 * from the {@link DefaultData} class - inserts row with the PK prefix "row" - the row "row-1" (-1)
 * has a value for every column - the row "row-0" and beyond have a null for a single column in each
 * row, one column at a time - call {@link #columnForDatatype(ApiDataType)} to get the column for a
 * specific data type
 */
public class VectorDimension5TableScenario extends TestDataScenario {

  public static final ApiColumnDef CONTENT_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("content"), ApiDataTypeDefs.TEXT);
  public static final ApiColumnDef INDEXED_VECTOR_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("indexed_vector"), ApiVectorType.from(5));
  public static final ApiColumnDef UNINDEXED_VECTOR_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("unindexed_vector"), ApiVectorType.from(5));

  public static final ArrayNode KNOWN_VECTOR = JsonNodeFactory.instance.arrayNode(5);
  public static final Map<String, Object> KNOWN_VECTOR_ROW = new LinkedHashMap<>();
  public static final String KNOWN_VECTOR_ROW_JSON;

  static {
    for (int i = 0; i < 5; i++) {
      KNOWN_VECTOR.add(1.0f);
    }

    KNOWN_VECTOR_ROW.put(fieldName(ID_COL), "row-1");
    KNOWN_VECTOR_ROW.put(
        fieldName(CONTENT_COL), "This is the content for row-1 - all 1.0 vectors ");
    KNOWN_VECTOR_ROW.put(fieldName(INDEXED_VECTOR_COL), KNOWN_VECTOR);
    KNOWN_VECTOR_ROW.put(fieldName(UNINDEXED_VECTOR_COL), KNOWN_VECTOR);

    KNOWN_VECTOR_ROW_JSON = asJSON(KNOWN_VECTOR_ROW);
  }

  public VectorDimension5TableScenario(String keyspaceName, String tableName) {
    super(keyspaceName, tableName, ID_COL, createColumns(), new DefaultData());
  }

  private static ApiColumnDefContainer createColumns() {

    var columns = new ApiColumnDefContainer();
    columns.put(ID_COL);
    columns.put(CONTENT_COL);
    columns.put(INDEXED_VECTOR_COL);
    columns.put(UNINDEXED_VECTOR_COL);
    return columns;
  }

  @Override
  protected void createIndexes() {
    assertTableCommand(keyspaceName, tableName)
        .templated()
        .createVectorIndex(
            "idx_" + tableName + "_" + fieldName(INDEXED_VECTOR_COL), fieldName(INDEXED_VECTOR_COL))
        .wasSuccessful();
  }

  @Override
  protected void insertRows() {

    var rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 20; i++) {
      var row = new HashMap<String, Object>();
      row.put(fieldName(ID_COL), "row" + i);
      row.put(fieldName(CONTENT_COL), "This is the content for row " + i);
      var vector = columnValue(INDEXED_VECTOR_COL);
      row.put(fieldName(INDEXED_VECTOR_COL), vector);
      row.put(fieldName(UNINDEXED_VECTOR_COL), vector);
      rows.add(row);
    }

    rows.add(KNOWN_VECTOR_ROW);

    insertManyRows(rows);
  }
}
