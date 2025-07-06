package io.stargate.sgv2.jsonapi.api.v1.util.scenarios;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.TemplateRunner.asJSON;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.fixtures.data.DefaultData;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.*;

/**
 * Creates a table for testing vectors with:
 *
 * <ul>
 *   <li>PK of the single field column {@link TestDataScenario#ID_COL}
 *   <li>A text column {@link #CONTENT_COL}
 *   <li>Two vector columns {@link #INDEXED_VECTOR_COL} that is indexed and {@link
 *       #UNINDEXED_VECTOR_COL} that is not, both have dimension 5
 *   <li>20 rows with ID "row${number}" where number is 0+ that have a random vector, same in each
 *       vector col
 *   <li>One row with a vector all set to 1.0 , that has id row-1, so you can check to get a vector
 *       that is expected
 *   <li>The well known vector row availanle in {@link #KNOWN_VECTOR_ROW_JSON}
 * </ul>
 */
public class VectorDimension5TableScenario extends TestDataScenario {

  public static final ApiColumnDef CONTENT_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("content"), ApiDataTypeDefs.TEXT);
  public static final ApiColumnDef INDEXED_VECTOR_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("indexed_vector"), new ApiVectorType(5, null));
  public static final ApiColumnDef UNINDEXED_VECTOR_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("unindexed_vector"), new ApiVectorType( 5, null));

  public static final ArrayNode KNOWN_VECTOR = JsonNodeFactory.instance.arrayNode(5);
  public static final float[] KNOWN_VECTOR_ARRAY = new float[5];
  public static final Map<String, Object> KNOWN_VECTOR_ROW = new LinkedHashMap<>();
  public static final String KNOWN_VECTOR_ROW_JSON;

  static {
    for (int i = 0; i < 5; i++) {
      KNOWN_VECTOR.add(1.0f);
      KNOWN_VECTOR_ARRAY[i] = 1.0f;
    }

    KNOWN_VECTOR_ROW.put(fieldName(ID_COL), "row-1");
    KNOWN_VECTOR_ROW.put(
        fieldName(CONTENT_COL), "This is the content for row-1 - all 1.0 vectors ");
    KNOWN_VECTOR_ROW.put(fieldName(INDEXED_VECTOR_COL), KNOWN_VECTOR);
    KNOWN_VECTOR_ROW.put(fieldName(UNINDEXED_VECTOR_COL), KNOWN_VECTOR);

    KNOWN_VECTOR_ROW_JSON = asJSON(KNOWN_VECTOR_ROW);
  }

  public VectorDimension5TableScenario(String keyspaceName, String tableName) {
    super(keyspaceName, tableName, ID_COL, List.of(), createColumns(), new DefaultData());
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
