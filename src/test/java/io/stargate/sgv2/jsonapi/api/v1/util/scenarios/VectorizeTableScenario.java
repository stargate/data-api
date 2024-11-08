package io.stargate.sgv2.jsonapi.api.v1.util.scenarios;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.fixtures.data.DefaultData;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDefContainer;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import java.util.*;

/**
 * Creates a table for testing vectorize with:
 *
 * <ul>
 *   <li>Insertion
 *   <li>Sort: vectorize sort on unknown vector column #UNINDEXED_VECTOR_COL} that is not, both have
 *       dimension 5
 * </ul>
 */
public class VectorizeTableScenario extends TestDataScenario {

  public static final VectorizeDefinition vectorizeDefinition =
      new VectorizeDefinition("custom", "text-embedding-ada-002", Map.of(), Map.of());
  // total 5 columns in the table
  public static final ApiColumnDef CONTENT_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("content"), ApiDataTypeDefs.TEXT);
  public static final ApiColumnDef INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF =
      new ApiColumnDef(
          CqlIdentifier.fromCql("indexed_vector_col1"), ApiVectorType.from(5, vectorizeDefinition));
  public static final ApiColumnDef INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF =
      new ApiColumnDef(CqlIdentifier.fromCql("indexed_vector_col2"), ApiVectorType.from(5));
  public static final ApiColumnDef UNINDEXED_VECTOR_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("unindexed_vector_column"), ApiVectorType.from(5));

  public VectorizeTableScenario(String keyspaceName, String tableName) {
    super(keyspaceName, tableName, ID_COL, List.of(), createColumns(), new DefaultData());
  }

  private static ApiColumnDefContainer createColumns() {
    var columns = new ApiColumnDefContainer();
    columns.put(ID_COL);
    columns.put(CONTENT_COL);
    columns.put(INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF);
    columns.put(INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF);
    columns.put(UNINDEXED_VECTOR_COL);
    return columns;
  }

  @Override
  protected void createIndexes() {
    createIndex(INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF);
    createIndex(INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF);
  }

  private void createIndex(ApiColumnDef apiColumnDef) {
    assertTableCommand(keyspaceName, tableName)
        .templated()
        .createVectorIndex(
            "idx_" + tableName + "_" + fieldName(apiColumnDef), fieldName(apiColumnDef))
        .wasSuccessful();
  }

  @Override
  protected void insertRows() {

    var rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 20; i++) {
      var row = new HashMap<String, Object>();
      row.put(fieldName(ID_COL), "row" + i);
      row.put(fieldName(CONTENT_COL), "This is the content for row " + i);
      row.put(
          fieldName(INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF),
          columnValue(INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF));
      row.put(
          fieldName(INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF),
          columnValue(INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF));
      row.put(fieldName(UNINDEXED_VECTOR_COL), columnValue(UNINDEXED_VECTOR_COL));
      rows.add(row);
    }

    insertManyRows(rows);
  }
}
