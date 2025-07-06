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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
  public static final ApiColumnDef INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1 =
      new ApiColumnDef(
          CqlIdentifier.fromCql("indexed_vectorize_def_1"),
          new ApiVectorType(5, vectorizeDefinition));
  public static final ApiColumnDef INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_2 =
      new ApiColumnDef(
          CqlIdentifier.fromCql("indexed_vectorize_def_2"),
          new ApiVectorType(5, vectorizeDefinition));
  public static final ApiColumnDef INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF_1 =
      new ApiColumnDef(CqlIdentifier.fromCql("indexed_no_vectorize_def_1"), new ApiVectorType(5, null));
  public static final ApiColumnDef UNINDEXED_VECTOR_COL_1 =
      new ApiColumnDef(CqlIdentifier.fromCql("unindexed_vector_1"), new ApiVectorType(5, null));

  public static final List<String> DEFAULT_INSERTED_ROW_IDS =
      IntStream.rangeClosed(1, 20).mapToObj(i -> "row" + i).collect(Collectors.toList());

  public VectorizeTableScenario(String keyspaceName, String tableName) {
    super(keyspaceName, tableName, ID_COL, List.of(), createColumns(), new DefaultData());
  }

  private static ApiColumnDefContainer createColumns() {
    var columns = new ApiColumnDefContainer();
    columns.put(ID_COL);
    columns.put(CONTENT_COL);
    columns.put(INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1);
    columns.put(INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_2);
    columns.put(INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF_1);
    columns.put(UNINDEXED_VECTOR_COL_1);
    return columns;
  }

  @Override
  protected void createIndexes() {
    createIndex(INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1);
    createIndex(INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_2);
    createIndex(INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF_1);
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
    for (String defaultInsertedRowId : DEFAULT_INSERTED_ROW_IDS) {
      var row = new HashMap<String, Object>();
      row.put(fieldName(ID_COL), defaultInsertedRowId);
      row.put(fieldName(CONTENT_COL), "This is the content for " + defaultInsertedRowId);
      row.put(
          fieldName(INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1),
          columnValue(INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_1));
      row.put(
          fieldName(INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_2),
          columnValue(INDEXED_VECTOR_COL_WITH_VECTORIZE_DEF_2));
      row.put(
          fieldName(INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF_1),
          columnValue(INDEXED_VECTOR_COL_WITHOUT_VECTORIZE_DEF_1));
      row.put(fieldName(UNINDEXED_VECTOR_COL_1), columnValue(UNINDEXED_VECTOR_COL_1));
      rows.add(row);
    }

    insertManyRows(rows);
  }
}
