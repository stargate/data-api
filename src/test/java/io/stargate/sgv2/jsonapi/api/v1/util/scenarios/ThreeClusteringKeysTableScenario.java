package io.stargate.sgv2.jsonapi.api.v1.util.scenarios;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.fixtures.data.DefaultData;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates a table for testing vectors with:
 *
 * <ul>
 *   <li>PK of the single field column {@link TestDataScenario#ID_COL}
 *   <li>Three Clusteron
 *   <li>Two vector columns {@link #INDEXED_VECTOR_COL} that is indexed and {@link
 *       #UNINDEXED_VECTOR_COL} that is not, both have dimension 5
 *   <li>20 rows with ID "row${number}" where number is 0+ that have a random vector, same in each
 *       vector col
 *   <li>One row with a vector all set to 1.0 , that has id row-1, so you can check to get a vector
 *       that is expected
 * </ul>
 */
public class ThreeClusteringKeysTableScenario extends TestDataScenario {

  public static final ApiColumnDef CLUSTER_COL_1 =
      new ApiColumnDef(CqlIdentifier.fromCql("cluster_col_1"), ApiDataTypeDefs.INT);
  public static final ApiColumnDef CLUSTER_COL_2 =
      new ApiColumnDef(CqlIdentifier.fromCql("cluster_col_2"), ApiDataTypeDefs.INT);
  public static final ApiColumnDef CLUSTER_COL_3 =
      new ApiColumnDef(CqlIdentifier.fromCql("cluster_col_3"), ApiDataTypeDefs.INT);
  public static final ApiColumnDef OFFSET_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("offset"), ApiDataTypeDefs.INT);

  public ThreeClusteringKeysTableScenario(String keyspaceName, String tableName) {
    super(
        keyspaceName,
        tableName,
        ID_COL,
        createClusteringDefs(),
        createColumns(),
        new DefaultData());
  }

  private static ApiColumnDefContainer createColumns() {

    var columns = new ApiColumnDefContainer();
    columns.put(ID_COL);
    columns.put(CLUSTER_COL_1);
    columns.put(CLUSTER_COL_2);
    columns.put(CLUSTER_COL_3);
    columns.put(OFFSET_COL);
    return columns;
  }

  private static List<ApiClusteringDef> createClusteringDefs() {

    return List.of(
        new ApiClusteringDef(CLUSTER_COL_1, ApiClusteringOrder.ASC),
        new ApiClusteringDef(CLUSTER_COL_2, ApiClusteringOrder.ASC),
        new ApiClusteringDef(CLUSTER_COL_3, ApiClusteringOrder.DESC));
  }

  @Override
  protected void insertRows() {

    var rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 20; i++) {
      var row = new HashMap<String, Object>();
      row.put(fieldName(ID_COL), "row-1");
      row.put(fieldName(CLUSTER_COL_1), i);
      row.put(fieldName(CLUSTER_COL_2), i);
      row.put(fieldName(CLUSTER_COL_3), i);
      row.put(fieldName(OFFSET_COL), i);
      rows.add(row);
    }
    insertManyRows(rows);
  }
}
