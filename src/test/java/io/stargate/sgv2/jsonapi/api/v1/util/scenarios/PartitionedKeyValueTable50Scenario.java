package io.stargate.sgv2.jsonapi.api.v1.util.scenarios;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.fixtures.data.DefaultData;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** basic table with 100 rows to test pagination */
public class PartitionedKeyValueTable50Scenario extends TestDataScenario {

  public static final ApiColumnDef CLUSTER_COL_1 =
      new ApiColumnDef(CqlIdentifier.fromCql("cluster_col_1"), ApiDataTypeDefs.INT);
  public static final ApiColumnDef VALUE_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("value_col"), ApiDataTypeDefs.TEXT);

  public PartitionedKeyValueTable50Scenario(String keyspaceName, String tableName) {
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
    columns.put(VALUE_COL);
    return columns;
  }

  private static List<ApiClusteringDef> createClusteringDefs() {

    return List.of(new ApiClusteringDef(CLUSTER_COL_1, ApiClusteringOrder.ASC));
  }

  @Override
  protected void insertRows() {

    var rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 50; i++) {
      var row = new HashMap<String, Object>();
      row.put(fieldName(ID_COL), "row-1");
      row.put(fieldName(CLUSTER_COL_1), i);
      row.put(fieldName(VALUE_COL), "value-" + i);
      rows.add(row);
    }
    insertManyRows(rows);
  }
}
