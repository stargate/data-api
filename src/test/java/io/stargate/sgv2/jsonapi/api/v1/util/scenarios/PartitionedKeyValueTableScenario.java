package io.stargate.sgv2.jsonapi.api.v1.util.scenarios;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.stargate.sgv2.jsonapi.fixtures.data.DefaultData;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates a table for testing pagination with:
 *
 * <ul>
 *   <li>In total 3 partitions with 50 rows each
 *   <li>Partition key of the single field column {@link TestDataScenario#ID_COL}
 *   <li>Cluster key of the single field column {@link #CLUSTER_COL_1}
 *   <li>One value column {@link #VALUE_COL}
 * </ul>
 */
public class PartitionedKeyValueTableScenario extends TestDataScenario {

  public static final ApiColumnDef CLUSTER_COL_1 =
      new ApiColumnDef(CqlIdentifier.fromCql("cluster_col_1"), ApiDataTypeDefs.INT);
  public static final ApiColumnDef VALUE_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("value_col"), ApiDataTypeDefs.TEXT);
  protected static ArrayList<Map<String, Object>> data = new ArrayList<>();

  public PartitionedKeyValueTableScenario(String keyspaceName, String tableName) {
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
    // Insert 50 rows in each of the 3 partitions
    for (int partition = 0; partition < 3; partition++) {
      for (int i = 0; i < 50; i++) {
        var row = new HashMap<String, Object>();
        row.put(fieldName(ID_COL), "partition-" + partition);
        row.put(fieldName(CLUSTER_COL_1), i);
        row.put(fieldName(VALUE_COL), "value-" + i);
        data.add(row);
      }
    }
    insertManyRows(data);
  }

  public String getPageContent(int skip, int limit, String partitionKey) throws Exception {
    List<Map<String, Object>> page;
    if (partitionKey != null) {
      page =
          data.stream()
              .filter(row -> row.get(fieldName(ID_COL)).equals(partitionKey))
              .skip(skip)
              .limit(limit)
              .toList();
    } else {
      page = data.stream().skip(skip).limit(limit).toList();
    }
    return MAPPER.writeValueAsString(page);
  }
}
