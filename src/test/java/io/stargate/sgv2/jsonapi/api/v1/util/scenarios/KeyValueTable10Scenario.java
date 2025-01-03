package io.stargate.sgv2.jsonapi.api.v1.util.scenarios;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.fixtures.data.DefaultData;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiClusteringDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDefContainer;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** basic key-value table with 10 rows */
public class KeyValueTable10Scenario extends TestDataScenario {

  public static final ApiColumnDef VALUE_COL =
      new ApiColumnDef(CqlIdentifier.fromCql("value_col"), ApiDataTypeDefs.TEXT);

  public KeyValueTable10Scenario(String keyspaceName, String tableName) {
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
    columns.put(VALUE_COL);
    return columns;
  }

  private static List<ApiClusteringDef> createClusteringDefs() {

    return List.of();
  }

  @Override
  protected void insertRows() {

    var rows = new ArrayList<Map<String, Object>>();
    for (int i = 0; i < 10; i++) {
      var row = new HashMap<String, Object>();
      row.put(fieldName(ID_COL), "row" + i);
      row.put(fieldName(VALUE_COL), "value-" + i);
      rows.add(row);
    }
    insertManyRows(rows);
  }
}
