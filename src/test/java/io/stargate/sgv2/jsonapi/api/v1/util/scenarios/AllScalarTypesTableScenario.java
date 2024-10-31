package io.stargate.sgv2.jsonapi.api.v1.util.scenarios;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.fixtures.data.DefaultData;
import io.stargate.sgv2.jsonapi.fixtures.types.ApiDataTypesForTesting;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDefContainer;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Creates a table with: - PK of the single field column {@link TestDataScenario#ID_COL} - a column
 * for each scalar type in {@link ApiDataTypesForTesting#ALL_SCALAR_TYPES_FOR_CREATE} - the non PK
 * columns are named with the prefix {@link #COL_NAME_PREFIX} then the data type name - gets data
 * from the {@link DefaultData} class - inserts row with the PK prefix "row" - the row "row-1" (-1)
 * has a value for every column - the row "row-0" and beyond have a null for a single column in each
 * row, one column at a time - call {@link #columnForDatatype(ApiDataType)} to get the column for a
 * specific data type
 */
public class AllScalarTypesTableScenario extends TestDataScenario {

  public static final String COL_NAME_PREFIX = "col_";

  public AllScalarTypesTableScenario(String keyspaceName, String tableName) {
    super(keyspaceName, tableName, ID_COL, createColumns(), new DefaultData());
  }

  public ApiColumnDef columnForDatatype(ApiDataType type) {
    return nonPkColumns.get(columnNameForType(type));
  }

  public static CqlIdentifier columnNameForType(ApiDataType type) {
    return CqlIdentifier.fromInternal(COL_NAME_PREFIX + type.typeName().apiName());
  }

  private static ApiColumnDefContainer createColumns() {
    var columns = new ApiColumnDefContainer();
    columns.put(ID_COL);
    addColumnsForTypes(
        columns,
        ApiDataTypesForTesting.ALL_SCALAR_TYPES_FOR_CREATE,
        AllScalarTypesTableScenario::columnNameForType);
    return columns;
  }

  @Override
  protected void insertRows() {
    AtomicInteger pkCounter = new AtomicInteger(-1);

    var rows =
        createRowForEachNonPkColumnWithNull(apiColumnDef -> "row" + pkCounter.getAndIncrement());

    insertManyRows(rows);
  }
}
